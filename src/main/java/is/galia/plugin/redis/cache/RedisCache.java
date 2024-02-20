/*
 * Copyright © 2024 Baird Creek Software LLC
 *
 * Licensed under the PolyForm Noncommercial License, version 1.0.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://polyformproject.org/licenses/noncommercial/1.0.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package is.galia.plugin.redis.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import is.galia.cache.AbstractCache;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.cache.VariantCache;
import is.galia.config.Configuration;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.StatResult;
import is.galia.operation.OperationList;
import is.galia.plugin.Plugin;
import is.galia.plugin.redis.config.Key;
import io.lettuce.core.MapScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Cache using Redis via the <a href="https://lettuce.io">Lettuce</a>
 * client.</p>
 *
 * <p>Content is structured as follows:</p>
 *
 * {@snippet
 * {
 *     #{@link #IMAGE_HASH_KEY}: {
 *         "operation list string representation": image byte array
 *     },
 *     #{@link #INFO_HASH_KEY}: {
 *         "identifier": "UTF-8 JSON string"
 *     }
 * }}
 */
public final class RedisCache extends AbstractCache
        implements VariantCache, InfoCache, Plugin {

    /**
     * Custom Lettuce codec that enables us to store byte array values
     * corresponding to UTF-8 string keys.
     */
    static class CustomRedisCodec implements RedisCodec<String, byte[]> {

        private final RedisCodec<String,String> keyDelegate = StringCodec.UTF8;
        private final RedisCodec<byte[],byte[]> valueDelegate = new ByteArrayCodec();

        @Override
        public ByteBuffer encodeKey(String k) {
            return keyDelegate.encodeKey(k);
        }

        @Override
        public ByteBuffer encodeValue(byte[] o) {
            return valueDelegate.encodeValue(o);
        }

        @Override
        public String decodeKey(ByteBuffer byteBuffer) {
            return keyDelegate.decodeKey(byteBuffer);
        }

        @Override
        public byte[] decodeValue(ByteBuffer byteBuffer) {
            return valueDelegate.decodeValue(byteBuffer);
        }

    }

    private static final Logger LOGGER = LoggerFactory.
            getLogger(RedisCache.class);

    private static final String IMAGE_HASH_KEY =
            RedisCache.class.getName() + ".image";
    private static final String INFO_HASH_KEY =
            RedisCache.class.getName() + ".info";

    private static RedisClient client;
    private static StatefulRedisConnection<String, byte[]> connection;

    private static synchronized RedisClient getClient() {
        if (client == null) {
            Configuration config = Configuration.forApplication();
            RedisURI redisUri    = RedisURI.Builder
                    .redis(config.getString(Key.REDISCACHE_HOST.key()))
                    .withPort(config.getInt(Key.REDISCACHE_PORT.key(), 6379))
                    .withSsl(config.getBoolean(Key.REDISCACHE_SSL.key(), false))
                    .withPassword(config.getString(Key.REDISCACHE_PASSWORD.key(), "").toCharArray())
                    .withDatabase(config.getInt(Key.REDISCACHE_DATABASE.key(), 0))
                    .build();
            client = RedisClient.create(redisUri);
        }
        return client;
    }

    private static synchronized StatefulRedisConnection<String, byte[]> getConnection() {
        if (connection == null) {
            RedisClient client = getClient();
            connection = client.connect(new CustomRedisCodec());
        }
        return connection;
    }

    private static String imageKey(OperationList opList) {
        return opList.toString();
    }

    private static String infoKey(Identifier identifier) {
        return identifier.toString();
    }

    private void purgeImages() {
        LOGGER.debug("purgeImages(): purging {}...", IMAGE_HASH_KEY);
        getConnection().sync().del(IMAGE_HASH_KEY);
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(RedisCache.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return getClass().getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void onApplicationStop() {}

    @Override
    public void initializePlugin() {}

    //endregion
    //region Cache methods

    @Override
    public void evict(Identifier identifier) {
        // Purge info
        String infoKey = infoKey(identifier);
        LOGGER.debug("evict(Identifier): purging {}...", infoKey);
        getConnection().sync().hdel(INFO_HASH_KEY, infoKey);

        // Purge images
        ScanArgs imagePattern = ScanArgs.Builder.matches(identifier + "*");
        LOGGER.debug("evict(Identifier): purging {}...", imagePattern);

        MapScanCursor<String, byte[]> cursor = getConnection().sync().
                hscan(IMAGE_HASH_KEY, imagePattern);
        for (String key : cursor.getMap().keySet()) {
            getConnection().sync().hdel(IMAGE_HASH_KEY, key);
        }
    }

    /**
     * No-op.
     */
    @Override
    public void evictInvalid() {
        LOGGER.debug("evictInvalid(): " +
                "nothing to do (expiration must be configured in Redis)");
    }

    @Override
    public void purge() {
        evictInfos();
        purgeImages();
    }

    @Override
    public void shutdown() {
        getConnection().close();
        getClient().close();
    }

    //endregion
    //region InfoCache methods

    @Override
    public void evictInfos() {
        LOGGER.debug("evictInfos(): deleting {}...", INFO_HASH_KEY);
        getConnection().sync().del(INFO_HASH_KEY);
    }

    @Override
    public Optional<Info> fetchInfo(Identifier identifier) throws IOException {
        byte[] json = getConnection().sync().hget(INFO_HASH_KEY,
                infoKey(identifier));
        if (json != null) {
            String jsonStr = new String(json, StandardCharsets.UTF_8);
            return Optional.of(Info.fromJSON(jsonStr));
        }
        return Optional.empty();
    }

    @Override
    public void put(Identifier identifier, Info info) throws IOException {
        try {
            put(identifier, info.toJSON());
        } catch (JsonProcessingException e) {
            LOGGER.error("put(): {}", e.getMessage());
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Identifier identifier, String info) {
        LOGGER.debug("put(): caching info for {}", identifier);
        getConnection().sync().hset(
                INFO_HASH_KEY,
                infoKey(identifier),
                info.getBytes(StandardCharsets.UTF_8));
    }

    //endregion
    //region VariantCache methods

    @Override
    public void evict(OperationList opList) {
        String imageKey = imageKey(opList);
        LOGGER.debug("evict(OperationList): purging {}...", imageKey);
        getConnection().sync().hdel(IMAGE_HASH_KEY, imageKey);
    }

    /**
     * @param statResult If Redis' maxmemory-policy configuration directive is
     *                   not set to one of the LFU policies, the given
     *                   instance's last-modified time will be updated.
     */
    @Override
    public InputStream newVariantImageInputStream(
            OperationList opList,
            StatResult statResult) {
        final String imageKey = imageKey(opList);
        // Try to populate the StatResult using OBJECT IDLETIME.
        final Long idleSeconds  = getConnection().sync().objectIdletime(imageKey);
        if (idleSeconds != null) {
            final long lastModified = Instant.now().getEpochSecond() - idleSeconds;
            statResult.setLastModified(Instant.ofEpochSecond(lastModified));
        }
        // Return the stream
        if (getConnection().sync().hexists(IMAGE_HASH_KEY, imageKey)) {
            return new RedisInputStream(
                    getConnection(), IMAGE_HASH_KEY, imageKey);
        }
        return null;
    }

    @Override
    public CompletableOutputStream
    newVariantImageOutputStream(OperationList opList) {
        return new RedisOutputStream(
                getConnection(), opList, IMAGE_HASH_KEY, imageKey(opList),
                getAllObservers());
    }

}
