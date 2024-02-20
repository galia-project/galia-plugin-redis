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

import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.config.Configuration;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.StatResult;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.redis.test.TestUtils;
import is.galia.plugin.redis.BaseTest;
import is.galia.util.ConcurrentProducerConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static is.galia.config.Key.VARIANT_CACHE_TTL;


class RedisCacheTest extends BaseTest {

    private static final int ASYNC_WAIT = 3500;

    private RedisCache instance;

    @BeforeAll
    public static void beforeClass() {
        new RedisCache().onApplicationStart();
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        instance = new RedisCache();
        instance.initializePlugin();
        instance.purge();
    }

    @Override
    public void tearDown() {
        super.tearDown();
        instance.purge();
    }

    //region Decoder methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(RedisCache.class.getSimpleName(),
                instance.getPluginName());
    }

    //endregion
    //region Cache methods

    /* evict(Identifier) */

    @Test
    void evict() throws Exception {
        // add an image and an info
        final Identifier id1        = new Identifier("cats");

        final OperationList opList1 = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id1, new Info());

        // add another image and another info
        final Identifier id2        = new Identifier("dogs");
        final OperationList opList2 = OperationList.builder()
                .withIdentifier(id2)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id2, new Info());

        assertNotNull(instance.fetchInfo(id1));
        assertNotNull(instance.fetchInfo(id2));

        // evict one of the info/image pairs
        instance.evict(id1);

        Thread.sleep(2000);

        // assert that its info and image are gone
        assertFalse(instance.fetchInfo(id1).isPresent());

        try (InputStream is = instance.newVariantImageInputStream(opList1)) {
            assertNull(is);
        }

        // ... but the other one is still there
        assertNotNull(instance.fetchInfo(id2));
        try (InputStream is = instance.newVariantImageInputStream(opList2)) {
            assertNotNull(is);
        }
    }

    /* evict(OperationList) */

    @Test
    void evictWithOperationList() throws Exception {
        // Seed a variant image
        OperationList ops1 = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // Seed another variant image
        OperationList ops2 = OperationList.builder()
                .withIdentifier(new Identifier("dogs"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        Thread.sleep(ASYNC_WAIT);

        // Evict the first one
        instance.evict(ops1);

        // Assert that it's gone
        TestUtils.assertNotExists(instance, ops1);

        // Assert that the other one was NOT evicted
        TestUtils.assertExists(instance, ops2);
    }

    /* evictInfos() */

    @Test
    void evictInfos() throws Exception {
        Identifier identifier = new Identifier(IMAGE);
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info             = new Info();

        // assert that a particular image doesn't exist
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            assertNull(is);
        }

        // assert that a particular info doesn't exist
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // add the image
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add the info
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        // assert that they've been added
        TestUtils.assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // Evict infos
        instance.evictInfos();

        // assert that the info has been evicted
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // assert that the image has NOT been evicted
        TestUtils.assertExists(instance, opList);
    }

    /* fetchInfo(Identifier) */

    @Test
    void fetchInfoWithExistingValidImage() throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info = new Info();
        instance.put(identifier, info);

        Optional<Info> actual = instance.fetchInfo(identifier);
        assertEquals(actual.orElseThrow(), info);
    }

    @Test
    void fetchInfoWithNonexistentImage() throws Exception {
        assertFalse(instance.fetchInfo(new Identifier("bogus")).isPresent());
    }

    @Test
    void fetchInfoPopulatesSerializationTimestampWhenNotAlreadySet()
            throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info = new Info();
        instance.put(identifier, info);

        info = instance.fetchInfo(identifier).orElseThrow();
        assertNotNull(info.getSerializationTimestamp());
    }

    @Test
    void fetchInfoConcurrently() {
        // This is tested by putConcurrently()
    }

    /* newVariantImageInputStream(OperationList) */

    @Test
    void newVariantImageInputStreamWithZeroTTL() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 0);

        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in and assert same size
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageInputStreamWithNonexistentImage() {
        OperationList ops = new OperationList(new Identifier("cats"));
        instance.purge();
        TestUtils.assertNotExists(instance, ops);
    }

    @Test
    void newVariantImageInputStreamConcurrently() throws Exception {
        final OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();

        new ConcurrentProducerConsumer(() -> {
            try (CompletableOutputStream os =
                         instance.newVariantImageOutputStream(ops)) {
                Files.copy(FIXTURE, os);
                os.complete();
            }
            return null;
        }, () -> {
            try (InputStream is = instance.newVariantImageInputStream(ops)) {
                if (is != null) {
                    //noinspection StatementWithEmptyBody
                    while (is.read() != -1) {
                        // consume the stream fully
                    }
                }
            }
            return null;
        }).run();
    }

    /* newVariantImageInputStream(OperationList, StatResult) */

    @Disabled // TODO: figure out why OBJECT IDLETIME is returning null
    @Test
    void newVariantImageInputStreamPopulatesStatResult() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 0);
        OperationList opList   = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch   = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        StatResult statResult = new StatResult();
        try (InputStream is = instance.newVariantImageInputStream(opList, statResult)) {
            assertNotNull(statResult.getLastModified());
            is.readAllBytes();
        }
    }

    /* newVariantImageOutputStream() */

    @Test
    void newVariantImageOutputStream() throws Exception {
        OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageOutputStreamDoesNotLeaveDetritusWhenStreamIsIncompletelyWritten()
            throws Exception {
        OperationList ops    = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            // don't set it complete
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Try to read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            assertNull(is);
        }
    }

    @Test
    void newVariantImageOutputStreamConcurrently() {
        // This is tested in testNewVariantImageInputStreamConcurrently()
    }

    @Test
    void newVariantImageOutputStreamOverwritesExistingImage() {
        // TODO: write this
    }

    /* purge() */

    @Test
    void purge() throws Exception {
        Identifier identifier = new Identifier(IMAGE);
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info = new Info();

        // add the image
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add the info
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        // assert that they've been added
        TestUtils.assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // purge everything
        instance.purge();

        // assert that the info has been purged
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // assert that the image has been purged
        TestUtils.assertNotExists(instance, opList);
    }

    /* put(Identifier, Info) */

    @Test
    void putWithInfo() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();

        instance.put(identifier, info);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link InfoCache#put(Identifier, Info)}
     * and {@link InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithInfoConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, info);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }).run();
    }

    /* put(Identifier, String) */

    @Test
    void putWithString() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        instance.put(identifier, infoStr);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link InfoCache#put(Identifier, String)}
     * and {@link InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithStringConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, infoStr);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }).run();
    }

}
