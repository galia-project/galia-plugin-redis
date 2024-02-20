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

import io.lettuce.core.api.StatefulRedisConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads data into a buffer and provides stream access to it.
 */
class RedisInputStream extends InputStream {

    private final StatefulRedisConnection<String, byte[]> connection;
    private final String hashKey;
    private final String valueKey;
    private ByteArrayInputStream bufferStream;

    RedisInputStream(StatefulRedisConnection<String, byte[]> connection,
                     String hashKey,
                     String valueKey) {
        this.connection = connection;
        this.hashKey    = hashKey;
        this.valueKey   = valueKey;
    }

    private void bufferValue() {
        byte[] value = connection.sync().hget(hashKey, valueKey);
        bufferStream = new ByteArrayInputStream(value);
    }

    @Override
    public void close() throws IOException {
        try {
            if (bufferStream != null) {
                bufferStream.close();
            }
        } finally {
            super.close();
        }
    }

    @Override
    public int read() {
        if (bufferStream == null) {
            bufferValue();
        }
        return bufferStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (bufferStream == null) {
            bufferValue();
        }
        return bufferStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (bufferStream == null) {
            bufferValue();
        }
        return bufferStream.read(b, off, len);
    }

}
