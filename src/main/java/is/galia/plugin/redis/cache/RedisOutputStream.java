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

import is.galia.async.VirtualThreadPool;
import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.operation.OperationList;
import io.lettuce.core.api.StatefulRedisConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

class RedisOutputStream extends CompletableOutputStream {

    private final StatefulRedisConnection<String, byte[]> connection;
    private final OperationList opList;
    private final String hashKey;
    private final String valueKey;
    private final ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    private final Set<CacheObserver> observers;

    RedisOutputStream(StatefulRedisConnection<String, byte[]> connection,
                      OperationList opList,
                      String hashKey,
                      String valueKey,
                      Set<CacheObserver> observers) {
        this.connection = connection;
        this.opList     = opList;
        this.hashKey    = hashKey;
        this.valueKey   = valueKey;
        this.observers  = observers;
    }

    /**
     * Uses {@code HSET} to write the data to Redis in a separate thread
     * and returns immediately.
     */
    @Override
    public void close() throws IOException {
        try {
            if (isComplete()) {
                VirtualThreadPool.getInstance().submit(() -> {
                    connection.sync().hset(hashKey, valueKey,
                            bufferStream.toByteArray());
                    observers.forEach(o -> o.onImageWritten(opList));
                });
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void flush() throws IOException {
        bufferStream.flush();
    }

    @Override
    public void write(int b) {
        bufferStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        bufferStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        bufferStream.write(b, off, len);
    }

}
