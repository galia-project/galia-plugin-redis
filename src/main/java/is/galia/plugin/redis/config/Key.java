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

package is.galia.plugin.redis.config;

public enum Key {

    REDISCACHE_DATABASE("cache.RedisCache.database"),
    REDISCACHE_HOST    ("cache.RedisCache.host"),
    REDISCACHE_PASSWORD("cache.RedisCache.password"),
    REDISCACHE_PORT    ("cache.RedisCache.port"),
    REDISCACHE_SSL     ("cache.RedisCache.ssl");

    private final String key;

    Key(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public String toString() {
        return key();
    }

}
