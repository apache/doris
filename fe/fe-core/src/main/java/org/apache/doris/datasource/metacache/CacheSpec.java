// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.DdlException;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Common cache specification for external metadata caches.
 *
 * <p>Semantics:
 * <ul>
 *   <li>enable=false disables cache</li>
 *   <li>ttlSecond=0 disables cache, ttlSecond=-1 means no expiration</li>
 *   <li>capacity=0 disables cache; capacity is count-based</li>
 * </ul>
 */
public final class CacheSpec {
    public static final long CACHE_NO_TTL = -1L;
    public static final long CACHE_TTL_DISABLE_CACHE = 0L;
    private static final String META_CACHE_PREFIX = "meta.cache.";
    private static final String KEY_ENABLE = ".enable";
    private static final String KEY_TTL_SECOND = ".ttl-second";
    private static final String KEY_CAPACITY = ".capacity";

    private final boolean enable;
    private final long ttlSecond;
    private final long capacity;

    private CacheSpec(boolean enable, long ttlSecond, long capacity) {
        this.enable = enable;
        this.ttlSecond = ttlSecond;
        this.capacity = capacity;
    }

    public static CacheSpec of(boolean enable, long ttlSecond, long capacity) {
        return new CacheSpec(enable, ttlSecond, capacity);
    }

    public static PropertySpec.Builder propertySpecBuilder() {
        return new PropertySpec.Builder();
    }

    public static CacheSpec fromProperties(Map<String, String> properties,
            String enableKey, boolean defaultEnable,
            String ttlKey, long defaultTtlSecond,
            String capacityKey, long defaultCapacity) {
        return fromProperties(properties, propertySpecBuilder()
                .enable(enableKey, defaultEnable)
                .ttl(ttlKey, defaultTtlSecond)
                .capacity(capacityKey, defaultCapacity)
                .build());
    }

    public static CacheSpec fromProperties(Map<String, String> properties, PropertySpec propertySpec) {
        boolean enable = getBooleanProperty(properties, propertySpec.getEnableKey(), propertySpec.isDefaultEnable());
        long ttlSecond = getLongProperty(properties, propertySpec.getTtlKey(), propertySpec.getDefaultTtlSecond());
        long capacity = getLongProperty(properties, propertySpec.getCapacityKey(), propertySpec.getDefaultCapacity());
        return of(enable, ttlSecond, capacity);
    }

    /**
     * Build a cache spec from catalog properties by standard external meta cache key pattern:
     * meta.cache.&lt;engine&gt;.&lt;entry&gt;.(enable|ttl-second|capacity)
     */
    public static CacheSpec fromProperties(Map<String, String> properties,
            String engine, String entryName, CacheSpec defaultSpec) {
        return fromProperties(properties, metaCachePropertySpec(engine, entryName, defaultSpec));
    }

    public static PropertySpec metaCachePropertySpec(String engine, String entryName, CacheSpec defaultSpec) {
        String cacheKeyPrefix = META_CACHE_PREFIX + engine + "." + entryName;
        return propertySpecBuilder()
                .enable(cacheKeyPrefix + KEY_ENABLE, defaultSpec.isEnable())
                .ttl(cacheKeyPrefix + KEY_TTL_SECOND, defaultSpec.getTtlSecond())
                .capacity(cacheKeyPrefix + KEY_CAPACITY, defaultSpec.getCapacity())
                .build();
    }

    /**
     * Apply compatibility key mapping before cache spec parsing.
     *
     * <p>Map format: {@code legacyKey -> newKey}. If both keys exist, new key wins.
     */
    public static Map<String, String> applyCompatibilityMap(
            Map<String, String> properties, Map<String, String> compatibilityMap) {
        Map<String, String> mapped = new HashMap<>();
        if (properties != null) {
            mapped.putAll(properties);
        }
        if (compatibilityMap == null || compatibilityMap.isEmpty()) {
            return mapped;
        }
        compatibilityMap.forEach((legacyKey, newKey) -> {
            if (legacyKey == null || newKey == null || legacyKey.equals(newKey)) {
                return;
            }
            if (!mapped.containsKey(newKey) && mapped.containsKey(legacyKey)) {
                mapped.put(newKey, mapped.get(legacyKey));
            }
        });
        return mapped;
    }

    public static void checkBooleanProperty(String value, String key) throws DdlException {
        if (value == null) {
            return;
        }
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static void checkLongProperty(String value, long minValue, String key) throws DdlException {
        if (value == null) {
            return;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
        if (parsed < minValue) {
            throw new DdlException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static boolean isCacheEnabled(boolean enable, long ttlSecond, long capacity) {
        return enable && ttlSecond != 0 && capacity != 0;
    }

    /**
     * Build standard external meta cache key prefix for one engine.
     * Example: {@code meta.cache.iceberg.}
     */
    public static String metaCacheKeyPrefix(String engine) {
        return META_CACHE_PREFIX + engine + ".";
    }

    /**
     * Returns true when the given property key belongs to one engine's meta cache namespace.
     */
    public static boolean isMetaCacheKeyForEngine(String key, String engine) {
        return key != null && engine != null && key.startsWith(metaCacheKeyPrefix(engine));
    }

    /**
     * Convert ttlSecond to OptionalLong for CacheFactory.
     * ttlSecond=-1 means no expiration; ttlSecond=0 disables cache.
     */
    public static OptionalLong toExpireAfterAccess(long ttlSecond) {
        if (ttlSecond == CACHE_NO_TTL) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Math.max(ttlSecond, CACHE_TTL_DISABLE_CACHE));
    }

    private static boolean getBooleanProperty(Map<String, String> properties, String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private static long getLongProperty(Map<String, String> properties, String key, long defaultValue) {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return NumberUtils.toLong(value, defaultValue);
    }

    public boolean isEnable() {
        return enable;
    }

    public long getTtlSecond() {
        return ttlSecond;
    }

    public long getCapacity() {
        return capacity;
    }

    public static final class PropertySpec {
        private final String enableKey;
        private final boolean defaultEnable;
        private final String ttlKey;
        private final long defaultTtlSecond;
        private final String capacityKey;
        private final long defaultCapacity;

        private PropertySpec(String enableKey, boolean defaultEnable, String ttlKey,
                long defaultTtlSecond, String capacityKey, long defaultCapacity) {
            this.enableKey = enableKey;
            this.defaultEnable = defaultEnable;
            this.ttlKey = ttlKey;
            this.defaultTtlSecond = defaultTtlSecond;
            this.capacityKey = capacityKey;
            this.defaultCapacity = defaultCapacity;
        }

        public String getEnableKey() {
            return enableKey;
        }

        public boolean isDefaultEnable() {
            return defaultEnable;
        }

        public String getTtlKey() {
            return ttlKey;
        }

        public long getDefaultTtlSecond() {
            return defaultTtlSecond;
        }

        public String getCapacityKey() {
            return capacityKey;
        }

        public long getDefaultCapacity() {
            return defaultCapacity;
        }

        public static final class Builder {
            private String enableKey;
            private boolean defaultEnable;
            private String ttlKey;
            private long defaultTtlSecond;
            private String capacityKey;
            private long defaultCapacity;

            public Builder enable(String key, boolean defaultValue) {
                this.enableKey = key;
                this.defaultEnable = defaultValue;
                return this;
            }

            public Builder ttl(String key, long defaultValue) {
                this.ttlKey = key;
                this.defaultTtlSecond = defaultValue;
                return this;
            }

            public Builder capacity(String key, long defaultValue) {
                this.capacityKey = key;
                this.defaultCapacity = defaultValue;
                return this;
            }

            public PropertySpec build() {
                return new PropertySpec(
                        Objects.requireNonNull(enableKey, "enableKey is required"),
                        defaultEnable,
                        Objects.requireNonNull(ttlKey, "ttlKey is required"),
                        defaultTtlSecond,
                        Objects.requireNonNull(capacityKey, "capacityKey is required"),
                        defaultCapacity);
            }
        }
    }
}
