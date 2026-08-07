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

package org.apache.doris.connector.cache;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common cache specification for external metadata caches.
 *
 * <p>Connector-side copy of the meta-cache property model (independent-copy meta-cache migration). fe-core is
 * NOT changed: it keeps its own {@code org.apache.doris.datasource.metacache.CacheSpec}; this is a separate
 * class under {@code org.apache.doris.connector.*} used only by the connector plugins. Although that prefix is
 * parent-first, fe-core does not depend on this module, so the class resolves parent → miss → CHILD and is
 * child-loaded per plugin — fe-core and the plugins do NOT share one {@code Class} identity. It carries no
 * third-party dependency (JDK only) and never crosses the fe-core↔connector boundary as an object (only its
 * {@code IllegalArgumentException}, a JDK type, crosses), so it is safe on both classpaths.
 *
 * <p>The {@code check*Property} validators throw {@link IllegalArgumentException} (fe-core's
 * {@code PluginDrivenExternalCatalog.checkProperties} re-wraps it into a {@code DdlException} verbatim; the
 * legacy fe-core catalogs that still call these validators declare {@code throws DdlException} but no longer
 * need it). The user-facing message text is identical to the legacy one ({@code "... is wrong, value is ..."}).
 *
 * <p>Semantics:
 * <ul>
 *   <li>enable=false disables cache</li>
 *   <li>ttlSecond=0 disables cache, ttlSecond=-1 means no expiration</li>
 *   <li>capacity=0 disables a count-bounded cache</li>
 *   <li>when maxWeight is present, it replaces capacity as the effective bound</li>
 *   <li>maxWeight accepts an optional binary unit such as KB, MB, or GB; a bare number means bytes</li>
 * </ul>
 */
public final class CacheSpec {
    public static final long CACHE_NO_TTL = -1L;
    public static final long CACHE_TTL_DISABLE_CACHE = 0L;
    private static final String META_CACHE_PREFIX = "meta.cache.";
    private static final String KEY_ENABLE = ".enable";
    private static final String KEY_TTL_SECOND = ".ttl-second";
    private static final String KEY_CAPACITY = ".capacity";
    private static final String KEY_MAX_WEIGHT = ".max-weight";
    private static final Pattern DATA_SIZE_PATTERN = Pattern.compile("(\\d+)([a-zA-Z]*)");

    private final boolean enable;
    private final long ttlSecond;
    private final long capacity;
    private final OptionalLong maxWeight;

    private CacheSpec(boolean enable, long ttlSecond, long capacity, OptionalLong maxWeight) {
        this.enable = enable;
        this.ttlSecond = ttlSecond;
        this.capacity = capacity;
        this.maxWeight = Objects.requireNonNull(maxWeight, "maxWeight is required");
    }

    public static CacheSpec of(boolean enable, long ttlSecond, long capacity) {
        return new CacheSpec(enable, ttlSecond, capacity, OptionalLong.empty());
    }

    public static CacheSpec ofWeight(boolean enable, long ttlSecond, long capacity, long maxWeight) {
        return new CacheSpec(enable, ttlSecond, capacity, OptionalLong.of(maxWeight));
    }

    /**
     * Build an ENABLED spec from a connector-resolved TTL under the "{@code <= 0} disables" contract.
     *
     * <p>A connector that resolves its own single {@code ttl-second} knob (iceberg's shared
     * {@code meta.cache.iceberg.table.ttl-second}, paimon's snapshot cache) treats any non-positive TTL as
     * "disable caching, always read live". That is NOT the raw {@link CacheSpec} contract, which reads
     * {@code ttlSecond == -1} as {@link #CACHE_NO_TTL} ("no expiration", still ENABLED) and only
     * {@code ttlSecond == 0} as {@link #CACHE_TTL_DISABLE_CACHE} ("disabled"). This factory folds any
     * non-positive TTL to the disable sentinel so a negative operator value disables the cache rather than
     * silently becoming a never-expiring one. It is exactly the
     * {@code ttlSecond > 0 ? of(true, ttlSecond, capacity) : of(true, CACHE_TTL_DISABLE_CACHE, capacity)}
     * expression each per-catalog cache used to inline.
     */
    public static CacheSpec ofConnectorTtl(long ttlSecond, long capacity) {
        return of(true, ttlSecond > 0 ? ttlSecond : CACHE_TTL_DISABLE_CACHE, capacity);
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
        OptionalLong maxWeight = getOptionalDataSizeProperty(
                properties, propertySpec.getMaxWeightKey(), propertySpec.getDefaultMaxWeight());
        return new CacheSpec(enable, ttlSecond, capacity, maxWeight);
    }

    /**
     * Build a cache spec from catalog properties by standard external meta cache key pattern:
     * meta.cache.&lt;engine&gt;.&lt;entry&gt;.(enable|ttl-second|capacity|max-weight)
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
                .maxWeight(cacheKeyPrefix + KEY_MAX_WEIGHT, defaultSpec.getMaxWeight())
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

    public static void checkBooleanProperty(String value, String key) {
        if (value == null) {
            return;
        }
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static void checkLongProperty(String value, long minValue, String key) {
        if (value == null) {
            return;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The parameter " + key + " is wrong, value is " + value);
        }
        if (parsed < minValue) {
            throw new IllegalArgumentException("The parameter " + key + " is wrong, value is " + value);
        }
    }

    public static void checkDataSizeProperty(String value, String key) {
        if (value == null) {
            return;
        }
        try {
            parseDataSizeAllowZero(value);
        } catch (IllegalArgumentException e) {
            throw invalidDataSizeProperty(key, value);
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
     * Build the standard external meta cache TTL key for one engine+entry.
     * Example: {@code meta.cache.hive.file.ttl-second}.
     *
     * <p>Used to translate a legacy catalog TTL knob (e.g. {@code file.meta.cache.ttl-second}) into the
     * namespaced key a cache actually reads, via {@link #applyCompatibilityMap}.
     */
    public static String metaCacheTtlKey(String engine, String entryName) {
        return META_CACHE_PREFIX + engine + "." + entryName + KEY_TTL_SECOND;
    }

    /**
     * Build the standard external meta cache maximum-weight key for one engine+entry.
     */
    public static String metaCacheMaxWeightKey(String engine, String entryName) {
        return META_CACHE_PREFIX + engine + "." + entryName + KEY_MAX_WEIGHT;
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
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static OptionalLong getOptionalDataSizeProperty(
            Map<String, String> properties, String key, OptionalLong defaultValue) {
        if (key == null) {
            return defaultValue;
        }
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return OptionalLong.of(parseDataSizeAllowZero(value));
        } catch (IllegalArgumentException e) {
            throw invalidDataSizeProperty(key, value);
        }
    }

    private static long parseDataSizeAllowZero(String value) {
        Matcher matcher = DATA_SIZE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("invalid data size");
        }

        long number;
        try {
            number = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid data size", e);
        }

        long multiplier;
        switch (matcher.group(2).toUpperCase(Locale.ROOT)) {
            case "":
            case "B":
                multiplier = 1L;
                break;
            case "K":
            case "KB":
                multiplier = 1L << 10;
                break;
            case "M":
            case "MB":
                multiplier = 1L << 20;
                break;
            case "G":
            case "GB":
                multiplier = 1L << 30;
                break;
            case "T":
            case "TB":
                multiplier = 1L << 40;
                break;
            case "P":
            case "PB":
                multiplier = 1L << 50;
                break;
            default:
                throw new IllegalArgumentException("invalid data size unit");
        }
        try {
            return Math.multiplyExact(number, multiplier);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("data size is too large", e);
        }
    }

    private static IllegalArgumentException invalidDataSizeProperty(String key, String value) {
        return new IllegalArgumentException("The parameter " + key + " is wrong, value is " + value);
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

    public OptionalLong getMaxWeight() {
        return maxWeight;
    }

    public boolean isWeightBounded() {
        return maxWeight.isPresent();
    }

    public boolean isCacheEnabled() {
        return enable
                && ttlSecond != CACHE_TTL_DISABLE_CACHE
                && maxWeight.orElse(capacity) != 0L;
    }

    public static final class PropertySpec {
        private final String enableKey;
        private final boolean defaultEnable;
        private final String ttlKey;
        private final long defaultTtlSecond;
        private final String capacityKey;
        private final long defaultCapacity;
        private final String maxWeightKey;
        private final OptionalLong defaultMaxWeight;

        private PropertySpec(String enableKey, boolean defaultEnable, String ttlKey,
                long defaultTtlSecond, String capacityKey, long defaultCapacity,
                String maxWeightKey, OptionalLong defaultMaxWeight) {
            this.enableKey = enableKey;
            this.defaultEnable = defaultEnable;
            this.ttlKey = ttlKey;
            this.defaultTtlSecond = defaultTtlSecond;
            this.capacityKey = capacityKey;
            this.defaultCapacity = defaultCapacity;
            this.maxWeightKey = maxWeightKey;
            this.defaultMaxWeight = defaultMaxWeight;
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

        public String getMaxWeightKey() {
            return maxWeightKey;
        }

        public OptionalLong getDefaultMaxWeight() {
            return defaultMaxWeight;
        }

        public static final class Builder {
            private String enableKey;
            private boolean defaultEnable;
            private String ttlKey;
            private long defaultTtlSecond;
            private String capacityKey;
            private long defaultCapacity;
            private String maxWeightKey;
            private OptionalLong defaultMaxWeight = OptionalLong.empty();

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

            public Builder maxWeight(String key, OptionalLong defaultValue) {
                this.maxWeightKey = Objects.requireNonNull(key, "maxWeightKey is required");
                this.defaultMaxWeight = Objects.requireNonNull(defaultValue, "defaultMaxWeight is required");
                return this;
            }

            public PropertySpec build() {
                return new PropertySpec(
                        Objects.requireNonNull(enableKey, "enableKey is required"),
                        defaultEnable,
                        Objects.requireNonNull(ttlKey, "ttlKey is required"),
                        defaultTtlSecond,
                        Objects.requireNonNull(capacityKey, "capacityKey is required"),
                        defaultCapacity,
                        maxWeightKey,
                        defaultMaxWeight);
            }
        }
    }
}
