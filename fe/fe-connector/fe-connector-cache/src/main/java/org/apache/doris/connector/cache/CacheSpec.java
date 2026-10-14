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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common cache specification for external metadata caches.
 *
 * <p>This is the single property model shared by fe-core and connector plugins. It carries no fe-core dependency,
 * so cache configuration and validation remain available to independently loaded connectors.
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
 *   <li>capacity=0 disables cache; a positive capacity remains the count safety limit</li>
 *   <li>max-weight is an optional estimated retained-byte admission limit</li>
 * </ul>
 */
public final class CacheSpec {
    private static final Logger LOG = LogManager.getLogger(CacheSpec.class);
    public static final long CACHE_NO_TTL = -1L;
    public static final long CACHE_TTL_DISABLE_CACHE = 0L;
    private static final String META_CACHE_PREFIX = "meta.cache.";
    private static final String KEY_ENABLE = ".enable";
    private static final String KEY_TTL_SECOND = ".ttl-second";
    private static final String KEY_CAPACITY = ".capacity";
    private static final String KEY_MAX_WEIGHT = ".max-weight";
    private static final Pattern DATA_VOLUME_PATTERN = Pattern.compile(
            "^([0-9]+)\\s*(B|KB|MB|GB|TB|PB)?$", Pattern.CASE_INSENSITIVE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private final boolean enable;
    private final long ttlSecond;
    private final long capacity;
    private final OptionalLong maxWeight;

    private CacheSpec(boolean enable, long ttlSecond, long capacity, OptionalLong maxWeight) {
        this.enable = enable;
        this.ttlSecond = ttlSecond;
        this.capacity = capacity;
        this.maxWeight = Objects.requireNonNull(maxWeight, "maxWeight");
    }

    public static CacheSpec of(boolean enable, long ttlSecond, long capacity) {
        return new CacheSpec(enable, ttlSecond, capacity, OptionalLong.empty());
    }

    public static CacheSpec ofWeight(boolean enable, long ttlSecond, long capacity, long maxWeight) {
        if (maxWeight <= 0L) {
            throw new IllegalArgumentException("maxWeight must be positive: " + maxWeight);
        }
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
        OptionalLong maxWeight = getWeightProperty(properties, propertySpec.getMaxWeightKey());
        return new CacheSpec(enable, ttlSecond, capacity, maxWeight);
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
                .maxWeight(cacheKeyPrefix + KEY_MAX_WEIGHT)
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

    /** Strict CREATE/ALTER-time validation for the catalog weight property. */
    public static OptionalLong checkCatalogWeightProperty(Map<String, String> properties) {
        if (properties == null) {
            return OptionalLong.empty();
        }
        String catalogValue = properties.get(MetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY);
        if (catalogValue == null) {
            return OptionalLong.empty();
        }
        long parsed = parseWeight(catalogValue,
                MetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY, false, 0L);
        if (parsed <= 0L) {
            throw new IllegalArgumentException(
                    MetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY + " must be positive");
        }
        return OptionalLong.of(parsed);
    }

    /** Validates consumed weight values, tolerating unknown entries persisted by another version. */
    public static void checkWeightProperties(
            Map<String, String> properties, String engine, String... knownEntries) {
        checkWeightProperties(properties, Collections.emptyMap(), engine, knownEntries);
    }

    /** DDL validation: reject unknown entries only when explicitly submitted by this statement. */
    public static void checkWeightProperties(Map<String, String> properties,
            Map<String, String> submittedProperties, String engine, String... knownEntries) {
        OptionalLong catalogMax = checkCatalogWeightProperty(properties);
        if (properties == null) {
            return;
        }
        String prefix = metaCacheKeyPrefix(engine);
        Set<String> weightKeys = new HashSet<>();
        for (String entry : knownEntries) {
            weightKeys.add(prefix + entry + KEY_MAX_WEIGHT);
        }
        for (String key : submittedProperties.keySet()) {
            if (key.startsWith(prefix) && key.endsWith(KEY_MAX_WEIGHT) && !weightKeys.contains(key)) {
                throw new IllegalArgumentException("Unknown metadata cache weight property: " + key);
            }
        }
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = property.getKey();
            if (!weightKeys.contains(key)) {
                // ALTER merges properties and cannot remove keys written by a newer connector version.
                // Like other connector properties, validate consumed values but tolerate unknown keys.
                continue;
            }
            long parsed = parseWeight(property.getValue(), key, false, 0L);
            if (parsed <= 0L) {
                throw new IllegalArgumentException(key + " must be positive");
            }
            if (catalogMax.isPresent() && parsed > catalogMax.getAsLong()) {
                throw new IllegalArgumentException(key + " can not exceed "
                        + MetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY);
            }
        }
    }

    public static boolean isCacheEnabled(boolean enable, long ttlSecond, long capacity) {
        return enable && ttlSecond != 0 && capacity != 0;
    }

    /** Parse bytes with an optional binary unit, or a heap percentage when explicitly allowed. */
    public static long parseWeight(String value, String key, boolean allowPercent, long maxHeapBytes) {
        String normalized = Objects.requireNonNull(value, "value").trim();
        if (normalized.isEmpty()) {
            throw invalidWeight(key, value);
        }
        if (normalized.endsWith("%")) {
            if (!allowPercent || maxHeapBytes <= 0L) {
                throw invalidWeight(key, value);
            }
            try {
                BigDecimal percentage = new BigDecimal(
                        normalized.substring(0, normalized.length() - 1).trim());
                if (percentage.signum() < 0 || percentage.compareTo(BigDecimal.valueOf(100L)) > 0) {
                    throw invalidWeight(key, value);
                }
                return checkedLong(BigDecimal.valueOf(maxHeapBytes)
                        .multiply(percentage)
                        .divide(BigDecimal.valueOf(100L))
                        .toBigInteger(), key, value);
            } catch (NumberFormatException e) {
                throw invalidWeight(key, value);
            }
        }
        Matcher matcher = DATA_VOLUME_PATTERN.matcher(normalized);
        if (!matcher.matches()) {
            throw invalidWeight(key, value);
        }
        BigInteger amount = new BigInteger(matcher.group(1));
        String rawUnit = matcher.group(2);
        String unit = rawUnit == null ? "B" : rawUnit.toUpperCase(Locale.ROOT);
        int power;
        switch (unit) {
            case "B":
                power = 0;
                break;
            case "KB":
                power = 1;
                break;
            case "MB":
                power = 2;
                break;
            case "GB":
                power = 3;
                break;
            case "TB":
                power = 4;
                break;
            case "PB":
                power = 5;
                break;
            default:
                throw invalidWeight(key, value);
        }
        return checkedLong(amount.multiply(BigInteger.valueOf(1024L).pow(power)), key, value);
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
     * Returns true when the given property key belongs to one engine's meta cache namespace.
     */
    public static boolean isMetaCacheKeyForEngine(String key, String engine) {
        return key != null && engine != null && key.startsWith(metaCacheKeyPrefix(engine));
    }

    /**
     * Convert ttlSecond to the optional expiry used by the cache runtime.
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

    private static OptionalLong getWeightProperty(Map<String, String> properties, String key) {
        if (key == null) {
            return OptionalLong.empty();
        }
        String value = properties.get(key);
        if (value == null) {
            return OptionalLong.empty();
        }
        try {
            long parsed = parseWeight(value, key, false, 0L);
            if (parsed <= 0L) {
                throw invalidWeight(key, value);
            }
            return OptionalLong.of(parsed);
        } catch (IllegalArgumentException e) {
            LOG.warn("Ignoring invalid persisted metadata cache weight property {}={}", key, value);
            return OptionalLong.empty();
        }
    }

    private static long checkedLong(BigInteger value, String key, String rawValue) {
        if (value.signum() < 0 || value.compareTo(LONG_MAX) > 0) {
            throw invalidWeight(key, rawValue);
        }
        return value.longValue();
    }

    private static IllegalArgumentException invalidWeight(String key, String value) {
        return new IllegalArgumentException("Invalid cache weight for '" + key + "': " + value);
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
        return isCacheEnabled(enable, ttlSecond, capacity)
                && (!maxWeight.isPresent() || maxWeight.getAsLong() != 0L);
    }

    public static final class PropertySpec {
        private final String enableKey;
        private final boolean defaultEnable;
        private final String ttlKey;
        private final long defaultTtlSecond;
        private final String capacityKey;
        private final long defaultCapacity;
        private final String maxWeightKey;

        private PropertySpec(String enableKey, boolean defaultEnable, String ttlKey,
                long defaultTtlSecond, String capacityKey, long defaultCapacity, String maxWeightKey) {
            this.enableKey = enableKey;
            this.defaultEnable = defaultEnable;
            this.ttlKey = ttlKey;
            this.defaultTtlSecond = defaultTtlSecond;
            this.capacityKey = capacityKey;
            this.defaultCapacity = defaultCapacity;
            this.maxWeightKey = maxWeightKey;
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

        public static final class Builder {
            private String enableKey;
            private boolean defaultEnable;
            private String ttlKey;
            private long defaultTtlSecond;
            private String capacityKey;
            private long defaultCapacity;
            private String maxWeightKey;

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

            public Builder maxWeight(String key) {
                this.maxWeightKey = Objects.requireNonNull(key, "key");
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
                        maxWeightKey);
            }
        }
    }
}
