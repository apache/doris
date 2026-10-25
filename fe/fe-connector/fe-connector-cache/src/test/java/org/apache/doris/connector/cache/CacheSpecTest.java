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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Pins the shared {@link CacheSpec} validators and parsing after the prior copies and hand-rolled checks were
 * collapsed here.
 *
 * <p><b>WHY this matters:</b> this class restores the legacy CREATE/ALTER CATALOG meta-cache property
 * validation that was dropped at the SPI cutover. The validators MUST throw {@link IllegalArgumentException}
 * (NOT a fe-core {@code DdlException}, which is unavailable here and would not be caught by
 * {@code PluginDrivenExternalCatalog.checkProperties}) and MUST emit the exact legacy message substring
 * {@code "is wrong"} so the user-facing error and the regression assertions (e.g.
 * {@code test_iceberg_table_meta_cache} / {@code test_paimon_table_meta_cache}) still match.
 */
public class CacheSpecTest {

    @Test
    public void checkBooleanPropertyAcceptsTrueFalseAndNull() {
        CacheSpec.checkBooleanProperty("true", "k.enable");
        CacheSpec.checkBooleanProperty("false", "k.enable");
        CacheSpec.checkBooleanProperty("TRUE", "k.enable");
        CacheSpec.checkBooleanProperty(null, "k.enable");
    }

    @Test
    public void checkBooleanPropertyRejectsNonBoolean() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkBooleanProperty("on", "k.enable"));
        Assertions.assertEquals("The parameter k.enable is wrong, value is on", e.getMessage());
    }

    @Test
    public void checkLongPropertyAcceptsInRangeAndNull() {
        CacheSpec.checkLongProperty("10", 0, "k.ttl");
        CacheSpec.checkLongProperty("0", 0, "k.ttl");
        CacheSpec.checkLongProperty(null, 0, "k.ttl");
        // iceberg/paimon ttl min is -1 (the "no expiration" sentinel), so -1 is accepted.
        CacheSpec.checkLongProperty("-1", -1L, "k.ttl");
    }

    @Test
    public void checkLongPropertyRejectsBelowMin() {
        // Legacy hive-style min 0 rejects -1.
        IllegalArgumentException belowZero = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkLongProperty("-1", 0, "k.ttl"));
        Assertions.assertEquals("The parameter k.ttl is wrong, value is -1", belowZero.getMessage());

        // The concrete failing case: -2 with iceberg/paimon min -1.
        IllegalArgumentException belowMinusOne = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkLongProperty("-2", -1L, "meta.cache.iceberg.table.ttl-second"));
        Assertions.assertEquals(
                "The parameter meta.cache.iceberg.table.ttl-second is wrong, value is -2",
                belowMinusOne.getMessage());
        Assertions.assertTrue(belowMinusOne.getMessage().contains("is wrong"));
    }

    @Test
    public void checkLongPropertyRejectsNonNumeric() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkLongProperty("abc", 0, "k.capacity"));
        Assertions.assertEquals("The parameter k.capacity is wrong, value is abc", e.getMessage());
    }

    @Test
    public void fromPropertiesWithExplicitKeys() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k.enable", "false");
        properties.put("k.ttl", "123");
        properties.put("k.capacity", "456");

        CacheSpec spec = CacheSpec.fromProperties(
                properties,
                "k.enable", true,
                "k.ttl", CacheSpec.CACHE_NO_TTL,
                "k.capacity", 100);

        Assertions.assertFalse(spec.isEnable());
        Assertions.assertEquals(123, spec.getTtlSecond());
        Assertions.assertEquals(456, spec.getCapacity());
    }

    @Test
    public void fromPropertiesWithPropertySpecBuilder() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k.enable", "false");
        properties.put("k.ttl", "123");
        properties.put("k.capacity", "456");

        CacheSpec spec = CacheSpec.fromProperties(properties, CacheSpec.propertySpecBuilder()
                .enable("k.enable", true)
                .ttl("k.ttl", CacheSpec.CACHE_NO_TTL)
                .capacity("k.capacity", 100)
                .build());

        Assertions.assertFalse(spec.isEnable());
        Assertions.assertEquals(123, spec.getTtlSecond());
        Assertions.assertEquals(456, spec.getCapacity());
    }

    @Test
    public void fromPropertiesWithEngineEntryKeys() {
        Map<String, String> properties = new HashMap<>();
        properties.put("meta.cache.hive.schema.ttl-second", "0");

        CacheSpec defaultSpec = CacheSpec.fromProperties(
                new HashMap<>(),
                "enable", true,
                "ttl", 60,
                "capacity", 100);

        CacheSpec spec = CacheSpec.fromProperties(properties, "hive", "schema", defaultSpec);
        Assertions.assertTrue(spec.isEnable());
        Assertions.assertEquals(0, spec.getTtlSecond());
        Assertions.assertEquals(100, spec.getCapacity());
    }

    @Test
    public void fromPropertiesIsBestEffort() {
        // Parsing must never throw; a bad value falls back to the default (validation is a separate step).
        Map<String, String> props = new HashMap<>();
        props.put("meta.cache.iceberg.table.enable", "true");
        props.put("meta.cache.iceberg.table.ttl-second", "not-a-number");
        CacheSpec spec = CacheSpec.fromProperties(props, "iceberg", "table",
                CacheSpec.of(false, 3600L, 1000L));
        Assertions.assertTrue(spec.isEnable());
        Assertions.assertEquals(3600L, spec.getTtlSecond());
        Assertions.assertEquals(1000L, spec.getCapacity());
    }

    @Test
    public void applyCompatibilityMap() {
        Map<String, String> properties = new HashMap<>();
        properties.put("legacy.ttl", "10");
        properties.put("new.ttl", "20");
        properties.put("legacy.capacity", "30");

        Map<String, String> compatibilityMap = new HashMap<>();
        compatibilityMap.put("legacy.ttl", "new.ttl");
        compatibilityMap.put("legacy.capacity", "new.capacity");

        Map<String, String> mapped = CacheSpec.applyCompatibilityMap(properties, compatibilityMap);

        // New key keeps precedence if already present.
        Assertions.assertEquals("20", mapped.get("new.ttl"));
        // Missing new key is copied from legacy key.
        Assertions.assertEquals("30", mapped.get("new.capacity"));
        // Original map is not modified.
        Assertions.assertFalse(properties.containsKey("new.capacity"));
    }

    @Test
    public void ofSemantics() {
        CacheSpec enabled = CacheSpec.of(true, 60, 100);
        Assertions.assertTrue(enabled.isEnable());
        Assertions.assertEquals(60, enabled.getTtlSecond());
        Assertions.assertEquals(100, enabled.getCapacity());

        CacheSpec zeroTtl = CacheSpec.of(true, 0, 100);
        Assertions.assertTrue(zeroTtl.isEnable());
        Assertions.assertEquals(0, zeroTtl.getTtlSecond());
        Assertions.assertEquals(100, zeroTtl.getCapacity());

        CacheSpec disabled = CacheSpec.of(false, 60, 100);
        Assertions.assertFalse(disabled.isEnable());
    }

    @Test
    public void ofConnectorTtlFoldsNonPositiveToDisabled() {
        // A positive ttl passes through unchanged and stays enabled.
        CacheSpec positive = CacheSpec.ofConnectorTtl(60, 100);
        Assertions.assertTrue(positive.isEnable());
        Assertions.assertEquals(60, positive.getTtlSecond());
        Assertions.assertEquals(100, positive.getCapacity());
        Assertions.assertTrue(CacheSpec.isCacheEnabled(
                positive.isEnable(), positive.getTtlSecond(), positive.getCapacity()));

        // ttl == 0 is already the disable sentinel and stays disabled.
        CacheSpec zero = CacheSpec.ofConnectorTtl(0, 100);
        Assertions.assertEquals(CacheSpec.CACHE_TTL_DISABLE_CACHE, zero.getTtlSecond());
        Assertions.assertFalse(CacheSpec.isCacheEnabled(
                zero.isEnable(), zero.getTtlSecond(), zero.getCapacity()));

        // The load-bearing case: a NEGATIVE ttl must FOLD to the disable sentinel (0), NOT pass through as the
        // -1 "no expiration (enabled)" sentinel. Without the fold, a negative operator value would silently
        // produce a never-expiring cache -- the exact bug this factory replaces the per-cache ternary to avoid.
        CacheSpec minusOne = CacheSpec.ofConnectorTtl(CacheSpec.CACHE_NO_TTL, 100);
        Assertions.assertEquals(CacheSpec.CACHE_TTL_DISABLE_CACHE, minusOne.getTtlSecond());
        Assertions.assertFalse(CacheSpec.isCacheEnabled(
                minusOne.isEnable(), minusOne.getTtlSecond(), minusOne.getCapacity()));

        CacheSpec minusTwo = CacheSpec.ofConnectorTtl(-2, 100);
        Assertions.assertEquals(CacheSpec.CACHE_TTL_DISABLE_CACHE, minusTwo.getTtlSecond());
        Assertions.assertFalse(CacheSpec.isCacheEnabled(
                minusTwo.isEnable(), minusTwo.getTtlSecond(), minusTwo.getCapacity()));
    }

    @Test
    public void isCacheEnabledMatchesLegacyFormula() {
        Assertions.assertTrue(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(false, CacheSpec.CACHE_NO_TTL, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(true, 0, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 0));
    }

    @Test
    public void toExpireAfterAccessMapsSentinels() {
        Assertions.assertFalse(CacheSpec.toExpireAfterAccess(CacheSpec.CACHE_NO_TTL).isPresent());

        OptionalLong disabled = CacheSpec.toExpireAfterAccess(0);
        Assertions.assertTrue(disabled.isPresent());
        Assertions.assertEquals(0, disabled.getAsLong());

        Assertions.assertEquals(15, CacheSpec.toExpireAfterAccess(15).getAsLong());
        // -2 is not the -1 sentinel, so it clamps to 0 (disable), not empty.
        Assertions.assertEquals(0, CacheSpec.toExpireAfterAccess(-2).getAsLong());
    }

    @Test
    public void isMetaCacheKeyForEngine() {
        Assertions.assertTrue(
                CacheSpec.isMetaCacheKeyForEngine("meta.cache.iceberg.table.ttl-second", "iceberg"));
        Assertions.assertFalse(
                CacheSpec.isMetaCacheKeyForEngine("meta.cache.paimon.table.ttl-second", "iceberg"));
        Assertions.assertFalse(CacheSpec.isMetaCacheKeyForEngine(null, "iceberg"));
    }

    @Test
    public void parsesAndValidatesWeightProperties() {
        Assertions.assertEquals(1024L, CacheSpec.parseWeight("1KB", "weight", false, 0L));
        Assertions.assertEquals(512L, CacheSpec.parseWeight("25%", "weight", true, 2048L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.parseWeight("1.5GB", "weight", false, 0L));

        Map<String, String> properties = new HashMap<>();
        properties.put("meta.cache.max-weight", "2GB");
        properties.put("meta.cache.iceberg.table.max-weight", "1GB");
        CacheSpec.checkWeightProperties(properties, "iceberg", "table");

        properties.put("meta.cache.iceberg.table.max-weight", "3GB");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(properties, "iceberg", "table"));

        properties.put("meta.cache.iceberg.table.max-weight", "0");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(properties, "iceberg", "table"));

        properties.put("meta.cache.iceberg.table.max-weight", "1GB");
        properties.put("meta.cache.iceberg.unknown.max-weight", "1MB");
        Assertions.assertDoesNotThrow(() -> CacheSpec.checkWeightProperties(properties, "iceberg", "table"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(properties, properties, "iceberg", "table"));
        Assertions.assertDoesNotThrow(() -> CacheSpec.checkWeightProperties(
                properties, Collections.singletonMap("comment", "updated"), "iceberg", "table"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CacheSpec.checkWeightProperties(
                properties, Collections.singletonMap("meta.cache.iceberg.max-weight", "1MB"), "iceberg", "table"));

        properties.clear();
        properties.put("meta.cache.max-weight", "invalid");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkCatalogWeightProperty(properties));
    }

    @Test
    public void submittedEngineKeysMustNameAKnownOptionWithAValueOfItsType() {
        IllegalArgumentException misspelled = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkSubmittedEngineProperties(
                        Collections.singletonMap("meta.cache.iceberg.table.max-weigth", "16MB"), "iceberg"));
        Assertions.assertEquals("Unknown external meta cache property: meta.cache.iceberg.table.max-weigth",
                misspelled.getMessage());
        for (String key : new String[] {"meta.cache.iceberg.max-weight", "meta.cache.iceberg.enable",
                "meta.cache.iceberg..enable", "meta.cache.iceberg.table."}) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> CacheSpec.checkSubmittedEngineProperties(
                    Collections.singletonMap(key, "1"), "iceberg"), key);
        }
        IllegalArgumentException enable = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkSubmittedEngineProperties(
                        Collections.singletonMap("meta.cache.iceberg.table.enable", "1"), "iceberg"));
        Assertions.assertEquals("The parameter meta.cache.iceberg.table.enable is wrong, value is 1",
                enable.getMessage());
        IllegalArgumentException ttl = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkSubmittedEngineProperties(
                        Collections.singletonMap("meta.cache.iceberg.table.ttl-second", "-2"), "iceberg"));
        Assertions.assertEquals("The parameter meta.cache.iceberg.table.ttl-second is wrong, value is -2",
                ttl.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> CacheSpec.checkSubmittedEngineProperties(
                Collections.singletonMap("meta.cache.iceberg.manifest.capacity", "-1"), "iceberg"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CacheSpec.checkSubmittedEngineProperties(
                Collections.singletonMap("meta.cache.iceberg.manifest.max-weight", "0"), "iceberg"));

        // Entries are not checked: a newer connector may read one this version does not know. Other engines'
        // keys, the catalog weight and plain properties belong to other validators.
        Map<String, String> accepted = new HashMap<>();
        accepted.put("meta.cache.iceberg.schema.ttl-second", "0");
        accepted.put("meta.cache.iceberg.future_entry.enable", "TRUE");
        accepted.put("meta.cache.iceberg.table.ttl-second", "-1");
        accepted.put("meta.cache.iceberg.table.capacity", "0");
        accepted.put("meta.cache.iceberg.table.max-weight", "64MB");
        accepted.put("meta.cache.paimon.table.enable", "1");
        accepted.put("meta.cache.max-weight", "1GB");
        accepted.put("comment", "x");
        CacheSpec.checkSubmittedEngineProperties(accepted, "iceberg");
    }

    @Test
    public void checkWeightPropertiesValidatesOnlyTheSubmittedEngineKeys() {
        Map<String, String> stored = new HashMap<>();
        stored.put("meta.cache.iceberg.table.max-weigth", "16MB");
        stored.put("meta.cache.iceberg.table.enable", "1");
        Assertions.assertDoesNotThrow(() -> CacheSpec.checkWeightProperties(
                stored, Collections.singletonMap("comment", "updated"), "iceberg", "table"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CacheSpec.checkWeightProperties(
                stored, Collections.singletonMap("meta.cache.iceberg.table.enable", "yes"), "iceberg", "table"));
    }

    @Test
    public void submittedWeightsMayNotExceedTheGlobalLimit() {
        OptionalLong global = OptionalLong.of(1024L * 1024 * 1024);
        Map<String, String> catalogAbove = Collections.singletonMap("meta.cache.max-weight", "2GB");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(catalogAbove, catalogAbove, global, "iceberg", "table"));
        Assertions.assertEquals("meta.cache.max-weight (2147483648 bytes) can not exceed the FE global max weight"
                + " external_meta_cache_max_weight (1073741824 bytes)", e.getMessage());
        Map<String, String> catalogAtLimit = Collections.singletonMap("meta.cache.max-weight", "1GB");
        CacheSpec.checkWeightProperties(catalogAtLimit, catalogAtLimit, global, "iceberg", "table");

        // Without a catalog weight the global limit bounds an entry weight; with one the catalog weight does.
        Map<String, String> entryAbove = Collections.singletonMap("meta.cache.iceberg.table.max-weight", "2GB");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(entryAbove, entryAbove, global, "iceberg", "table"));
        Map<String, String> limitedCatalog = new HashMap<>(entryAbove);
        limitedCatalog.put("meta.cache.max-weight", "1GB");
        IllegalArgumentException parent = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.checkWeightProperties(limitedCatalog, entryAbove, global, "iceberg", "table"));
        Assertions.assertEquals("meta.cache.iceberg.table.max-weight can not exceed meta.cache.max-weight",
                parent.getMessage());

        // A stored weight is not compared again, e.g. after the limit was lowered, and no limit bounds nothing.
        CacheSpec.checkWeightProperties(catalogAbove, Collections.singletonMap("comment", "updated"), global,
                "iceberg", "table");
        CacheSpec.checkWeightProperties(catalogAbove, catalogAbove, OptionalLong.empty(), "iceberg", "table");
        CacheSpec.checkSubmittedWeightsAgainstGlobal(entryAbove, entryAbove, global, "paimon");
    }

    @Test
    public void invalidPersistedEntryWeightFallsBackToUnbounded() {
        Map<String, String> properties = new HashMap<>();
        properties.put("meta.cache.hive.file.max-weight", "invalid");
        CacheSpec spec = CacheSpec.fromProperties(
                properties, "hive", "file", CacheSpec.of(true, 60L, 100L));
        Assertions.assertFalse(spec.getMaxWeight().isPresent());

        properties.put("meta.cache.hive.file.max-weight", "0");
        spec = CacheSpec.fromProperties(properties, "hive", "file",
                CacheSpec.of(true, 10L, 100L));
        Assertions.assertFalse(spec.getMaxWeight().isPresent());
    }
}
