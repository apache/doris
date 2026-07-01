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

package org.apache.doris.connector.api.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Pins the connector-side {@link CacheSpec} validators and parsing.
 *
 * <p><b>WHY this matters:</b> this class restores the legacy CREATE/ALTER CATALOG meta-cache
 * property validation that was dropped at the SPI cutover. The validators MUST throw
 * {@link IllegalArgumentException} (NOT a fe-core {@code DdlException}, which is unavailable here and
 * would not be caught by {@code PluginDrivenExternalCatalog.checkProperties}) and MUST emit the exact
 * legacy message substring {@code "is wrong"} so the user-facing error and the regression assertions
 * (e.g. {@code test_iceberg_table_meta_cache} / {@code test_paimon_table_meta_cache}) still match.
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
}
