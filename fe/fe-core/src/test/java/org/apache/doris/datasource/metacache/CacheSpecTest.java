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

import org.apache.doris.connector.cache.CacheSpec;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

public class CacheSpecTest {

    @Test
    public void testFromPropertiesWithExplicitKeys() {
        Map<String, String> properties = Maps.newHashMap();
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
    public void testFromPropertiesWithPropertySpecBuilder() {
        Map<String, String> properties = Maps.newHashMap();
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
    public void testFromPropertiesWithEngineEntryKeys() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("meta.cache.hive.schema.ttl-second", "0");

        CacheSpec defaultSpec = CacheSpec.fromProperties(
                Maps.newHashMap(),
                "enable", true,
                "ttl", 60,
                "capacity", 100);

        CacheSpec spec = CacheSpec.fromProperties(properties, "hive", "schema", defaultSpec);
        Assertions.assertTrue(spec.isEnable());
        Assertions.assertEquals(0, spec.getTtlSecond());
        Assertions.assertEquals(100, spec.getCapacity());
    }

    @Test
    public void testApplyCompatibilityMap() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("legacy.ttl", "10");
        properties.put("new.ttl", "20");
        properties.put("legacy.capacity", "30");

        Map<String, String> compatibilityMap = Maps.newHashMap();
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
    public void testOfSemantics() {
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
        Assertions.assertEquals(60, disabled.getTtlSecond());
        Assertions.assertEquals(100, disabled.getCapacity());
    }

    @Test
    public void testPropertyValidationHelpers() throws Exception {
        CacheSpec.checkBooleanProperty("true", "k.enable");
        CacheSpec.checkBooleanProperty("false", "k.enable");

        try {
            CacheSpec.checkBooleanProperty("on", "k.enable");
            Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("k.enable"));
        }

        CacheSpec.checkLongProperty("10", 0, "k.ttl");
        try {
            CacheSpec.checkLongProperty("-1", 0, "k.ttl");
            Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().contains("k.ttl"));
        }
    }

    @Test
    public void testIsCacheEnabled() {
        Assertions.assertTrue(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(false, CacheSpec.CACHE_NO_TTL, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(true, 0, 1));
        Assertions.assertFalse(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 0));
    }

    @Test
    public void testToExpireAfterAccess() {
        OptionalLong noTtl = CacheSpec.toExpireAfterAccess(CacheSpec.CACHE_NO_TTL);
        Assertions.assertFalse(noTtl.isPresent());

        OptionalLong disabled = CacheSpec.toExpireAfterAccess(0);
        Assertions.assertTrue(disabled.isPresent());
        Assertions.assertEquals(0, disabled.getAsLong());

        OptionalLong positive = CacheSpec.toExpireAfterAccess(15);
        Assertions.assertTrue(positive.isPresent());
        Assertions.assertEquals(15, positive.getAsLong());

        OptionalLong negativeOther = CacheSpec.toExpireAfterAccess(-2);
        Assertions.assertTrue(negativeOther.isPresent());
        Assertions.assertEquals(0, negativeOther.getAsLong());
    }
}
