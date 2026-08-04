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

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertFalse(spec.isEnable());
        Assert.assertEquals(123, spec.getTtlSecond());
        Assert.assertEquals(456, spec.getCapacity());
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

        Assert.assertFalse(spec.isEnable());
        Assert.assertEquals(123, spec.getTtlSecond());
        Assert.assertEquals(456, spec.getCapacity());
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
        Assert.assertTrue(spec.isEnable());
        Assert.assertEquals(0, spec.getTtlSecond());
        Assert.assertEquals(100, spec.getCapacity());
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
        Assert.assertEquals("20", mapped.get("new.ttl"));
        // Missing new key is copied from legacy key.
        Assert.assertEquals("30", mapped.get("new.capacity"));
        // Original map is not modified.
        Assert.assertFalse(properties.containsKey("new.capacity"));
    }

    @Test
    public void testOfSemantics() {
        CacheSpec enabled = CacheSpec.of(true, 60, 100);
        Assert.assertTrue(enabled.isEnable());
        Assert.assertEquals(60, enabled.getTtlSecond());
        Assert.assertEquals(100, enabled.getCapacity());

        CacheSpec zeroTtl = CacheSpec.of(true, 0, 100);
        Assert.assertTrue(zeroTtl.isEnable());
        Assert.assertEquals(0, zeroTtl.getTtlSecond());
        Assert.assertEquals(100, zeroTtl.getCapacity());

        CacheSpec disabled = CacheSpec.of(false, 60, 100);
        Assert.assertFalse(disabled.isEnable());
        Assert.assertEquals(60, disabled.getTtlSecond());
        Assert.assertEquals(100, disabled.getCapacity());
    }

    @Test
    public void testPropertyValidationHelpers() throws Exception {
        CacheSpec.checkBooleanProperty("true", "k.enable");
        CacheSpec.checkBooleanProperty("false", "k.enable");

        try {
            CacheSpec.checkBooleanProperty("on", "k.enable");
            Assert.fail("expected DdlException");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("k.enable"));
        }

        CacheSpec.checkLongProperty("10", 0, "k.ttl");
        try {
            CacheSpec.checkLongProperty("-1", 0, "k.ttl");
            Assert.fail("expected DdlException");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("k.ttl"));
        }
    }

    @Test
    public void testIsCacheEnabled() {
        Assert.assertTrue(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 1));
        Assert.assertFalse(CacheSpec.isCacheEnabled(false, CacheSpec.CACHE_NO_TTL, 1));
        Assert.assertFalse(CacheSpec.isCacheEnabled(true, 0, 1));
        Assert.assertFalse(CacheSpec.isCacheEnabled(true, CacheSpec.CACHE_NO_TTL, 0));
    }

    @Test
    public void testToExpireAfterAccess() {
        OptionalLong noTtl = CacheSpec.toExpireAfterAccess(CacheSpec.CACHE_NO_TTL);
        Assert.assertFalse(noTtl.isPresent());

        OptionalLong disabled = CacheSpec.toExpireAfterAccess(0);
        Assert.assertTrue(disabled.isPresent());
        Assert.assertEquals(0, disabled.getAsLong());

        OptionalLong positive = CacheSpec.toExpireAfterAccess(15);
        Assert.assertTrue(positive.isPresent());
        Assert.assertEquals(15, positive.getAsLong());

        OptionalLong negativeOther = CacheSpec.toExpireAfterAccess(-2);
        Assert.assertTrue(negativeOther.isPresent());
        Assert.assertEquals(0, negativeOther.getAsLong());
    }
}
