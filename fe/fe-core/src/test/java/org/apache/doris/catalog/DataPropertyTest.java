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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DataPropertyTest {

    @Test
    public void testCooldownTimeMs() throws Exception {
        Config.default_storage_medium = "ssd";
        DataProperty dataProperty = new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        dataProperty = new DataProperty(TStorageMedium.SSD);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        long storageCooldownTimeMs = System.currentTimeMillis() + 24 * 3600 * 1000L;
        dataProperty = new DataProperty(TStorageMedium.SSD, storageCooldownTimeMs, "");
        Assert.assertEquals(storageCooldownTimeMs, dataProperty.getCooldownTimeMs());

        dataProperty = new DataProperty(TStorageMedium.HDD);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());
    }

    @Test
    public void testDefaultMediumAllocationMode() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());

        DataProperty dataProperty2 = new DataProperty(TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS, "");
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty2.getMediumAllocationMode());
    }

    @Test
    public void testStrictFlagRoundTrip() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        dataProperty.setMediumAllocationMode(MediumAllocationMode.STRICT);

        String json = GsonUtils.GSON.toJson(dataProperty);
        Assert.assertTrue("new field must be persisted", json.contains("mediumAllocationMode"));
        Assert.assertTrue(json.contains("STRICT") || json.contains("strict"));

        DataProperty restored = GsonUtils.GSON.fromJson(json, DataProperty.class);
        Assert.assertEquals(MediumAllocationMode.STRICT, restored.getMediumAllocationMode());
        Assert.assertTrue("legacy shim must agree with enum", restored.isStorageMediumSpecified());
    }

    /**
     * On master the old {@code storageMediumSpecified} boolean had no
     * {@code @SerializedName} annotation, so it was never written to the
     * image at all. Simulate replaying such an old image (no mediumAllocationMode
     * key) and verify we fall back to ADAPTIVE rather than NPE / STRICT.
     */
    @Test
    public void testOldImageDeserialisesToAdaptive() throws Exception {
        String legacyJson = "{"
                + "\"storageMedium\":\"HDD\","
                + "\"cooldownTimeMs\":" + DataProperty.MAX_COOLDOWN_TIME_MS + ","
                + "\"storagePolicy\":\"\","
                + "\"isMutable\":true"
                + "}";

        DataProperty restored = GsonUtils.GSON.fromJson(legacyJson, DataProperty.class);
        restored.gsonPostProcess();

        Assert.assertEquals(TStorageMedium.HDD, restored.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, restored.getMediumAllocationMode());
        Assert.assertFalse(restored.isStorageMediumSpecified());
    }

    @Test
    public void testLegacySetterMapsToEnum() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());

        dataProperty.setStorageMediumSpecified(true);
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());

        dataProperty.setStorageMediumSpecified(false);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());
    }

    @Test
    public void testAnalyzeDataPropertyPreservesMediumAllocationModeWhenMediumUnspecified() throws Exception {
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD);
        oldDataProperty.setMediumAllocationMode(MediumAllocationMode.STRICT);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_MUTABLE, "false");

        DataProperty updated = PropertyAnalyzer.analyzeDataProperty(properties, oldDataProperty);
        Assert.assertEquals(MediumAllocationMode.STRICT, updated.getMediumAllocationMode());
        Assert.assertFalse(updated.isMutable());
    }
}
