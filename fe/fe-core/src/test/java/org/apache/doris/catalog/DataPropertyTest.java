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

import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;

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
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());
    }

    @Test
    public void testSetMediumAllocationMode() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);

        dataProperty.setMediumAllocationMode(MediumAllocationMode.STRICT);
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());

        dataProperty.setMediumAllocationMode(MediumAllocationMode.ADAPTIVE);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());
    }

    @Test
    public void testConstructorWithMediumAllocationMode() {
        DataProperty dataProperty = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());
    }

    @Test
    public void testCopyConstructor() {
        DataProperty original = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "test_policy",
                true,
                MediumAllocationMode.STRICT
        );

        DataProperty copy = new DataProperty(original);

        Assert.assertEquals(original.getStorageMedium(), copy.getStorageMedium());
        Assert.assertEquals(original.getCooldownTimeMs(), copy.getCooldownTimeMs());
        Assert.assertEquals(original.getStoragePolicy(), copy.getStoragePolicy());
        Assert.assertEquals(original.isMutable(), copy.isMutable());
        Assert.assertEquals(original.getMediumAllocationMode(), copy.getMediumAllocationMode());
    }

    @Test
    public void testIsStorageMediumSpecified() {
        DataProperty strictProperty = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );
        Assert.assertTrue(strictProperty.isStorageMediumSpecified());

        DataProperty adaptiveProperty = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.ADAPTIVE
        );
        Assert.assertFalse(adaptiveProperty.isStorageMediumSpecified());
    }

    @Test
    public void testEqualsWithMediumAllocationMode() {
        DataProperty property1 = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        DataProperty property2 = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        DataProperty property3 = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.ADAPTIVE
        );

        Assert.assertEquals(property1, property2);
        Assert.assertNotEquals(property1, property3);
    }

    @Test
    public void testHashCodeWithMediumAllocationMode() {
        DataProperty property1 = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        DataProperty property2 = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        Assert.assertEquals(property1.hashCode(), property2.hashCode());
    }

    @Test
    public void testToStringContainsMediumAllocationMode() {
        DataProperty property = new DataProperty(
                TStorageMedium.SSD,
                DataProperty.MAX_COOLDOWN_TIME_MS,
                "",
                true,
                MediumAllocationMode.STRICT
        );

        String str = property.toString();
        Assert.assertTrue(str.contains("medium allocation mode"));
        Assert.assertTrue(str.contains("STRICT"));
    }

    @Test
    public void testMediumAllocationModeGetValue() {
        Assert.assertEquals("strict", MediumAllocationMode.STRICT.getValue());
        Assert.assertEquals("adaptive", MediumAllocationMode.ADAPTIVE.getValue());
    }

    @Test
    public void testMediumAllocationModeIsStrict() {
        Assert.assertTrue(MediumAllocationMode.STRICT.isStrict());
        Assert.assertFalse(MediumAllocationMode.ADAPTIVE.isStrict());
    }

    @Test
    public void testMediumAllocationModeIsAdaptive() {
        Assert.assertFalse(MediumAllocationMode.STRICT.isAdaptive());
        Assert.assertTrue(MediumAllocationMode.ADAPTIVE.isAdaptive());
    }

    @Test
    public void testMediumAllocationModeFromStringValid() throws AnalysisException {
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("strict"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("adaptive"));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("STRICT"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("ADAPTIVE"));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString(" strict "));
    }

    @Test(expected = AnalysisException.class)
    public void testMediumAllocationModeFromStringInvalidNull() throws AnalysisException {
        MediumAllocationMode.fromString(null);
    }

    @Test(expected = AnalysisException.class)
    public void testMediumAllocationModeFromStringInvalidEmpty() throws AnalysisException {
        MediumAllocationMode.fromString("");
    }

    @Test(expected = AnalysisException.class)
    public void testMediumAllocationModeFromStringInvalidValue() throws AnalysisException {
        MediumAllocationMode.fromString("invalid");
    }

    @Test
    public void testMediumAllocationModeFromStringErrorMessage() {
        try {
            MediumAllocationMode.fromString("wrong");
            Assert.fail("Expected AnalysisException");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid medium_allocation_mode value"));
            Assert.assertTrue(e.getMessage().contains("'wrong'"));
            Assert.assertTrue(e.getMessage().contains("strict"));
            Assert.assertTrue(e.getMessage().contains("adaptive"));
        }
    }
}
