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

package org.apache.doris.common;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzerTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testBfColumns() throws AnalysisException {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", PrimitiveType.INT));
        columns.add(new Column("k2", PrimitiveType.TINYINT));
        columns.add(new Column("v1",
                        ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("v2",
                        ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.SUM, "0", ""));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k1");

        Set<String> bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        Assert.assertEquals(Sets.newHashSet("k1"), bfColumns);
    }

    @Test
    public void testBfColumnsError() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", PrimitiveType.INT));
        columns.add(new Column("k2", PrimitiveType.TINYINT));
        columns.add(new Column("k3", PrimitiveType.BOOLEAN));
        columns.add(new Column("v1",
                        ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("v2", ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.SUM, "0", ""));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();

        // no bf columns
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "");
        try {
            Assert.assertEquals(Sets.newHashSet(), PropertyAnalyzer.analyzeBloomFilterColumns(
                    properties, columns, KeysType.AGG_KEYS));
        } catch (AnalysisException e) {
            Assert.fail();
        }

        // k4 not exist
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k4");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("column does not exist in table"));
        }

        // tinyint not supported
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k2");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("TINYINT is not supported"));
        }

        // bool not supported
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k3");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("BOOLEAN is not supported"));
        }

        // not replace value
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "v2");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Bloom filter index should only be used"));
        }

        // reduplicated column
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k1,K1");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, KeysType.AGG_KEYS);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Reduplicated bloom filter column"));
        }
    }

    @Test
    public void testBfFpp() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "0.05");
        Assert.assertEquals(0.05, PropertyAnalyzer.analyzeBloomFilterFpp(properties), 0.0001);
    }

    @Test
    public void testStorageMedium() throws AnalysisException {
        long tomorrowTs = System.currentTimeMillis() / 1000 + 86400;
        String tomorrowTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(tomorrowTs * 1000));

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, tomorrowTimeStr);
        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.SSD));
        // avoid UT fail because time zone different
        DateLiteral dateLiteral = new DateLiteral(tomorrowTimeStr, Type.DATETIME);
        Assert.assertEquals(dateLiteral.unixTimestamp(TimeUtils.getTimeZone()), dataProperty.getCooldownTimeMs());
    }

    @Test
    public void testStorageFormat() throws AnalysisException {
        HashMap<String, String> propertiesV1 = Maps.newHashMap();
        HashMap<String, String> propertiesV2 = Maps.newHashMap();
        HashMap<String, String> propertiesDefault = Maps.newHashMap();
        propertiesV1.put(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, "v1");
        propertiesV2.put(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, "v2");
        propertiesDefault.put(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, "default");
        Assert.assertEquals(TStorageFormat.V2, PropertyAnalyzer.analyzeStorageFormat(null));
        Assert.assertEquals(TStorageFormat.V2, PropertyAnalyzer.analyzeStorageFormat(propertiesV2));
        Assert.assertEquals(TStorageFormat.V2, PropertyAnalyzer.analyzeStorageFormat(propertiesDefault));
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage(
                "Storage format V1 has been deprecated since version 0.14," + " please use V2 instead");
        PropertyAnalyzer.analyzeStorageFormat(propertiesV1);
    }

    @Test
    public void testTag() throws AnalysisException {
        HashMap<String, String> properties = Maps.newHashMap();
        properties.put("tag.location", "l1");
        properties.put("other", "prop");
        Map<String, String> tagMap = PropertyAnalyzer.analyzeBackendTagsProperties(properties, null);
        Assert.assertEquals("l1", tagMap.get("location"));
        Assert.assertEquals(1, tagMap.size());
        Assert.assertEquals(1, properties.size());

        properties.clear();
        tagMap = PropertyAnalyzer.analyzeBackendTagsProperties(properties, Tag.DEFAULT_BACKEND_TAG);
        Assert.assertEquals(1, tagMap.size());
        Assert.assertEquals(Tag.DEFAULT_BACKEND_TAG.value, tagMap.get(Tag.TYPE_LOCATION));
    }

    @Test
    public void testStorageDictPageSize() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();

        // Test default value
        Assert.assertEquals(PropertyAnalyzer.STORAGE_DICT_PAGE_SIZE_DEFAULT_VALUE,
                PropertyAnalyzer.analyzeStorageDictPageSize(properties));

        // Test valid value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "8192"); // 8KB
        Assert.assertEquals(8192, PropertyAnalyzer.analyzeStorageDictPageSize(properties));

        // Test lower boundary value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "4096"); // 4KB
        Assert.assertEquals(4096, PropertyAnalyzer.analyzeStorageDictPageSize(properties));

        // Test upper boundary value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "10485760"); // 10MB
        Assert.assertEquals(10485760, PropertyAnalyzer.analyzeStorageDictPageSize(properties));

        // Test invalid number format
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "invalid");
        try {
            PropertyAnalyzer.analyzeStorageDictPageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid storage dict page size"));
        }

        // Test value below minimum limit
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "-1024"); // 1KB
        try {
            PropertyAnalyzer.analyzeStorageDictPageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage dict page size must be between 0 and 100MB"));
        }

        // Test value above maximum limit
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE, "209715200"); // 200MB
        try {
            PropertyAnalyzer.analyzeStorageDictPageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage dict page size must be between 0 and 100MB"));
        }
    }

    @Test
    public void testStoragePageSize() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();

        // Test default value
        Assert.assertEquals(PropertyAnalyzer.STORAGE_PAGE_SIZE_DEFAULT_VALUE,
                PropertyAnalyzer.analyzeStoragePageSize(properties));

        // Test valid value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "8192"); // 8KB
        Assert.assertEquals(8192, PropertyAnalyzer.analyzeStoragePageSize(properties));

        // Test lower boundary value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "4096"); // 4KB
        Assert.assertEquals(4096, PropertyAnalyzer.analyzeStoragePageSize(properties));

        // Test upper boundary value
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "10485760"); // 10MB
        Assert.assertEquals(10485760, PropertyAnalyzer.analyzeStoragePageSize(properties));

        // Test invalid number format
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "invalid");
        try {
            PropertyAnalyzer.analyzeStoragePageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid storage page size"));
        }

        // Test value below minimum limit
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "1024"); // 1KB
        try {
            PropertyAnalyzer.analyzeStoragePageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage page size must be between 4KB and 10MB"));
        }

        // Test value above maximum limit
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE, "20971520"); // 20MB
        try {
            PropertyAnalyzer.analyzeStoragePageSize(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Storage page size must be between 4KB and 10MB"));
        }
    }

    @Test
    public void testAnalyzeInvertedIndexFileStorageFormat() throws AnalysisException {
        TInvertedIndexFileStorageFormat result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(null);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V2, result);

        Config.inverted_index_storage_format = "V1";
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(new HashMap<>());
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V1, result);

        Map<String, String> propertiesWithV1 = new HashMap<>();
        propertiesWithV1.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v1");
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV1);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V1, result);

        Map<String, String> propertiesWithV2 = new HashMap<>();
        propertiesWithV2.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v2");
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV2);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V2, result);

        Map<String, String> propertiesWithV3 = new HashMap<>();
        propertiesWithV3.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v3");
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV3);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

        Config.inverted_index_storage_format = "V1";
        Map<String, String> propertiesWithDefaultV1 = new HashMap<>();
        propertiesWithDefaultV1.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "default");
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithDefaultV1);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V1, result);

        Config.inverted_index_storage_format = "V2";
        Map<String, String> propertiesWithDefaultV2 = new HashMap<>();
        propertiesWithDefaultV2.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "default");
        result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithDefaultV2);
        Assertions.assertEquals(TInvertedIndexFileStorageFormat.V2, result);

        Map<String, String> propertiesWithUnknown = new HashMap<>();
        propertiesWithUnknown.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "unknown_format");
        try {
            PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithUnknown);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertEquals(
                    "errCode = 2, detailMessage = unknown inverted index storage format: unknown_format",
                    e.getMessage());
        }
    }

    @Test
    public void testAnalyzeDataProperty() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());

        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());

        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());

        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());

        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());

        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());
    }

    @Test
    public void testAnalyzeDataPropertyWithInvalidMediumAllocationMode() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "invalid");
        try {
            PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
            Assert.fail("Should throw AnalysisException for invalid medium_allocation_mode value");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid medium_allocation_mode value"));
        }
    }

    @Test
    public void testMediumAllocationModeFromStringValidInputs() throws AnalysisException {
        // Test valid inputs: "strict", "adaptive", case variations
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("strict"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("adaptive"));

        // Test case insensitive
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("STRICT"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("ADAPTIVE"));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("Strict"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("Adaptive"));

        // Test with whitespace (should be trimmed)
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString(" strict "));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString(" adaptive "));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("\tstrict\n"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("\tadaptive\n"));
    }

    @Test
    public void testMediumAllocationModeFromStringInvalidInputs() {
        // Test null input
        try {
            MediumAllocationMode.fromString(null);
            Assert.fail("Expected AnalysisException for null input");
        } catch (AnalysisException e) {
            Assert.assertEquals("errCode = 2, detailMessage = medium_allocation_mode cannot be null or empty", e.getMessage());
        }

        // Test empty string
        try {
            MediumAllocationMode.fromString("");
            Assert.fail("Expected AnalysisException for empty string");
        } catch (AnalysisException e) {
            Assert.assertEquals("errCode = 2, detailMessage = medium_allocation_mode cannot be null or empty", e.getMessage());
        }

        // Test whitespace only
        try {
            MediumAllocationMode.fromString("   ");
            Assert.fail("Expected AnalysisException for whitespace only");
        } catch (AnalysisException e) {
            Assert.assertEquals("errCode = 2, detailMessage = medium_allocation_mode cannot be null or empty", e.getMessage());
        }

        // Test invalid string
        try {
            MediumAllocationMode.fromString("invalid");
            Assert.fail("Expected AnalysisException for invalid string");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid medium_allocation_mode value: 'invalid'"));
            Assert.assertTrue(e.getMessage().contains("Valid options are: 'strict', 'adaptive'"));
        }

        // Test another invalid string
        try {
            MediumAllocationMode.fromString("random");
            Assert.fail("Expected AnalysisException for invalid string");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid medium_allocation_mode value: 'random'"));
            Assert.assertTrue(e.getMessage().contains("Valid options are: 'strict', 'adaptive'"));
        }

        // Test partial match (should not work)
        try {
            MediumAllocationMode.fromString("str");
            Assert.fail("Expected AnalysisException for partial match");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid medium_allocation_mode value: 'str'"));
            Assert.assertTrue(e.getMessage().contains("Valid options are: 'strict', 'adaptive'"));
        }
    }

    @Test
    public void testMediumAllocationModeErrorMessageFormat() {
        // Test that error messages are properly formatted and informative
        try {
            MediumAllocationMode.fromString("wrong_value");
            Assert.fail("Expected AnalysisException");
        } catch (AnalysisException e) {
            String message = e.getMessage();
            // Verify error message contains the invalid value
            Assert.assertTrue("Error message should contain the invalid value",
                            message.contains("'wrong_value'"));
            // Verify error message lists valid options
            Assert.assertTrue("Error message should list 'strict' as valid option",
                            message.contains("'strict'"));
            Assert.assertTrue("Error message should list 'adaptive' as valid option",
                            message.contains("'adaptive'"));
            // Verify error message format (considering the errCode prefix)
            Assert.assertTrue("Error message should contain 'Invalid medium_allocation_mode value'",
                            message.contains("Invalid medium_allocation_mode value"));
            Assert.assertTrue("Error message should contain 'Valid options are'",
                            message.contains("Valid options are"));
        }
    }

    @Test
    public void testMediumAllocationModeHelperMethods() {
        // Test isStrict() method
        Assert.assertTrue("STRICT policy should return true for isStrict()",
                         MediumAllocationMode.STRICT.isStrict());
        Assert.assertFalse("ADAPTIVE policy should return false for isStrict()",
                          MediumAllocationMode.ADAPTIVE.isStrict());

        // Test isAdaptive() method
        Assert.assertFalse("STRICT policy should return false for isAdaptive()",
                          MediumAllocationMode.STRICT.isAdaptive());
        Assert.assertTrue("ADAPTIVE policy should return true for isAdaptive()",
                         MediumAllocationMode.ADAPTIVE.isAdaptive());

        // Test getValue() method
        Assert.assertEquals("STRICT policy should return 'strict'",
                           "strict", MediumAllocationMode.STRICT.getValue());
        Assert.assertEquals("ADAPTIVE policy should return 'adaptive'",
                           "adaptive", MediumAllocationMode.ADAPTIVE.getValue());
    }

    @Test
    public void testValidateMediumAllocationModeSuccess() throws AnalysisException {
        // Test STRICT + SSD storage medium → success
        PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.STRICT, TStorageMedium.SSD);

        // Test STRICT + HDD storage medium → success
        PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.STRICT, TStorageMedium.HDD);

        // Test ADAPTIVE + SSD storage medium → success
        PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.ADAPTIVE, TStorageMedium.SSD);

        // Test ADAPTIVE + HDD storage medium → success
        PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.ADAPTIVE, TStorageMedium.HDD);

        // Test ADAPTIVE + null storage medium → success (adaptive should work with any or no medium)
        PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.ADAPTIVE, null);
    }

    @Test
    public void testValidateMediumAllocationModeFailure() {
        // Test STRICT + null storage medium → failure
        try {
            PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.STRICT, null);
            Assert.fail("Expected AnalysisException for STRICT policy without storage_medium");
        } catch (AnalysisException e) {
            String message = e.getMessage();
            Assert.assertTrue("Error message should contain 'medium_allocation_mode 'strict' requires storage_medium'",
                            message.contains("medium_allocation_mode 'strict' requires storage_medium to be specified"));
            Assert.assertTrue("Error message should suggest setting storage_medium to 'SSD' or 'HDD'",
                            message.contains("Please set storage_medium to 'SSD' or 'HDD'"));
            Assert.assertTrue("Error message should suggest using 'adaptive' policy",
                            message.contains("or use medium_allocation_mode 'adaptive'"));
        }
    }

    @Test
    public void testValidateMediumAllocationModeErrorMessage() {
        // Test that error message is properly formatted and helpful
        try {
            PropertyAnalyzer.validateMediumAllocationMode(MediumAllocationMode.STRICT, null);
            Assert.fail("Expected AnalysisException");
        } catch (AnalysisException e) {
            String expectedMessage = "medium_allocation_mode 'strict' requires storage_medium to be specified. "
                                   + "Please set storage_medium to 'SSD' or 'HDD', or use medium_allocation_mode 'adaptive'";
            Assert.assertTrue("Error message should match expected format",
                            e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testAnalyzeDataPropertyWithValidationIntegration() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();

        // Test STRICT + SSD → success
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());
        Assert.assertEquals(TStorageMedium.SSD, dataProperty.getStorageMedium());

        // Test STRICT + HDD → success
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals(MediumAllocationMode.STRICT, dataProperty.getMediumAllocationMode());
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());

        // Test ADAPTIVE + null storage medium → success
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.HDD));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, dataProperty.getMediumAllocationMode());
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium()); // Should keep old value
    }

    @Test
    public void testTablePropertyBuildMediumAllocationModeUpgradeScenarios() {
        // Test upgrade scenario with storage_medium
        Map<String, String> propertiesWithStorageMedium = Maps.newHashMap();
        propertiesWithStorageMedium.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");

        TableProperty tableProperty1 = new TableProperty(propertiesWithStorageMedium);
        tableProperty1.buildStorageMedium(); // Build storage medium first
        tableProperty1.buildMediumAllocationMode();

        Assert.assertEquals("Should auto-assign STRICT for table with storage_medium",
                           MediumAllocationMode.STRICT, tableProperty1.getMediumAllocationMode());

        // Test upgrade scenario without storage_medium
        Map<String, String> propertiesWithoutStorageMedium = Maps.newHashMap();

        TableProperty tableProperty2 = new TableProperty(propertiesWithoutStorageMedium);
        tableProperty2.buildStorageMedium(); // Build storage medium first (will be null)
        tableProperty2.buildMediumAllocationMode();

        Assert.assertEquals("Should auto-assign ADAPTIVE for table without storage_medium",
                           MediumAllocationMode.ADAPTIVE, tableProperty2.getMediumAllocationMode());
    }

    @Test
    public void testTablePropertyBuildMediumAllocationModeWithExplicitValues() {
        // Test with explicit medium_allocation_mode = strict and storage_medium = HDD
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        properties1.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");

        TableProperty tableProperty1 = new TableProperty(properties1);
        tableProperty1.buildStorageMedium();
        tableProperty1.buildMediumAllocationMode();

        Assert.assertEquals("Should use explicit STRICT policy",
                           MediumAllocationMode.STRICT, tableProperty1.getMediumAllocationMode());

        // Test with explicit medium_allocation_mode = adaptive
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");

        TableProperty tableProperty2 = new TableProperty(properties2);
        tableProperty2.buildStorageMedium();
        tableProperty2.buildMediumAllocationMode();

        Assert.assertEquals("Should use explicit ADAPTIVE policy",
                           MediumAllocationMode.ADAPTIVE, tableProperty2.getMediumAllocationMode());
    }

    @Test
    public void testTablePropertyBuildMediumAllocationModeInvalidValues() {
        // Test invalid medium_allocation_mode value
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "invalid_policy");

        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();

        try {
            tableProperty.buildMediumAllocationMode();
            Assert.fail("Expected RuntimeException for invalid medium_allocation_mode value");
        } catch (RuntimeException e) {
            Assert.assertTrue("Error message should indicate invalid medium_allocation_mode configuration",
                            e.getMessage().contains("Invalid medium_allocation_mode configuration"));
            Assert.assertTrue("Cause should be AnalysisException",
                            e.getCause() instanceof AnalysisException);
            Assert.assertTrue("Cause message should contain invalid value",
                            e.getCause().getMessage().contains("invalid_policy"));
        }
    }

    @Test
    public void testTablePropertyBuildMediumAllocationModeValidationFailure() {
        // Test STRICT policy without storage_medium (should fail validation)
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        // Don't set storage_medium

        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium(); // This will set storageMedium to null

        try {
            tableProperty.buildMediumAllocationMode();
            Assert.fail("Expected RuntimeException for STRICT policy without storage_medium");
        } catch (RuntimeException e) {
            Assert.assertTrue("Error message should indicate invalid medium_allocation_mode configuration",
                            e.getMessage().contains("Invalid medium_allocation_mode configuration"));
            Assert.assertTrue("Cause should be AnalysisException",
                            e.getCause() instanceof AnalysisException);
            Assert.assertTrue("Cause message should indicate STRICT requires storage_medium",
                            e.getCause().getMessage().contains("medium_allocation_mode 'strict' requires storage_medium"));
        }
    }

    @Test
    public void testTablePropertyBuildMediumAllocationModeConsistencyValidation() {
        // Test that validation is properly integrated
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "strict");
        properties1.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");

        TableProperty tableProperty1 = new TableProperty(properties1);
        tableProperty1.buildStorageMedium();
        tableProperty1.buildMediumAllocationMode(); // Should succeed

        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty1.getMediumAllocationMode());
        Assert.assertEquals(TStorageMedium.SSD, tableProperty1.getStorageMedium());

        // Test ADAPTIVE with any storage medium should work
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");

        TableProperty tableProperty2 = new TableProperty(properties2);
        tableProperty2.buildStorageMedium();
        tableProperty2.buildMediumAllocationMode(); // Should succeed

        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty2.getMediumAllocationMode());
        Assert.assertEquals(TStorageMedium.HDD, tableProperty2.getStorageMedium());

        // Test ADAPTIVE with null storage medium should work
        Map<String, String> properties3 = Maps.newHashMap();
        properties3.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");

        TableProperty tableProperty3 = new TableProperty(properties3);
        tableProperty3.buildStorageMedium();
        tableProperty3.buildMediumAllocationMode(); // Should succeed

        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty3.getMediumAllocationMode());
        Assert.assertNull(tableProperty3.getStorageMedium());
    }
}
