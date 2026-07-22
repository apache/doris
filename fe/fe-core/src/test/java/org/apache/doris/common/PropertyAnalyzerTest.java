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
import org.apache.doris.analysis.DateLiteralUtils;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.SessionVariable;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
    public void testAnalyzeFileCacheTtlSeconds() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS, "0");
        Assert.assertEquals(0L, PropertyAnalyzer.analyzeTTL(properties));

        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS,
                String.valueOf(PropertyAnalyzer.MAX_FILE_CACHE_TTL_SECONDS));
        Assert.assertEquals(PropertyAnalyzer.MAX_FILE_CACHE_TTL_SECONDS, PropertyAnalyzer.analyzeTTL(properties));

        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS,
                String.valueOf(PropertyAnalyzer.MAX_FILE_CACHE_TTL_SECONDS + 1L));
        try {
            PropertyAnalyzer.analyzeTTL(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("please use "
                    + PropertyAnalyzer.MAX_FILE_CACHE_TTL_SECONDS));
        }

        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS, String.valueOf(Long.MAX_VALUE));
        try {
            PropertyAnalyzer.analyzeTTL(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Larger values may overflow in BE"));
            Assert.assertTrue(e.getMessage().contains("please use"));
        }

        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS, "invalid");
        try {
            PropertyAnalyzer.analyzeTTL(properties);
            Assert.fail("Expected an AnalysisException to be thrown");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("formats error or is out of range"));
        }
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
        DateLiteral dateLiteral = DateLiteralUtils.createDateLiteral(tomorrowTimeStr, Type.DATETIME);
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
        String originFormat = Config.inverted_index_storage_format;
        try {
            Config.inverted_index_storage_format = "V3";
            TInvertedIndexFileStorageFormat result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(null);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

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
        } finally {
            Config.inverted_index_storage_format = originFormat;
        }
    }

    @Test
    public void testAnalyzeVariantMaxSparseColumnStatisticsSize() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE, "0");
        try {
            PropertyAnalyzer.analyzeVariantMaxSparseColumnStatisticsSize(properties, 0);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE, "1");
        Assertions.assertEquals(1, PropertyAnalyzer.analyzeVariantMaxSparseColumnStatisticsSize(properties, 0));
        Assertions.assertFalse(properties.containsKey(
                PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE));
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE, "-1");
        try {
            PropertyAnalyzer.analyzeVariantMaxSparseColumnStatisticsSize(properties, 0);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE, "50001");
        try {
            PropertyAnalyzer.analyzeVariantMaxSparseColumnStatisticsSize(properties, 0);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_MAX_SPARSE_COLUMN_STATISTICS_SIZE, "invalid");
        try {
            PropertyAnalyzer.analyzeVariantMaxSparseColumnStatisticsSize(properties, 0);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testCheckDefaultVariantMaxSparseColumnStatisticsSize() {
        SessionVariable sessionVariable = new SessionVariable();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> sessionVariable.checkDefaultVariantMaxSparseColumnStatisticsSize("0"));
        sessionVariable.checkDefaultVariantMaxSparseColumnStatisticsSize("1");
    }

    @Test
    public void testAnalyzeSequenceMap() throws AnalysisException {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", PrimitiveType.INT));
        columns.add(new Column("k2", PrimitiveType.TINYINT));
        columns.add(new Column("s1", PrimitiveType.DATETIMEV2));
        columns.add(new Column("s2", PrimitiveType.BIGINT));
        columns.add(new Column("c",
                ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.REPLACE, "", ""));
        columns.add(
                new Column("d", ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "0", ""));
        columns.add(new Column("e", ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "0", ""));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();

        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s1", "c,d");
        // test not unique table
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.AGG_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test conflict with PROPERTIES_FUNCTION_COLUMN
        properties.put(PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "." + PropertyAnalyzer.PROPERTIES_SEQUENCE_TYPE,
                "");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test sequence column data type doesn't correct
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".c", "s1, d");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test sequence column not exists
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s3", "s1,c,d");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test mapping column not exits
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s1", "c,d");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e,f");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test mapping column should not be key column
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s1", "c,d");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e,k1");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test mapping column should cover all value column
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s1", "c");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
            Assertions.fail("Expected AnalysisException was not thrown");
        } catch (AnalysisException e) {
            Assertions.assertNotNull(e.getMessage());
        }
        // test ok
        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s1", "c,d");
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".s2", "e");
        try {
            PropertyAnalyzer.analyzeSeqMapping(properties, columns, KeysType.UNIQUE_KEYS);
        } catch (AnalysisException e) {
            Assert.fail();
        }
    }

    @Test
    public void testStorageCooldownTimeWithTzOffset() throws AnalysisException {
        // A known UTC instant: 2027-06-15 12:30:00 UTC
        ZonedDateTime zdt = ZonedDateTime.of(2027, 6, 15, 12, 30, 0, 0, ZoneOffset.UTC);
        long expectedMillis = zdt.toInstant().toEpochMilli();

        // +00:00 suffix — must be parsed as an instant via TZ_FORMATTER
        String cooldownStr = "2027-06-15 12:30:00+00:00";
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownStr);
        DataProperty dp = PropertyAnalyzer.analyzeDataProperty(properties,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("TZ offset +00:00 should parse as exact UTC instant",
                expectedMillis, dp.getCooldownTimeMs());

        // -05:00 offset — same instant, different representation
        String cooldownStr2 = "2027-06-15 07:30:00-05:00";
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownStr2);
        DataProperty dp2 = PropertyAnalyzer.analyzeDataProperty(properties2,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("TZ offset -05:00 should parse as same UTC instant",
                expectedMillis, dp2.getCooldownTimeMs());
    }

    @Test
    public void testStorageCooldownTimeWithTzOffsetFractionalSeconds() throws AnalysisException {
        // A known UTC instant with fractional seconds: 2027-06-15 12:30:00.123456 UTC
        ZonedDateTime zdt = ZonedDateTime.of(2027, 6, 15, 12, 30, 0, 123456000, ZoneOffset.UTC);
        long expectedMillis = zdt.toInstant().toEpochMilli();

        // TIMESTAMPTZ(6) style string with +00:00 suffix
        String cooldownStr = "2027-06-15 12:30:00.123456+00:00";
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownStr);
        DataProperty dp = PropertyAnalyzer.analyzeDataProperty(properties,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("Fractional seconds with TZ offset should parse correctly",
                expectedMillis, dp.getCooldownTimeMs());

        // TIMESTAMPTZ(0) — no fractional seconds, with +00:00
        String cooldownStr2 = "2027-06-15 12:30:00+00:00";
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownStr2);
        DataProperty dp2 = PropertyAnalyzer.analyzeDataProperty(properties2,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("No fractional seconds (precision 0) should still parse",
                ZonedDateTime.of(2027, 6, 15, 12, 30, 0, 0, ZoneOffset.UTC)
                        .toInstant().toEpochMilli(),
                dp2.getCooldownTimeMs());
    }

    @Test
    public void testStorageCooldownTimeInvalidTzFormat() throws AnalysisException {
        // A value that looks like it has a TZ offset but is malformed
        // (missing minutes in offset) — should fall back to MAX_COOLDOWN_TIME_MS
        // instead of throwing an exception.
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, "2027-06-15 12:30:00+00");
        DataProperty dp = PropertyAnalyzer.analyzeDataProperty(properties,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("Malformed TZ offset should fall back to MAX",
                DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());
    }

    @Test
    public void testStorageCooldownTimeStrictDateValidation() throws AnalysisException {
        // An invalid calendar date with a valid TZ offset (Feb 29 in a
        // non-leap year) must be rejected by the STRICT resolver rather
        // than silently normalized to Feb 28, and should fall back to
        // MAX_COOLDOWN_TIME_MS.
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                "2027-02-29 00:00:00+00:00");
        DataProperty dp = PropertyAnalyzer.analyzeDataProperty(properties,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("Invalid date (Feb 29 in non-leap year) should fall back to MAX",
                DataProperty.MAX_COOLDOWN_TIME_MS, dp.getCooldownTimeMs());

        // Also test an impossible month (month=13)
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                "2027-13-01 00:00:00+00:00");
        DataProperty dp2 = PropertyAnalyzer.analyzeDataProperty(properties2,
                new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals("Invalid month (13) should fall back to MAX",
                DataProperty.MAX_COOLDOWN_TIME_MS, dp2.getCooldownTimeMs());
    }

    @Test
    public void testStorageCooldownTimeDstFallbackDistinctInstants() throws AnalysisException {
        // Two different UTC instants that would map to the same wall-clock
        // hour during America/Chicago DST fall-back (e.g. on 2026-11-01,
        // 01:30 CDT = 06:30 UTC and 01:30 CST = 07:30 UTC) must produce
        // different cooldown timestamps when expressed with explicit +00:00.
        //
        // Use dynamically-derived dates 5 years ahead so the test never
        // ages past System.currentTimeMillis() and gets rejected by the
        // future-time policy in analyzeDataProperty().
        ZonedDateTime baseUtc = ZonedDateTime.now(ZoneOffset.UTC).plusYears(5)
                .withHour(6).withMinute(30).withSecond(0).withNano(0);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

        // Earlier instant (e.g. 06:30 UTC)
        String earlierStr = fmt.format(baseUtc) + "+00:00";
        Map<String, String> propsEarlier = Maps.newHashMap();
        propsEarlier.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        propsEarlier.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, earlierStr);
        DataProperty dpEarlier = PropertyAnalyzer.analyzeDataProperty(propsEarlier,
                new DataProperty(TStorageMedium.SSD));
        long earlierMillis = baseUtc.toInstant().toEpochMilli();
        Assert.assertEquals("Explicit +00:00 should parse earlier DST hour correctly",
                earlierMillis, dpEarlier.getCooldownTimeMs());

        // Later instant (1 hour later, e.g. 07:30 UTC)
        ZonedDateTime laterUtc = baseUtc.plusHours(1);
        String laterStr = fmt.format(laterUtc) + "+00:00";
        Map<String, String> propsLater = Maps.newHashMap();
        propsLater.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        propsLater.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, laterStr);
        DataProperty dpLater = PropertyAnalyzer.analyzeDataProperty(propsLater,
                new DataProperty(TStorageMedium.SSD));
        long laterMillis = laterUtc.toInstant().toEpochMilli();
        Assert.assertEquals("Explicit +00:00 should parse later DST hour correctly",
                laterMillis, dpLater.getCooldownTimeMs());

        // The two instants must be distinct (1 hour apart).
        Assert.assertEquals("DST fall-back hours must differ by exactly 1 hour",
                3600000L, laterMillis - earlierMillis);
    }

    @Test
    public void testStorageCooldownTimeTzZoneIdsWinterTarget() throws AnalysisException {
        // Regression: when the session/JVM timezone is in DST (e.g.
        // America/Chicago in July = UTC-5), a cooldown value with Z/UTC/GMT
        // suffix pointing to a winter month (e.g. 2027-01 = CST, UTC-6) went
        // through the legacy DATETIME path because TZ_OFFSET_PATTERN only
        // matched numeric ±HH:MM offsets.  DateLiteralUtils.createDateLiteral()
        // first shifted the UTC instant using the current DST offset (-05:00),
        // then unixTimestamp() applied the target date's winter offset (-06:00),
        // producing a double-shifted wrong result.
        //
        // The fix normalizes Z/UTC/GMT to +00:00 and routes through
        // TZ_FORMATTER (instant-aware parse), bypassing the legacy path
        // entirely.
        TimeZone originalTimezone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

            // Winter-target instant: 2027-01-01 00:00:00 UTC.
            ZonedDateTime winterZdt = ZonedDateTime.of(
                    2027, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            long expectedMillis = winterZdt.toInstant().toEpochMilli();

            // Test with Z suffix
            Map<String, String> propsZ = Maps.newHashMap();
            propsZ.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
            propsZ.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                    "2027-01-01 00:00:00Z");
            DataProperty dpZ = PropertyAnalyzer.analyzeDataProperty(propsZ,
                    new DataProperty(TStorageMedium.SSD));
            Assert.assertEquals("'Z' suffix winter target must parse as UTC instant",
                    expectedMillis, dpZ.getCooldownTimeMs());

            // Test with UTC suffix
            Map<String, String> propsUtc = Maps.newHashMap();
            propsUtc.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
            propsUtc.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                    "2027-01-01 00:00:00UTC");
            DataProperty dpUtc = PropertyAnalyzer.analyzeDataProperty(propsUtc,
                    new DataProperty(TStorageMedium.SSD));
            Assert.assertEquals("'UTC' suffix winter target must parse as UTC instant",
                    expectedMillis, dpUtc.getCooldownTimeMs());

            // Test with GMT suffix
            Map<String, String> propsGmt = Maps.newHashMap();
            propsGmt.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
            propsGmt.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                    "2027-01-01 00:00:00GMT");
            DataProperty dpGmt = PropertyAnalyzer.analyzeDataProperty(propsGmt,
                    new DataProperty(TStorageMedium.SSD));
            Assert.assertEquals("'GMT' suffix winter target must parse as UTC instant",
                    expectedMillis, dpGmt.getCooldownTimeMs());

            // Summer-target Z with summer-time JVM must also work (baseline).
            ZonedDateTime summerZdt = ZonedDateTime.of(
                    2027, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            long expectedSummer = summerZdt.toInstant().toEpochMilli();
            Map<String, String> propsSummer = Maps.newHashMap();
            propsSummer.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
            propsSummer.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                    "2027-07-01 00:00:00Z");
            DataProperty dpSummer = PropertyAnalyzer.analyzeDataProperty(propsSummer,
                    new DataProperty(TStorageMedium.SSD));
            Assert.assertEquals("Summer target with JVM summer must also work",
                    expectedSummer, dpSummer.getCooldownTimeMs());
        } finally {
            TimeZone.setDefault(originalTimezone);
        }
    }
}
