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
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzerTest {
    private boolean previousVariantV2;
    private boolean previousVariantEscape;

    @org.junit.Before
    public void enableVariantV2() {
        previousVariantV2 = org.apache.doris.common.Config.enable_variant_v2;
        previousVariantEscape = org.apache.doris.common.Config.enable_variant_escape_dots_in_keys;
        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = true;
        org.apache.doris.common.Config.enable_variant_v2 = true;
    }

    @org.junit.After
    public void restoreVariantV2() {
        org.apache.doris.common.Config.enable_variant_v2 = previousVariantV2;
        org.apache.doris.common.Config.enable_variant_escape_dots_in_keys = previousVariantEscape;
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testVariantTableValidation() throws AnalysisException {
        Column variant = Mockito.mock(Column.class);
        Mockito.when(variant.getType()).thenReturn(Type.VARIANT);
        Index badIndex = new Index(1, "idx", ImmutableList.of("v"), IndexType.INVERTED,
                ImmutableMap.of("field_pattern", "a.bad%2e"), "");
        PropertyAnalyzer.validateVariantTable(false, ImmutableList.of(variant), ImmutableList.of(badIndex));
        Mockito.when(variant.getVariantEnableNestedGroup()).thenReturn(true);
        PropertyAnalyzer.validateVariantTable(false, ImmutableList.of(variant), ImmutableList.of(badIndex));
        Assertions.assertThrows(AnalysisException.class,
                () -> PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(variant), ImmutableList.of()));
        Mockito.when(variant.getVariantEnableNestedGroup()).thenReturn(false);
        Mockito.when(variant.getChildren()).thenReturn(ImmutableList.of(new Column("a.bad%2e", Type.INT)));
        Assertions.assertThrows(AnalysisException.class,
                () -> PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(variant), ImmutableList.of()));
        Mockito.when(variant.getChildren()).thenReturn(ImmutableList.of(new Column("a.good%2E", Type.INT)));
        PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(variant), ImmutableList.of());
        Assertions.assertThrows(AnalysisException.class,
                () -> PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(variant), ImmutableList.of(badIndex)));
        Index goodIndex = new Index(2, "good", ImmutableList.of("v"), IndexType.INVERTED,
                ImmutableMap.of("field_pattern", "a.good%2E*"), "");
        PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(variant), ImmutableList.of(goodIndex));
    }

    @Test
    public void testRejectVariantEscapeAlterAndExternal() throws AnalysisException {
        PropertyAnalyzer.rejectVariantEscapeDotsInKeysAlter(null);
        PropertyAnalyzer.rejectVariantEscapeDotsInKeysAlter(ImmutableMap.of("other", "true"));
        for (String key : ImmutableList.of("variant_escape_dots_in_keys", "VARIANT_ESCAPE_DOTS_IN_KEYS")) {
            for (String value : ImmutableList.of("true", "false", "invalid")) {
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> PropertyAnalyzer.rejectVariantEscapeDotsInKeysAlter(ImmutableMap.of(key, value)));
                Assertions.assertThrows(AnalysisException.class,
                        () -> PropertyAnalyzer.rejectVariantEscapeDotsInKeysExternal(ImmutableMap.of(key, value)));
            }
        }
    }

    @Test
    public void testConsistentVariantSources() throws AnalysisException {
        String key = PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS;
        OlapTable escaped = Mockito.mock(OlapTable.class);
        Mockito.when(escaped.getVariantEscapeDotsInKeys()).thenReturn(true);
        Mockito.when(escaped.getBaseSchema()).thenReturn(ImmutableList.of(new Column("v", Type.VARIANT)));
        OlapTable legacy = Mockito.mock(OlapTable.class);
        Mockito.when(legacy.getBaseSchema()).thenReturn(ImmutableList.of(new Column("v", Type.VARIANT)));
        OlapTable integers = Mockito.mock(OlapTable.class);
        Mockito.when(integers.getBaseSchema()).thenReturn(ImmutableList.of(new Column("k", Type.INT)));
        OlapTable escapedIntegers = Mockito.mock(OlapTable.class);
        Mockito.when(escapedIntegers.getVariantEscapeDotsInKeys()).thenReturn(true);
        Mockito.when(escapedIntegers.getBaseSchema()).thenReturn(ImmutableList.of(new Column("k", Type.INT)));
        Assertions.assertEquals(ImmutableMap.of(), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(escapedIntegers), true));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(escapedIntegers), false));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(integers, escaped), true));
        Assertions.assertEquals(ImmutableMap.of(), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(legacy), true));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "TRUE"), ImmutableList.of(escaped), true));
        Assertions.assertEquals(ImmutableMap.of(key, "false"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "FALSE"), ImmutableList.of(legacy), true));
        for (Map<String, String> props : ImmutableList.of(ImmutableMap.<String, String>of(), ImmutableMap.of(key, "true"), ImmutableMap.of(key, "false"))) {
            Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                    props, ImmutableList.of(legacy, escaped), true));
        }
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "false"), ImmutableList.of(escaped), true));
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "true"), ImmutableList.of(legacy), true));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "true"), ImmutableList.of(integers, Mockito.mock(TableIf.class)), true));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(legacy, escaped), false));
    }

    @Test
    public void testInheritVariantEscapeDotsInKeys() throws AnalysisException {
        String key = PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS;
        OlapTable escaped = Mockito.mock(OlapTable.class);
        Mockito.when(escaped.getVariantEscapeDotsInKeys()).thenReturn(true);
        OlapTable legacy = Mockito.mock(OlapTable.class);
        TableIf external = Mockito.mock(TableIf.class);
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "TRUE"), ImmutableList.of(legacy, external), false));
        Assertions.assertEquals(ImmutableMap.of(key, "false"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "FALSE"), ImmutableList.of(escaped), false));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(escaped), false));
        Assertions.assertEquals(ImmutableMap.of(), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(legacy), false));
        Assertions.assertEquals(ImmutableMap.of(), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(external), false));
        Assertions.assertEquals(ImmutableMap.of(), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(null, ImmutableList.of(), false));
        Assertions.assertEquals(ImmutableMap.of(key, "true"), PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(), ImmutableList.of(legacy, external, escaped), false));
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                ImmutableMap.of(key, "invalid"), ImmutableList.of(escaped), false));
    }

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
        String originFormat = Config.inverted_index_storage_format;
        try {
            Config.inverted_index_storage_format = "V3";
            TInvertedIndexFileStorageFormat result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(null);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

            // Config=V1 with no explicit property: V1 config is ignored, falls through to V3
            Config.inverted_index_storage_format = "V1";
            result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(new HashMap<>());
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

            Map<String, String> propertiesWithV1 = new HashMap<>();
            propertiesWithV1.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v1");
            AnalysisException v1Ex = Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV1));
            Assertions.assertTrue(v1Ex.getMessage().contains(
                    "Inverted index V1 is deprecated and no longer allowed for new index creation."));

            Map<String, String> propertiesWithV2 = new HashMap<>();
            propertiesWithV2.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v2");
            result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV2);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V2, result);

            Map<String, String> propertiesWithV3 = new HashMap<>();
            propertiesWithV3.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "v3");
            result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithV3);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

            // "default" + Config=V1: falls through to V3 (current default when config is not V2)
            Config.inverted_index_storage_format = "V1";
            Map<String, String> propertiesWithDefaultV1 = new HashMap<>();
            propertiesWithDefaultV1.put(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT, "default");
            result = PropertyAnalyzer.analyzeInvertedIndexFileStorageFormat(propertiesWithDefaultV1);
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3, result);

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
    public void testAnalyzePartitionInvertedIndexFileStorageFormat() throws AnalysisException {
        String originFormat = Config.inverted_index_storage_format;
        String originDeployMode = Config.deploy_mode;
        String originCloudUniqueId = Config.cloud_unique_id;
        boolean originPartitionFormatRollout = Config.enable_partition_inverted_index_storage_format_rollout;
        try {
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "";
            Config.enable_partition_inverted_index_storage_format_rollout = true;

            Map<String, String> properties = new HashMap<>();
            Config.inverted_index_storage_format = "SNII";
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "V3");
            Assertions.assertEquals(TInvertedIndexFileStorageFormat.V3,
                    PropertyAnalyzer.analyzePartitionInvertedIndexFileStorageFormat(properties));

            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "default");
            AnalysisException defaultException = Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzePartitionInvertedIndexFileStorageFormat(properties));
            Assertions.assertTrue(defaultException.getMessage().contains("does not support 'default'"));

            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "V1");
            AnalysisException v1Exception = Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzePartitionInvertedIndexFileStorageFormat(properties));
            Assertions.assertTrue(v1Exception.getMessage().contains("only supports V2, V3 and SNII"));

            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "SNII");
            Config.enable_partition_inverted_index_storage_format_rollout = false;
            Assertions.assertNull(PropertyAnalyzer.analyzePartitionInvertedIndexFileStorageFormat(properties));
            Assertions.assertFalse(properties.containsKey(
                    PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT));

            Config.enable_partition_inverted_index_storage_format_rollout = true;
            Config.deploy_mode = "local";
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "SNII");
            AnalysisException localException = Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.analyzePartitionInvertedIndexFileStorageFormat(properties));
            Assertions.assertTrue(localException.getMessage().contains("only supported in cloud mode"));
        } finally {
            Config.inverted_index_storage_format = originFormat;
            Config.deploy_mode = originDeployMode;
            Config.cloud_unique_id = originCloudUniqueId;
            Config.enable_partition_inverted_index_storage_format_rollout = originPartitionFormatRollout;
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
        // The default_variant_* variables can only be set with debug points enabled.
        boolean originalEnableDebugPoints = Config.enable_debug_points;
        try {
            Config.enable_debug_points = true;
            SessionVariable sessionVariable = new SessionVariable();
            UnsupportedOperationException e = Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> sessionVariable.checkDefaultVariantMaxSparseColumnStatisticsSize("0"));
            Assertions.assertTrue(e.getMessage().contains("it must between 1 and 50000"), e.getMessage());
            sessionVariable.checkDefaultVariantMaxSparseColumnStatisticsSize("1");
        } finally {
            Config.enable_debug_points = originalEnableDebugPoints;
        }
    }

    @Test
    public void testEscapedVariantPropertyAndStoragePathCodec() throws Exception {
        Map<String, String> properties = new HashMap<>();
        Assertions.assertFalse(PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(properties));
        for (String value : new String[] {"true", "TRUE", "false", "FALSE"}) {
            properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS, value);
            Assertions.assertEquals(Boolean.parseBoolean(value),
                    PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(properties));
            Assertions.assertTrue(properties.isEmpty());
        }
        properties.put(PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS, "invalid");
        Assertions.assertThrows(AnalysisException.class,
                () -> PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(properties));
        for (String key : new String[] {"", "a.b", "50%", "a%2Eb", "%25", "ordinary"}) {
            String encoded = PropertyAnalyzer.encodeVariantPathComponent(key);
            Assertions.assertEquals(key, PropertyAnalyzer.decodeVariantPathComponent(encoded));
            PropertyAnalyzer.validateVariantStoragePath("parent." + encoded);
        }
        for (String invalid : new String[] {"%", "%2", "%2e", "%20", "a%25%"}) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> PropertyAnalyzer.validateVariantStoragePath("parent." + invalid));
        }
    }

    @Test
    public void testEscapeRequiresPersistedV2Policy() throws Exception {
        Config.enable_variant_v2 = false;
        String key = PropertyAnalyzer.PROPERTIES_VARIANT_ESCAPE_DOTS_IN_KEYS;
        Assertions.assertFalse(PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(new HashMap<>()));
        Assertions.assertFalse(PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(
                new HashMap<>(ImmutableMap.of(key, "false"))));
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.analyzeVariantEscapeDotsInKeys(
                new HashMap<>(ImmutableMap.of(key, "true"))));
        Assertions.assertDoesNotThrow(
                () -> PropertyAnalyzer.validateVariantTable(true, ImmutableList.of(), ImmutableList.of()));
        OlapTable source = Mockito.mock(OlapTable.class);
        Mockito.when(source.getVariantEscapeDotsInKeys()).thenReturn(true);
        Mockito.when(source.getBaseSchema()).thenReturn(ImmutableList.of(new Column("v", Type.VARIANT)));
        for (boolean consistent : new boolean[] {true, false}) {
            Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.inheritVariantEscapeDotsInKeys(
                    ImmutableMap.of(), ImmutableList.of(source), consistent));
        }
        // A compute-only type is transient; enabling it cannot authorize persistent escaped storage.
        Assertions.assertTrue(org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE.isExecutionV2());
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.requireVariantV2ForEscape(true));
    }
}
