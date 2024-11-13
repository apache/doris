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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
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
}
