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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzerTest {

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

        Set<String> bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns);
        Assert.assertEquals(Sets.newHashSet("k1"), bfColumns);
    }

    @Test
    public void testBfColumnsError() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", PrimitiveType.INT));
        columns.add(new Column("k2", PrimitiveType.TINYINT));
        columns.add(new Column("v1", 
                        ScalarType.createType(PrimitiveType.VARCHAR), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("v2", ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.SUM, "0", ""));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();

        // no bf columns
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "");
        try {
            Assert.assertEquals(Sets.newHashSet(), PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns));
        } catch (AnalysisException e) {
            Assert.fail();
        }

        // k3 not exist
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k3");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("column does not exist in table"));
        }

        // tinyint not supported
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k2");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("TINYINT is not supported"));
        }

        // not replace value
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "v2");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Bloom filter index only used in"));
        }

        // reduplicated column
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k1,K1");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns);
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

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COLDOWN_TIME, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(tomorrowTs * 1000));
        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.SSD));
        Assert.assertEquals(tomorrowTs, dataProperty.getCooldownTimeMs() / 1000);
    }
}
