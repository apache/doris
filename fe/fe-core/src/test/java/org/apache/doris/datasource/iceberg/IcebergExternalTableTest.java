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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Days;
import org.apache.iceberg.transforms.Hours;
import org.apache.iceberg.transforms.Months;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergExternalTableTest {

    @Test
    public void testIsSupportedPartitionTable(@Mocked org.apache.iceberg.Table icebergTable,
                                              @Mocked PartitionSpec spec,
                                              @Mocked PartitionField field,
                                              @Mocked Schema schema) {
        IcebergExternalDatabase database = new IcebergExternalDatabase(null, 1L, "2", "2");
        IcebergExternalTable table = new IcebergExternalTable(1, "1", "1", null, database);
        Map<Integer, PartitionSpec> specs = Maps.newHashMap();
        new MockUp<IcebergExternalTable>() {
            @Mock
            private void makeSureInitialized() {
            }

            @Mock
            public Table getIcebergTable() {
                return icebergTable;
            }
        };
        // Test null
        specs.put(0, null);
        new Expectations() {{
                icebergTable.specs();
                result = specs;
            }};
        table.setTable(icebergTable);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        Assertions.assertFalse(table.isValidRelatedTable());
        new Verifications() {{
                icebergTable.specs();
                times = 1;
            }};
        Assertions.assertTrue(table.isValidRelatedTableCached());
        Assertions.assertFalse(table.validRelatedTableCache());

        // Test spec fields are empty.
        specs.put(0, spec);
        table.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        new Expectations() {{
                icebergTable.specs();
                result = specs;
            }};
        List<PartitionField> fields = Lists.newArrayList();
        new Expectations() {{
                spec.fields();
                result = fields;
            }};
        Assertions.assertFalse(table.isValidRelatedTable());
        new Verifications() {{
                spec.fields();
                times = 1;
            }};
        Assertions.assertTrue(table.isValidRelatedTableCached());
        Assertions.assertFalse(table.validRelatedTableCache());

        // Test spec fields are more than 1.
        specs.put(0, spec);
        table.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        new Expectations() {{
                icebergTable.specs();
                result = specs;
            }};
        fields.add(null);
        fields.add(null);
        new Expectations() {{
                spec.fields();
                result = fields;
            }};
        Assertions.assertFalse(table.isValidRelatedTable());
        new Verifications() {{
                spec.fields();
                times = 1;
            }};
        Assertions.assertTrue(table.isValidRelatedTableCached());
        Assertions.assertFalse(table.validRelatedTableCache());
        fields.clear();

        // Test true
        fields.add(field);
        table.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        new Expectations() {
            {
                icebergTable.schema();
                result = schema;

                schema.findColumnName(anyInt);
                result = "col1";
            }
        };
        new Expectations() {{
                field.transform();
                result = new Hours();
            }};
        Assertions.assertTrue(table.isValidRelatedTable());
        Assertions.assertTrue(table.isValidRelatedTableCached());
        Assertions.assertTrue(table.validRelatedTableCache());
        new Verifications() {{
                schema.findColumnName(anyInt);
                times = 1;
            }};
        new Expectations() {{
                field.transform();
                result = new Days();
            }};
        table.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        Assertions.assertTrue(table.isValidRelatedTable());
        new Expectations() {{
                field.transform();
                result = new Months();
            }};
        table.setIsValidRelatedTableCached(false);
        Assertions.assertFalse(table.isValidRelatedTableCached());
        Assertions.assertTrue(table.isValidRelatedTable());
        Assertions.assertTrue(table.isValidRelatedTableCached());
        Assertions.assertTrue(table.validRelatedTableCache());
    }

    @Test
    public void testGetPartitionRange() throws AnalysisException {
        IcebergExternalDatabase database = new IcebergExternalDatabase(null, 1L, "2", "2");
        IcebergExternalTable table = new IcebergExternalTable(1, "1", "1", null, database);
        Column c = new Column("ts", PrimitiveType.DATETIMEV2);
        List<Column> partitionColumns = Lists.newArrayList(c);
        table.setPartitionColumns(partitionColumns);

        // Test null partition value
        Range<PartitionKey> nullRange = table.getPartitionRange(null, "hour", partitionColumns);
        Assertions.assertEquals("0000-01-01 00:00:00",
                nullRange.lowerEndpoint().getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("0000-01-01 00:00:01",
                nullRange.upperEndpoint().getPartitionValuesAsStringList().get(0));

        // Test hour transform.
        Range<PartitionKey> hour = table.getPartitionRange("100", "hour", partitionColumns);
        PartitionKey lowKey = hour.lowerEndpoint();
        PartitionKey upKey = hour.upperEndpoint();
        Assertions.assertEquals("1970-01-05 04:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1970-01-05 05:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test day transform.
        Range<PartitionKey> day = table.getPartitionRange("100", "day", partitionColumns);
        lowKey = day.lowerEndpoint();
        upKey = day.upperEndpoint();
        Assertions.assertEquals("1970-04-11 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1970-04-12 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test month transform.
        Range<PartitionKey> month = table.getPartitionRange("100", "month", partitionColumns);
        lowKey = month.lowerEndpoint();
        upKey = month.upperEndpoint();
        Assertions.assertEquals("1978-05-01 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1978-06-01 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test year transform.
        Range<PartitionKey> year = table.getPartitionRange("100", "year", partitionColumns);
        lowKey = year.lowerEndpoint();
        upKey = year.upperEndpoint();
        Assertions.assertEquals("2070-01-01 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("2071-01-01 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test unsupported transform
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            table.getPartitionRange("100", "bucket", partitionColumns);
        });
        Assertions.assertEquals("Unsupported transform bucket", exception.getMessage());
    }

    @Test
    public void testSortRange() throws AnalysisException {
        IcebergExternalDatabase database = new IcebergExternalDatabase(null, 1L, "2", "2");
        IcebergExternalTable table = new IcebergExternalTable(1, "1", "1", null, database);
        Column c = new Column("c", PrimitiveType.DATETIMEV2);
        ArrayList<Column> columns = Lists.newArrayList(c);
        table.setPartitionColumns(Lists.newArrayList(c));
        PartitionItem nullRange = new RangePartitionItem(table.getPartitionRange(null, "hour", columns));
        PartitionItem year1970 = new RangePartitionItem(table.getPartitionRange("0", "year", columns));
        PartitionItem year1971 = new RangePartitionItem(table.getPartitionRange("1", "year", columns));
        PartitionItem month197002 = new RangePartitionItem(table.getPartitionRange("1", "month", columns));
        PartitionItem month197103 = new RangePartitionItem(table.getPartitionRange("14", "month", columns));
        PartitionItem month197204 = new RangePartitionItem(table.getPartitionRange("27", "month", columns));
        PartitionItem day19700202 = new RangePartitionItem(table.getPartitionRange("32", "day", columns));
        PartitionItem day19730101 = new RangePartitionItem(table.getPartitionRange("1096", "day", columns));
        Map<String, PartitionItem> map = Maps.newHashMap();
        map.put("nullRange", nullRange);
        map.put("year1970", year1970);
        map.put("year1971", year1971);
        map.put("month197002", month197002);
        map.put("month197103", month197103);
        map.put("month197204", month197204);
        map.put("day19700202", day19700202);
        map.put("day19730101", day19730101);
        List<Map.Entry<String, PartitionItem>> entries = table.sortPartitionMap(map);
        Assertions.assertEquals(8, entries.size());
        Assertions.assertEquals("nullRange", entries.get(0).getKey());
        Assertions.assertEquals("year1970", entries.get(1).getKey());
        Assertions.assertEquals("month197002", entries.get(2).getKey());
        Assertions.assertEquals("day19700202", entries.get(3).getKey());
        Assertions.assertEquals("year1971", entries.get(4).getKey());
        Assertions.assertEquals("month197103", entries.get(5).getKey());
        Assertions.assertEquals("month197204", entries.get(6).getKey());
        Assertions.assertEquals("day19730101", entries.get(7).getKey());

        Map<String, Set<String>> stringSetMap = table.mergeOverlapPartitions(map);
        Assertions.assertEquals(2, stringSetMap.size());
        Assertions.assertTrue(stringSetMap.containsKey("year1970"));
        Assertions.assertTrue(stringSetMap.containsKey("year1971"));

        Set<String> names1970 = stringSetMap.get("year1970");
        Assertions.assertEquals(3, names1970.size());
        Assertions.assertTrue(names1970.contains("year1970"));
        Assertions.assertTrue(names1970.contains("month197002"));
        Assertions.assertTrue(names1970.contains("day19700202"));

        Set<String> names1971 = stringSetMap.get("year1971");
        Assertions.assertEquals(2, names1971.size());
        Assertions.assertTrue(names1971.contains("year1971"));
        Assertions.assertTrue(names1971.contains("month197103"));

        Assertions.assertEquals(5, map.size());
        Assertions.assertTrue(map.containsKey("nullRange"));
        Assertions.assertTrue(map.containsKey("year1970"));
        Assertions.assertTrue(map.containsKey("year1971"));
        Assertions.assertTrue(map.containsKey("month197204"));
        Assertions.assertTrue(map.containsKey("day19730101"));
    }
}
