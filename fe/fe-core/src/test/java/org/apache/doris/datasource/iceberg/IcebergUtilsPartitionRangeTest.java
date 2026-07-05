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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergUtilsPartitionRangeTest {

    @Test
    public void testGetPartitionRange() throws AnalysisException {
        Column c = new Column("ts", PrimitiveType.DATETIMEV2);
        List<Column> partitionColumns = Lists.newArrayList(c);

        // Test null partition value
        Range<PartitionKey> nullRange = IcebergUtils.getPartitionRange(null, "hour", partitionColumns);
        Assertions.assertEquals("0000-01-01 00:00:00",
                nullRange.lowerEndpoint().getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("0000-01-01 00:00:01",
                nullRange.upperEndpoint().getPartitionValuesAsStringList().get(0));

        // Test hour transform.
        Range<PartitionKey> hour = IcebergUtils.getPartitionRange("100", "hour", partitionColumns);
        PartitionKey lowKey = hour.lowerEndpoint();
        PartitionKey upKey = hour.upperEndpoint();
        Assertions.assertEquals("1970-01-05 04:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1970-01-05 05:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test day transform.
        Range<PartitionKey> day = IcebergUtils.getPartitionRange("100", "day", partitionColumns);
        lowKey = day.lowerEndpoint();
        upKey = day.upperEndpoint();
        Assertions.assertEquals("1970-04-11 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1970-04-12 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test month transform.
        Range<PartitionKey> month = IcebergUtils.getPartitionRange("100", "month", partitionColumns);
        lowKey = month.lowerEndpoint();
        upKey = month.upperEndpoint();
        Assertions.assertEquals("1978-05-01 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("1978-06-01 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test year transform.
        Range<PartitionKey> year = IcebergUtils.getPartitionRange("100", "year", partitionColumns);
        lowKey = year.lowerEndpoint();
        upKey = year.upperEndpoint();
        Assertions.assertEquals("2070-01-01 00:00:00", lowKey.getPartitionValuesAsStringList().get(0));
        Assertions.assertEquals("2071-01-01 00:00:00", upKey.getPartitionValuesAsStringList().get(0));

        // Test unsupported transform
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            IcebergUtils.getPartitionRange("100", "bucket", partitionColumns);
        });
        Assertions.assertEquals("Unsupported transform bucket", exception.getMessage());
    }

    @Test
    public void testSortRange() throws AnalysisException {
        Column c = new Column("c", PrimitiveType.DATETIMEV2);
        ArrayList<Column> columns = Lists.newArrayList(c);
        PartitionItem nullRange = new RangePartitionItem(IcebergUtils.getPartitionRange(null, "hour", columns));
        PartitionItem year1970 = new RangePartitionItem(IcebergUtils.getPartitionRange("0", "year", columns));
        PartitionItem year1971 = new RangePartitionItem(IcebergUtils.getPartitionRange("1", "year", columns));
        PartitionItem month197002 = new RangePartitionItem(IcebergUtils.getPartitionRange("1", "month", columns));
        PartitionItem month197103 = new RangePartitionItem(IcebergUtils.getPartitionRange("14", "month", columns));
        PartitionItem month197204 = new RangePartitionItem(IcebergUtils.getPartitionRange("27", "month", columns));
        PartitionItem day19700202 = new RangePartitionItem(IcebergUtils.getPartitionRange("32", "day", columns));
        PartitionItem day19730101 = new RangePartitionItem(IcebergUtils.getPartitionRange("1096", "day", columns));
        Map<String, PartitionItem> map = Maps.newHashMap();
        map.put("nullRange", nullRange);
        map.put("year1970", year1970);
        map.put("year1971", year1971);
        map.put("month197002", month197002);
        map.put("month197103", month197103);
        map.put("month197204", month197204);
        map.put("day19700202", day19700202);
        map.put("day19730101", day19730101);
        List<Map.Entry<String, PartitionItem>> entries = IcebergUtils.sortPartitionMap(map);
        Assertions.assertEquals(8, entries.size());
        Assertions.assertEquals("nullRange", entries.get(0).getKey());
        Assertions.assertEquals("year1970", entries.get(1).getKey());
        Assertions.assertEquals("month197002", entries.get(2).getKey());
        Assertions.assertEquals("day19700202", entries.get(3).getKey());
        Assertions.assertEquals("year1971", entries.get(4).getKey());
        Assertions.assertEquals("month197103", entries.get(5).getKey());
        Assertions.assertEquals("month197204", entries.get(6).getKey());
        Assertions.assertEquals("day19730101", entries.get(7).getKey());

        Map<String, Set<String>> stringSetMap = IcebergUtils.mergeOverlapPartitions(map);
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
