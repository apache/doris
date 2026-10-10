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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;


public class MTMVRelatedPartitionDescSyncLimitGeneratorTest {

    @Test
    public void testGenerateMTMVPartitionSyncConfigByProperties() throws AnalysisException {
        MTMVRelatedPartitionDescSyncLimitGenerator generator = new MTMVRelatedPartitionDescSyncLimitGenerator();
        Map<String, String> mvProperties = Maps.newHashMap();
        MTMVPartitionSyncConfig config = generator
                .generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assertions.assertEquals(-1, config.getSyncLimit());
        Assertions.assertFalse(config.getDateFormat().isPresent());
        Assertions.assertEquals(MTMVPartitionSyncTimeUnit.DAY, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT, "1");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assertions.assertEquals(1, config.getSyncLimit());
        Assertions.assertFalse(config.getDateFormat().isPresent());
        Assertions.assertEquals(MTMVPartitionSyncTimeUnit.DAY, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT, "month");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assertions.assertEquals(1, config.getSyncLimit());
        Assertions.assertFalse(config.getDateFormat().isPresent());
        Assertions.assertEquals(MTMVPartitionSyncTimeUnit.MONTH, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT, "%Y%m%d");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assertions.assertEquals(1, config.getSyncLimit());
        Assertions.assertEquals("%Y%m%d", config.getDateFormat().get());
        Assertions.assertEquals(MTMVPartitionSyncTimeUnit.MONTH, config.getTimeUnit());
    }

    @Test
    public void testATableWithADefaultListPartitionIsKeptOutOfTheWindow() throws AnalysisException {
        // A default list partition takes the rows no other partition of the table claims, and which of its
        // rows belong to an MV partition is decided by their own key -- ADD PARTITION can claim a key without
        // moving the rows already held under it -- so a windowed read cannot keep those rows while dropping
        // an expired explicit partition: no predicate on the partition columns tells the two sets apart. Such
        // a table is therefore left whole, and the mapping keeps naming every partition the MV partition's
        // key range covers, which is what the refresh reads for it. The range table is the control: it is
        // windowed as before, so a change that simply stopped applying the window would fail here.
        MTMVRelatedPartitionDescSyncLimitGenerator generator = new MTMVRelatedPartitionDescSyncLimitGenerator();
        Column dateColumn = new Column("d", ScalarType.DATE);
        MTMVRelatedTableIf listTable = Mockito.mock(MTMVRelatedTableIf.class);
        MTMVRelatedTableIf rangeTable = Mockito.mock(MTMVRelatedTableIf.class);

        ListPartitionItem defaultPartition = listItem(dateColumn, "1970-01-01");
        defaultPartition.setDefaultPartition(true);
        Map<MTMVRelatedTableIf, Map<String, PartitionItem>> items = Maps.newHashMap();
        items.put(listTable, ImmutableMap.of(
                "p_expired", listItem(dateColumn, "1990-01-01"),
                "p_kept", listItem(dateColumn, "9999-01-01"),
                "p_default", defaultPartition));
        items.put(rangeTable, ImmutableMap.of(
                "p_old", rangeItem(dateColumn, "1990-01-01"),
                "p_new", rangeItem(dateColumn, "9999-01-01")));
        RelatedPartitionDescResult result = new RelatedPartitionDescResult(null);
        result.setItems(items);

        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT, "2");
        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT, "YEAR");
        generator.apply(Mockito.mock(MTMVPartitionInfo.class), mvProperties, result,
                Lists.newArrayList(dateColumn), Maps.newHashMap());

        Assertions.assertEquals(Sets.newHashSet("p_expired", "p_kept", "p_default"),
                result.getItems().get(listTable).keySet());
        Assertions.assertEquals(Sets.newHashSet("p_new"), result.getItems().get(rangeTable).keySet());
    }

    private static ListPartitionItem listItem(Column column, String value) throws AnalysisException {
        return new ListPartitionItem(ImmutableList.of(PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue(value)), ImmutableList.of(column.getType()), false)));
    }

    private static RangePartitionItem rangeItem(Column column, String value) throws AnalysisException {
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(value)),
                ImmutableList.of(column));
        return new RangePartitionItem(Range.lessThan(upper));
    }

    @Test
    public void testGetNowTruncSubSec() throws AnalysisException {
        MTMVRelatedPartitionDescSyncLimitGenerator generator = new MTMVRelatedPartitionDescSyncLimitGenerator();
        DateTimeV2Literal dateTimeLiteral = new DateTimeV2Literal("2020-02-03 20:10:10");
        try (MockedStatic<DateTimeAcquire> ms = Mockito.mockStatic(DateTimeAcquire.class)) {
            ms.when(DateTimeAcquire::now).thenReturn(dateTimeLiteral);
            long nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.DAY, 1);
            // 2020-02-03
            Assertions.assertEquals(1580659200L, nowTruncSubSec);
            nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.MONTH, 1);
            // 2020-02-01
            Assertions.assertEquals(1580486400L, nowTruncSubSec);
            nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.YEAR, 1);
            // 2020-01-01
            Assertions.assertEquals(1577808000L, nowTruncSubSec);
            nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.MONTH, 3);
            // 2019-12-01
            Assertions.assertEquals(1575129600L, nowTruncSubSec);
            nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.DAY, 4);
            // 2020-01-31
            Assertions.assertEquals(1580400000L, nowTruncSubSec);
        }

        dateTimeLiteral = new DateTimeV2Literal("1970-01-02 20:10:10");
        try (MockedStatic<DateTimeAcquire> ms = Mockito.mockStatic(DateTimeAcquire.class)) {
            ms.when(DateTimeAcquire::now).thenReturn(dateTimeLiteral);
            long nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.DAY, 3);
            Assertions.assertEquals(-115200L, nowTruncSubSec);
        }
    }
}
