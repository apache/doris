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

package org.apache.doris.statistics.model;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FileType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.stats.FilterEstimation;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.repository.StatsId;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;

class FileColumnStatisticsTest {
    @Test
    void testUnavailableRoundTripsAndZeroSizes() throws Exception {
        try (MockedStatic<StatisticsUtil> mocked = fileColumn()) {
            for (long[] sample : new long[][] {{0, 0, 0}, {10, 10, 0}, {10, 3, 97}}) {
                ResultRow row = row(sample[0], sample[1], sample[2]);
                ColStatsData data = new ColStatsData(row);
                Assertions.assertTrue(data.isValid());
                Assertions.assertTrue(data.ndvUnavailable);
                Assertions.assertTrue(data.toSQL(true).contains("," + sample[0] + ",NULL," + sample[1] + ",NULL,NULL,"));
                assertStats(data.toColumnStatistic(), sample);
                assertStats(ColumnStatistic.fromResultRow(row), sample);
                assertStats(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(data), ColStatsData.class)
                        .toColumnStatistic(), sample);
                ColumnStatistic column = data.toColumnStatistic();
                assertStats(ColumnStatistic.fromJson(column.toJson().toString()), sample);
                assertStats(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), ColumnStatistic.class), sample);
                ColumnStatisticBuilder copied = new ColumnStatisticBuilder(column);
                copied.normalizeAvgSizeByte(org.apache.doris.nereids.types.FileType.INSTANCE);
                assertStats(copied.build(), sample);
                ColStatsData persisted = new ColStatsData("2--1-f", 0, 1, 2, -1, "f", null, null, column);
                Assertions.assertTrue(persisted.ndvUnavailable);
                assertStats(persisted.toColumnStatistic(), sample);
            }
        }
    }

    @Test
    void testEmptyShortcutDoesNotClaimZeroNdv() {
        ColStatsData data = new ColStatsData(new StatsId("2--1-f", 0, 1, 2, -1, "f", null), true);
        Assertions.assertTrue(data.ndvUnavailable);
        Assertions.assertTrue(data.isValid());
        Assertions.assertTrue(data.toSQL(true).contains(",0,NULL,0,NULL,NULL,0,"));
    }

    @Test
    void testPartitionCarrierMergePreservesCountsAndBytes() throws Exception {
        try (MockedStatic<StatisticsUtil> mocked = fileColumn()) {
            // Empty HLL is a non-null persistence carrier, never a measurement of FILE NDV.
            PartitionColumnStatistic first = PartitionColumnStatistic.fromResultRow(partitionRow(10, 3, 70));
            PartitionColumnStatistic second = PartitionColumnStatistic.fromResultRow(partitionRow(5, 5, 0));
            Assertions.assertTrue(first.ndvUnavailable);
            Assertions.assertFalse(first.isUnKnown);
            ColumnStatistic merged = new PartitionColumnStatisticBuilder(first).merge(second).toColumnStatistics();
            assertStats(merged, new long[] {15, 8, 70});
            PartitionColumnStatistic empty = PartitionColumnStatistic.fromResultRow(partitionRow(0, 0, 0));
            Assertions.assertEquals(0, empty.dataSize);
            Assertions.assertTrue(empty.ndvUnavailable);
            Assertions.assertEquals(0, new PartitionColumnStatisticBuilder(empty).build().avgSizeByte);
        }
    }

    @Test
    void testPartitionNullCountsAreNotScaledTwice() {
        ColumnStatistic selectedPartitions = new ColumnStatisticBuilder(50).setNdvUnavailable(true)
                .setNumNulls(35).build();
        ColumnStatistic tableFallback = new ColumnStatisticBuilder(100).setNdvUnavailable(true)
                .setNumNulls(70).build();
        double selected = Deencapsulation.invoke(StatsCalculator.class, "scaleNullsAfterPartitionPruning",
                selectedPartitions, 50.0, 100.0);
        double fallback = Deencapsulation.invoke(StatsCalculator.class, "scaleNullsAfterPartitionPruning",
                tableFallback, 50.0, 100.0);
        Assertions.assertEquals(35, selected);
        Assertions.assertEquals(35, fallback);
    }

    @Test
    void testNullSelectivityUsesCollectedCounts() {
        ColumnStatistic column = new ColumnStatisticBuilder(100).setNdvUnavailable(true)
                .setNumNulls(23).setDataSize(770).setAvgSizeByte(7.7).build();
        SlotReference file = new SlotReference("f", org.apache.doris.nereids.types.FileType.INSTANCE);
        Statistics statistics = new Statistics(100, new HashMap<>());
        statistics.addColumnStats(file, column);
        Assertions.assertEquals(23, new FilterEstimation().estimate(new IsNull(file), statistics).getRowCount());
        Assertions.assertEquals(77, new FilterEstimation().estimate(new Not(new IsNull(file)), statistics).getRowCount());
        Assertions.assertTrue(statistics.findColumnStatistics(file).ndvUnavailable);
    }

    @Test
    void testMeasuredZeroRemainsAvailableAndContainersRemainUnsupported() {
        ColStatsData scalar = new ColStatsData(new StatsId());
        Assertions.assertFalse(scalar.ndvUnavailable);
        Assertions.assertFalse(new ColumnStatisticBuilder(0).setNdv(0).build().ndvUnavailable);
        Assertions.assertFalse(StatisticsUtil.isUnsupportedType(FileType.create()));
        Assertions.assertTrue(StatisticsUtil.isUnsupportedType(new org.apache.doris.catalog.ArrayType(FileType.create())));
    }

    private static MockedStatic<StatisticsUtil> fileColumn() {
        MockedStatic<StatisticsUtil> mocked = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
        mocked.when(() -> StatisticsUtil.findColumn(0, 1, 2, -1, "f")).thenReturn(new Column("f", FileType.create()));
        return mocked;
    }

    private static ResultRow row(long count, long nulls, long bytes) {
        return new ResultRow(Arrays.asList("2--1-f", "0", "1", "2", "-1", "f", null,
                String.valueOf(count), null, String.valueOf(nulls), null, null, String.valueOf(bytes), "2026-09-16", null));
    }

    private static ResultRow partitionRow(long count, long nulls, long bytes) {
        return new ResultRow(Arrays.asList("0", "1", "2", "-1", "p1", "f", String.valueOf(count), "AA==",
                String.valueOf(nulls), null, null, String.valueOf(bytes), "2026-09-16"));
    }

    private static void assertStats(ColumnStatistic value, long[] expected) {
        Assertions.assertFalse(value.isUnKnown);
        Assertions.assertTrue(value.ndvUnavailable);
        Assertions.assertNull(value.minExpr);
        Assertions.assertNull(value.maxExpr);
        Assertions.assertNull(value.hotValues);
        Assertions.assertEquals(expected[0], value.count);
        Assertions.assertEquals(expected[1], value.numNulls);
        Assertions.assertEquals(expected[2], value.dataSize);
        Assertions.assertEquals(expected[0] == 0 ? 0 : (double) expected[2] / expected[0], value.avgSizeByte);
    }
}
