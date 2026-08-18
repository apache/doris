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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;

/**
 * Test of calibrating the estimated stats of materialized view by its actual row count in
 * {@link StatsCalculator#computeOlapScan}.
 */
public class MvStatsCalibrationTest {

    private static final long BASE_INDEX_ID = 10L;
    private static final long MV_INDEX_ID = 20L;
    private static final long PARTITION_ID = 1L;
    // the estimated row count and actual row count of the mv
    private static final double ESTIMATED_ROW_COUNT = 100;
    private static final double ACTUAL_ROW_COUNT = 1000;

    private ConnectContext connectContext;
    private ConnectContext previousContext;
    private StatementContext statementContext;
    private RelationId relationId;
    private Env env;
    private AnalysisManager analysisManager;
    private StatisticsCache statisticsCache;
    private StatisticsCache.OlapTableStatistics olapTableStatistics;
    private OlapTable table;
    private LogicalOlapScan scan;
    private SlotReference groupByKeySlot;
    private SlotReference aggFunctionSlot;
    private SlotReference passthroughSlot;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        connectContext = MemoTestUtils.createConnectContext();
        statementContext = MemoTestUtils.createStatementContext(connectContext, "");
        connectContext.getSessionVariable().setEnableMaterializedViewStatsCalibration(true);

        env = Mockito.mock(Env.class);
        analysisManager = Mockito.mock(AnalysisManager.class);
        statisticsCache = Mockito.mock(StatisticsCache.class);
        olapTableStatistics = Mockito.mock(StatisticsCache.OlapTableStatistics.class);
        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
        Mockito.when(env.getStatisticsCache()).thenReturn(statisticsCache);
        Mockito.when(statisticsCache.getOlapTableStats(Mockito.any(LogicalOlapScan.class)))
                .thenReturn(olapTableStatistics);

        table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(table.getQualifiedDbName()).thenReturn("test");

        scan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(MV_INDEX_ID);
        Mockito.when(scan.getVirtualColumns()).thenReturn(ImmutableList.of());
        Mockito.when(table.getPartitionNum()).thenReturn(1);
        Mockito.when(table.getPartition(PARTITION_ID)).thenReturn(Mockito.mock(Partition.class));

        relationId = StatementScopeIdGenerator.newRelationId();
        Mockito.when(scan.getRelationId()).thenReturn(relationId);
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(PARTITION_ID));

        Column groupByKeyColumn = new Column("a", PrimitiveType.INT);
        Column aggFunctionColumn = new Column("cnt", PrimitiveType.BIGINT);
        Column passthroughColumn = new Column("p", PrimitiveType.INT);
        groupByKeySlot = new SlotReference(new ExprId(1), "a", IntegerType.INSTANCE, true,
                ImmutableList.of("test", "mv"), table, groupByKeyColumn, table, groupByKeyColumn);
        aggFunctionSlot = new SlotReference(new ExprId(2), "cnt", IntegerType.INSTANCE, true,
                ImmutableList.of("test", "mv"), table, aggFunctionColumn, table, aggFunctionColumn);
        passthroughSlot = new SlotReference(new ExprId(3), "p", IntegerType.INSTANCE, true,
                ImmutableList.of("test", "mv"), table, passthroughColumn, table, passthroughColumn);
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    // the mv is treated as selected when the selected index is not the base index
    private void mockBeReportedRowCount(double rowCount, double partitionRowCount) {
        Mockito.when(table.getRowCountForIndex(MV_INDEX_ID, true)).thenReturn((long) rowCount);
        Mockito.when(table.getRowCountForPartitionIndex(PARTITION_ID, MV_INDEX_ID, true))
                .thenReturn((long) partitionRowCount);
    }

    // register the estimated stats and the column classification of the mv scan to the statement context
    private void registerMvStats(Statistics estimatedStats, Set<Expression> groupByKeySlots,
            Set<Expression> aggFunctionSlots) {
        statementContext.addStatistics(relationId, estimatedStats);
        statementContext.addMaterializedViewColumnClassification(relationId, groupByKeySlots, aggFunctionSlots);
    }

    private Statistics computeOlapScan() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            return new StatsCalculator((CascadesContext) null).computeOlapScan(scan);
        }
    }

    @Test
    public void testCalibrateGroupByKeyAndAggFunctionOutput() {
        // mv def: select a, count(*) as cnt from t group by a
        // estimated: 100 rows, ndv(a)=100 (saturated single group by key), ndv(cnt)=50
        ColumnStatistic groupByKeyStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(ESTIMATED_ROW_COUNT).setNumNulls(0).build();
        ColumnStatistic aggFunctionStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(50).setNumNulls(10).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(groupByKeySlot, groupByKeyStat, aggFunctionSlot, aggFunctionStat));
        registerMvStats(estimatedStats, ImmutableSet.of(groupByKeySlot), ImmutableSet.of(aggFunctionSlot));
        mockBeReportedRowCount(ACTUAL_ROW_COUNT, ACTUAL_ROW_COUNT);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(groupByKeySlot, aggFunctionSlot));

        Statistics result = computeOlapScan();

        Assertions.assertEquals(ACTUAL_ROW_COUNT, result.getRowCount(), 0.001);
        // group by key ndv is structurally tied to the output row count in single group by key case
        Assertions.assertEquals(ACTUAL_ROW_COUNT, result.findColumnStatistics(groupByKeySlot).ndv, 0.001);
        Assertions.assertEquals(0, result.findColumnStatistics(groupByKeySlot).numNulls, 0.001);
        // aggregate function output ndv is never scaled, only clamped
        Assertions.assertEquals(50, result.findColumnStatistics(aggFunctionSlot).ndv, 0.001);
        // numNulls scales with the clamped ratio: ratio = 10, clamped to 2.0
        Assertions.assertEquals(20, result.findColumnStatistics(aggFunctionSlot).numNulls, 0.001);
    }

    @Test
    public void testCalibrateFilterReducedColumnKeepsNdv() {
        // mv def: select * from t where a = 1, the ndv of a is 1 which is a precise value
        ColumnStatistic filterReducedStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(1).setNumNulls(0).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(passthroughSlot, filterReducedStat));
        // no group by or aggregate function in the mv def, all columns are passthrough
        registerMvStats(estimatedStats, ImmutableSet.of(), ImmutableSet.of());
        mockBeReportedRowCount(ACTUAL_ROW_COUNT, ACTUAL_ROW_COUNT);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(passthroughSlot));

        Statistics result = computeOlapScan();

        Assertions.assertEquals(ACTUAL_ROW_COUNT, result.getRowCount(), 0.001);
        // low cardinality passthrough column keeps the estimated ndv
        Assertions.assertEquals(1, result.findColumnStatistics(passthroughSlot).ndv, 0.001);
        Assertions.assertEquals(0, result.findColumnStatistics(passthroughSlot).numNulls, 0.001);
    }

    @Test
    public void testCalibrateHighCardinalityPassthroughColumn() {
        // mv def: select p from t, p is a high cardinality column with ndv equal to the row count
        ColumnStatistic highCardinalityStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(ESTIMATED_ROW_COUNT).setNumNulls(0).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(passthroughSlot, highCardinalityStat));
        registerMvStats(estimatedStats, ImmutableSet.of(), ImmutableSet.of());
        mockBeReportedRowCount(ACTUAL_ROW_COUNT, ACTUAL_ROW_COUNT);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(passthroughSlot));

        Statistics result = computeOlapScan();

        Assertions.assertEquals(ACTUAL_ROW_COUNT, result.getRowCount(), 0.001);
        // high cardinality passthrough column scales with the clamped ratio: ratio = 10, clamped to 2.0
        Assertions.assertEquals(ESTIMATED_ROW_COUNT * 2, result.findColumnStatistics(passthroughSlot).ndv, 0.001);
    }

    @Test
    public void testCalibrationDisabledFallbackToEstimate() {
        connectContext.getSessionVariable().setEnableMaterializedViewStatsCalibration(false);
        ColumnStatistic groupByKeyStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(ESTIMATED_ROW_COUNT).setNumNulls(0).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(groupByKeySlot, groupByKeyStat));
        registerMvStats(estimatedStats, ImmutableSet.of(groupByKeySlot), ImmutableSet.of());
        mockBeReportedRowCount(ACTUAL_ROW_COUNT, ACTUAL_ROW_COUNT);
        // the scan output contains an extra slot which is not in the estimated stats
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(groupByKeySlot, aggFunctionSlot));

        Statistics result = computeOlapScan();

        // calibration is disabled, fall back to the estimated stats because actual >= estimated
        Assertions.assertEquals(ESTIMATED_ROW_COUNT, result.getRowCount(), 0.001);
        Assertions.assertNotNull(result.findColumnStatistics(aggFunctionSlot));
        // the shared stats registered in the statement context is not polluted
        Statistics registeredStats = statementContext.getStatistics(relationId).get();
        Assertions.assertEquals(ESTIMATED_ROW_COUNT, registeredStats.getRowCount(), 0.001);
        Assertions.assertNull(registeredStats.findColumnStatistics(aggFunctionSlot));
    }

    @Test
    public void testCalibrationSkippedWhenRowCountUnreported() {
        ColumnStatistic groupByKeyStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(ESTIMATED_ROW_COUNT).setNumNulls(0).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(groupByKeySlot, groupByKeyStat));
        registerMvStats(estimatedStats, ImmutableSet.of(groupByKeySlot), ImmutableSet.of());
        // BE does not report the row count and the table is not analyzed, getOlapTableRowCount returns -1
        mockBeReportedRowCount(-1, -1);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(groupByKeySlot));
        Mockito.when(olapTableStatistics.getColumnStatistics("a", connectContext))
                .thenReturn(ColumnStatistic.UNKNOWN);

        Statistics result = computeOlapScan();

        // calibration is skipped because the actual row count is not trustworthy
        Assertions.assertNotEquals(ACTUAL_ROW_COUNT, result.getRowCount());
    }

    @Test
    public void testCalibrationDoesNotPolluteSharedStats() {
        ColumnStatistic groupByKeyStat = new ColumnStatisticBuilder(ESTIMATED_ROW_COUNT)
                .setNdv(ESTIMATED_ROW_COUNT).setNumNulls(0).build();
        Statistics estimatedStats = new Statistics(ESTIMATED_ROW_COUNT,
                ImmutableMap.of(groupByKeySlot, groupByKeyStat));
        registerMvStats(estimatedStats, ImmutableSet.of(groupByKeySlot), ImmutableSet.of());
        mockBeReportedRowCount(ACTUAL_ROW_COUNT, ACTUAL_ROW_COUNT);
        Mockito.when(scan.getOutput()).thenReturn(ImmutableList.of(groupByKeySlot));

        computeOlapScan();

        // the shared stats registered in the statement context keeps the original estimated stats
        Statistics registeredStats = statementContext.getStatistics(relationId).get();
        Assertions.assertEquals(ESTIMATED_ROW_COUNT, registeredStats.getRowCount(), 0.001);
        Assertions.assertEquals(ESTIMATED_ROW_COUNT,
                registeredStats.findColumnStatistics(groupByKeySlot).ndv, 0.001);
    }
}
