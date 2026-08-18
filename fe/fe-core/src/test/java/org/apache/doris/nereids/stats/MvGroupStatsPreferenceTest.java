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
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
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

import java.util.Collections;
import java.util.Optional;

/**
 * Test that the group stats prefer the stats derived from the calibrated mv when the group
 * contains both the base expression and the mv expression.
 */
public class MvGroupStatsPreferenceTest {

    private static final long BASE_INDEX_ID = 10L;
    private static final long MV_INDEX_ID = 20L;
    private static final long PARTITION_ID = 1L;
    private static final long BASE_ROW_COUNT = 100L;
    private static final long MV_ROW_COUNT = 1000L;

    private ConnectContext connectContext;
    private ConnectContext previousContext;
    private StatementContext statementContext;
    private RelationId mvRelationId;
    private Env env;
    private AnalysisManager analysisManager;
    private StatisticsCache statisticsCache;
    private StatisticsCache.OlapTableStatistics olapTableStatistics;
    private LogicalOlapScan baseScan;
    private LogicalOlapScan mvScan;
    private SlotReference slot;
    private Group group;

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

        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getBaseIndexId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(baseTable.getQualifiedDbName()).thenReturn("test");
        Mockito.when(baseTable.getRowCountForIndex(BASE_INDEX_ID, true)).thenReturn(BASE_ROW_COUNT);
        Mockito.when(baseTable.getPartitionNum()).thenReturn(1);
        Mockito.when(baseTable.getPartition(PARTITION_ID)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(baseTable.getRowCountForPartitionIndex(PARTITION_ID, BASE_INDEX_ID, true))
                .thenReturn(BASE_ROW_COUNT);

        OlapTable mvTable = Mockito.mock(OlapTable.class);
        Mockito.when(mvTable.getBaseIndexId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(mvTable.getQualifiedDbName()).thenReturn("test");
        Mockito.when(mvTable.getRowCountForIndex(MV_INDEX_ID, true)).thenReturn(MV_ROW_COUNT);
        Mockito.when(mvTable.getPartitionNum()).thenReturn(1);
        Mockito.when(mvTable.getPartition(PARTITION_ID)).thenReturn(Mockito.mock(Partition.class));
        Mockito.when(mvTable.getRowCountForPartitionIndex(PARTITION_ID, MV_INDEX_ID, true))
                .thenReturn(MV_ROW_COUNT);

        Column column = new Column("k", PrimitiveType.INT);
        slot = new SlotReference(new ExprId(1), "k", IntegerType.INSTANCE, true,
                ImmutableList.of("test", "t"), baseTable, column, baseTable, column);

        baseScan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(baseScan.withGroupExpression(Mockito.any(Optional.class))).thenReturn(baseScan);
        Mockito.when(baseScan.accept(Mockito.any(), Mockito.isNull()))
                .thenAnswer(inv -> ((StatsCalculator) inv.getArgument(0)).computeOlapScan(baseScan));
        Mockito.when(baseScan.getTable()).thenReturn(baseTable);
        Mockito.when(baseScan.getSelectedIndexId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(baseScan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(PARTITION_ID));
        Mockito.when(baseScan.getOutput()).thenReturn(ImmutableList.of(slot));
        Mockito.when(baseScan.getVirtualColumns()).thenReturn(ImmutableList.of());
        Mockito.when(olapTableStatistics.getColumnStatistics("k", connectContext))
                .thenReturn(new ColumnStatisticBuilder(BASE_ROW_COUNT).setNdv(BASE_ROW_COUNT).build());

        mvScan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(mvScan.withGroupExpression(Mockito.any(Optional.class))).thenReturn(mvScan);
        Mockito.when(mvScan.accept(Mockito.any(), Mockito.isNull()))
                .thenAnswer(inv -> ((StatsCalculator) inv.getArgument(0)).computeOlapScan(mvScan));
        Mockito.when(mvScan.getTable()).thenReturn(mvTable);
        Mockito.when(mvScan.getSelectedIndexId()).thenReturn(MV_INDEX_ID);
        Mockito.when(mvScan.getSelectedPartitionIds()).thenReturn(ImmutableList.of(PARTITION_ID));
        Mockito.when(mvScan.getOutput()).thenReturn(ImmutableList.of(slot));
        Mockito.when(mvScan.getVirtualColumns()).thenReturn(ImmutableList.of());
        mvRelationId = StatementScopeIdGenerator.newRelationId();
        Mockito.when(mvScan.getRelationId()).thenReturn(mvRelationId);
        // register the estimated stats of the mv and the column classification
        Statistics estimatedStats = new Statistics(BASE_ROW_COUNT,
                ImmutableMap.of(slot, new ColumnStatisticBuilder(BASE_ROW_COUNT).setNdv(BASE_ROW_COUNT).build()));
        statementContext.addStatistics(mvRelationId, estimatedStats);
        statementContext.addMaterializedViewColumnClassification(mvRelationId, ImmutableSet.of(slot), ImmutableSet.of());

        group = new Group(null, new LogicalProperties(Collections::emptyList, () -> DataTrait.EMPTY_TRAIT));
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testGroupStatsPreferMvExpression() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // derive the base expression first, the group stats is based on the base scan
            GroupExpression baseExpression = new GroupExpression(baseScan, ImmutableList.of());
            group.addLogicalExpression(baseExpression);
            StatsCalculator.estimate(baseExpression, null);
            Assertions.assertEquals(BASE_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            Assertions.assertFalse(group.isFromMvStats());
            // derive the mv expression, the group stats should be covered by the calibrated mv stats
            GroupExpression mvExpression = new GroupExpression(mvScan, ImmutableList.of());
            group.addLogicalExpression(mvExpression);
            StatsCalculator.estimate(mvExpression, null);
            Assertions.assertEquals(MV_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            Assertions.assertTrue(group.isFromMvStats());
            Assertions.assertEquals(MV_ROW_COUNT, mvExpression.getEstOutputRowCount(), 0.001);
        }
    }

    @Test
    public void testNoCoverageWhenCalibrationDisabled() {
        connectContext.getSessionVariable().setEnableMaterializedViewStatsCalibration(false);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            GroupExpression baseExpression = new GroupExpression(baseScan, ImmutableList.of());
            group.addLogicalExpression(baseExpression);
            StatsCalculator.estimate(baseExpression, null);
            Assertions.assertEquals(BASE_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            // calibration is disabled, the mv expression returns the estimated stats without the
            // calibrated mark, so the group stats should not be covered
            GroupExpression mvExpression = new GroupExpression(mvScan, ImmutableList.of());
            group.addLogicalExpression(mvExpression);
            StatsCalculator.estimate(mvExpression, null);
            Assertions.assertEquals(BASE_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            Assertions.assertFalse(group.isFromMvStats());
        }
    }

    @Test
    public void testBaseExpressionDerivedAfterMvDoesNotOverwrite() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            // derive the mv expression first, the group stats is covered by the calibrated mv stats
            GroupExpression mvExpression = new GroupExpression(mvScan, ImmutableList.of());
            group.addLogicalExpression(mvExpression);
            StatsCalculator.estimate(mvExpression, null);
            Assertions.assertEquals(MV_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            Assertions.assertTrue(group.isFromMvStats());
            // the base expression derived later should not overwrite the group stats
            GroupExpression baseExpression = new GroupExpression(baseScan, ImmutableList.of());
            group.addLogicalExpression(baseExpression);
            StatsCalculator.estimate(baseExpression, null);
            Assertions.assertEquals(MV_ROW_COUNT, group.getStatistics().getRowCount(), 0.001);
            Assertions.assertTrue(group.isFromMvStats());
        }
    }

    @Test
    public void testCalibratedStatsMark() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Statistics statistics = new StatsCalculator((CascadesContext) null).computeOlapScan(mvScan);
            Assertions.assertEquals(MV_ROW_COUNT, statistics.getRowCount(), 0.001);
            Assertions.assertTrue(statistics.isFromMvCalibrated());
        }
    }
}
