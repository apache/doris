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

import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.rewrite.InitJoinOrder;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.analysis.AnalysisManager;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.cache.StatisticsCacheKey;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Tests sampled all-null statistics through the OLAP cache and join initialization rule. */
public class InvalidColumnStatsReorderTest {
    private MockedStatic<Env> mockedEnv;
    private DatabaseIf metadataDatabase;
    private ConnectContext previousContext;
    private StatisticsCache cache;
    private LogicalOlapScan small;
    private LogicalOlapScan video;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        cache = new StatisticsCache();
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(10000L);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(20000L);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Env env = Mockito.spy(Env.getCurrentEnv());
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        Mockito.when(env.getStatisticsCache()).thenReturn(cache);
        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        metadataDatabase = database;
        small = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                mockTable(30000L, "small"), ImmutableList.of("db"));
        OlapTable table = mockTable(30001L, "video");
        table.getFullSchema().get(1).setName("log_url");
        video = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("db"));
        // Populate every visible column to avoid asynchronous cache misses in this reproduction.
        for (LogicalOlapScan scan : ImmutableList.of(small, video)) {
            scan.getOutput().forEach(slot -> put(scan, slot.getName(), ColumnStatistic.UNKNOWN));
        }
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    private OlapTable mockTable(long id, String name) {
        OlapTable table = Mockito.spy(PlanConstructor.newOlapTable(id, name, 0));
        Mockito.doReturn(metadataDatabase).when(table).getDatabase();
        Mockito.doReturn(170948752L).when(table).getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean());
        return table;
    }

    private void put(LogicalOlapScan scan, String column, ColumnStatistic statistic) {
        cache.putCache(new StatisticsCacheKey(10000L, 20000L, scan.getTable().getId(), -1, column), statistic);
    }

    private ColumnStatistic anomalousStats() throws Exception {
        return new ColumnStatisticBuilder(124357137).setNdv(0).setNumNulls(124357137)
                .setAvgSizeByte(0).setDataSize(0)
                .setMinExpr(new StringLiteral("https://example.com/a"))
                .setMaxExpr(new StringLiteral("https://example.com/z")).build();
    }

    private CascadesContext context(Plan plan) {
        CascadesContext context = MemoTestUtils.createCascadesContext(plan);
        Assertions.assertSame(context.getConnectContext(), ConnectContext.get());
        Assertions.assertFalse(context.getConnectContext().getSessionVariable().isDisableJoinReorder());
        Assertions.assertFalse(context.getConnectContext().getState().isPlanWithUnKnownColumnStats());
        return context;
    }

    @Test
    public void testActualCachePathAndLegacyUnknownConversion() throws Exception {
        ColStatsData collected = new ColStatsData(new ResultRow(Arrays.asList(
                "id", "10000", "20000", "30001", "-1", "log_url", null,
                "124357137", "0", "124357137", "https://example.com/a", "https://example.com/z",
                "0", "2026-10-02 09:26:13", null)));
        Assertions.assertTrue(collected.isValid());
        ColumnStatistic bad = anomalousStats();
        put(video, "log_url", bad);
        CascadesContext context = context(video);
        Assertions.assertSame(bad, cache.getOlapTableStats(video)
                .getColumnStatistics("log_url", context.getConnectContext()));

        Method legacy = StatsCalculator.class.getDeclaredMethod("getColumnStatistic",
                org.apache.doris.catalog.TableIf.class, String.class, long.class);
        legacy.setAccessible(true);
        Assertions.assertSame(ColumnStatistic.UNKNOWN,
                legacy.invoke(new StatsCalculator(context), video.getTable(), "log_url", -1L));

        Optional<String> reason = StatsCalculator.disableJoinReorderIfStatsInvalid(
                ImmutableList.of(video), context);
        Assertions.assertFalse(reason.isPresent());
        Assertions.assertFalse(context.getConnectContext().getSessionVariable().isDisableJoinReorder());

        put(video, "log_url", ColumnStatistic.UNKNOWN);
        CascadesContext control = context(video);
        Assertions.assertFalse(StatsCalculator.disableJoinReorderIfStatsInvalid(
                ImmutableList.of(video), control).isPresent());
        Assertions.assertFalse(control.getConnectContext().getSessionVariable().isDisableJoinReorder());
    }

    @Test
    public void testSampledNullColumnDoesNotBlockOuterJoinSwap() throws Exception {
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, initializeJoin(ColumnStatistic.UNKNOWN));
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, initializeJoin(anomalousStats()));
        // Restore the legacy UNKNOWN result for this column only; retain all other inputs.
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, initializeJoin(ColumnStatistic.UNKNOWN));
        ColumnStatistic allNull = new ColumnStatisticBuilder(124357137)
                .setNdv(0).setNumNulls(124357137).build();
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, initializeJoin(allNull));
        ColumnStatistic valid = new ColumnStatisticBuilder(anomalousStats()).setNdv(10)
                .setNumNulls(0).build();
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, initializeJoin(valid));
        ColumnStatistic invalidNonNullCount = new ColumnStatisticBuilder(anomalousStats())
                .setNumNulls(1).build();
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, initializeJoin(invalidNonNullCount));
    }

    private JoinType initializeJoin(ColumnStatistic statistic) throws Exception {
        put(video, "log_url", statistic);
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) new LogicalPlanBuilder(small)
                .join(video, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)).build();
        // Fix derived row counts, isolating candidate suppression from estimation accuracy.
        small.setStatistics(new Statistics(837, Collections.emptyMap()));
        video.setStatistics(new Statistics(132067569, Collections.emptyMap()));
        Assertions.assertFalse(join.getExpressions().toString().contains("log_url"));
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(small.getOutput().get(0)), join);
        Assertions.assertFalse(project.getOutput().toString().contains("log_url"));
        CascadesContext context = context(project);
        List<Plan> result = new InitJoinOrder().build().transform(join, context);
        LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) result.get(0);
        if (rewritten.getJoinType() == JoinType.RIGHT_OUTER_JOIN) {
            Assertions.assertSame(small, rewritten.right());
            Assertions.assertSame(video, rewritten.left());
        } else {
            Assertions.assertTrue(context.getConnectContext().getSessionVariable().isDisableJoinReorder());
        }
        VariableMgr.revertSessionValue(context.getConnectContext().getSessionVariable());
        return rewritten.getJoinType();
    }
}
