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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

class IvmDeltaRewriterTest extends IvmDeltaTestBase {

    private static final Predicate<LogicalOlapScan> NO_EXCLUSIONS = scan -> false;

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Sets.newHashSet());
        Mockito.when(mtmv.getInsertedColumnNames()).thenReturn(ImmutableList.of("id", "name"));
        return mtmv;
    }

    /** Creates a baseTableStreams map with a single pending-delta stream for the scan's table. */
    private Map<TableNameInfo, IvmStreamRef> makeStreams(LogicalOlapScan scan) {
        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        IvmStreamRef ref = new IvmStreamRef(0);
        ref.setLatestTso(100);
        streams.put(IvmRefreshContext.toTableNameInfo(scan), ref);
        return streams;
    }

    private Map<TableNameInfo, IvmStreamRef> makeStreamsWithTso(LogicalOlapScan scan,
            long consumedTso, long latestTso) {
        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scan, consumedTso, latestTso);
        return streams;
    }

    private void addStream(Map<TableNameInfo, IvmStreamRef> streams,
            LogicalOlapScan scan, long consumedTso, long latestTso) {
        IvmStreamRef ref = new IvmStreamRef(consumedTso);
        ref.setLatestTso(latestTso);
        streams.put(IvmRefreshContext.toTableNameInfo(scan), ref);
    }

    private IvmRefreshContext rewriteContext(Map<TableNameInfo, IvmStreamRef> streams) {
        return new IvmRefreshContext(mockMtmv(), new ConnectContext(), new IvmNormalizeResult(), streams);
    }

    private LogicalJoin<LogicalOlapScan, LogicalOlapScan> crossJoin(
            LogicalOlapScan left, LogicalOlapScan right) {
        return new LogicalJoin<>(JoinType.CROSS_JOIN, left, right,
                new JoinReorderContext());
    }

    private List<LogicalOlapScan> collectScans(Plan plan) {
        return plan.collectToList(n -> n instanceof LogicalOlapScan);
    }

    // ==================== Delta rewrite tests ====================

    @Test
    void testScanOnlyProducesInsertBundle() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreams(scan);
        IvmRefreshContext ctx = new IvmRefreshContext(
                mtmv, new ConnectContext(), new IvmNormalizeResult(), streams);
        List<Command> commands = new IvmDeltaRewriter().rewrite(buildScanPlan(scan), ctx);

        Assertions.assertEquals(1, commands.size());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, commands.get(0));
    }

    @Test
    void testProjectScanProducesInsertBundle() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreams(scan);
        IvmRefreshContext ctx = new IvmRefreshContext(
                mtmv, new ConnectContext(), new IvmNormalizeResult(), streams);
        List<Command> commands = new IvmDeltaRewriter().rewrite(buildProjectScanPlan(scan), ctx);

        Assertions.assertEquals(1, commands.size());
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, commands.get(0));
    }

    @Test
    void testGroupedAggProducesDeleteSignSinkAndJoinPlan() {
        LogicalOlapScan scan = buildScan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreams(scan);
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        IvmRefreshContext ctx = new IvmRefreshContext(
                mtmv, bundle.connectContext, bundle.normalizeResult, streams);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(bundle.normalizedPlan, ctx).get(0);
        UnboundTableSink<?> sink = getSink(command);

        Assertions.assertEquals(mtmv.getInsertedColumnNames().size() + 1, sink.getColNames().size());
        Assertions.assertEquals(Column.DELETE_SIGN, sink.getColNames().get(sink.getColNames().size() - 1));
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertInstanceOf(LogicalFilter.class, finalProject.child());
        Assertions.assertInstanceOf(LogicalJoin.class, ((LogicalFilter<?>) finalProject.child()).child());
    }

    @Test
    void testContextRejectsNulls() {
        MTMV mtmv = mockMtmv();
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, new ConnectContext(), null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, null, null));
    }

    @Test
    void testUnifiedStrategyKeepsAggTerminalApplyPlan() {
        LogicalOlapScan scan = buildScan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreams(scan);
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        IvmRefreshContext ctx = new IvmRefreshContext(
                mtmv, bundle.connectContext, bundle.normalizeResult, streams);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(bundle.normalizedPlan, ctx).get(0);
        UnboundTableSink<?> sink = getSink(command);
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertTrue(finalProject.child() instanceof LogicalFilter
                || finalProject.child() instanceof LogicalJoin);
    }

    @Test
    void testUnifiedStrategyBuildsSinkProjectForNonAggPlan() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreams(scan);
        IvmRefreshContext ctx = new IvmRefreshContext(
                mtmv, new ConnectContext(), new IvmNormalizeResult(), streams);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(buildScanPlan(scan), ctx).get(0);
        UnboundTableSink<?> sink = getSink(command);
        Plan child = sink.child();
        Assertions.assertEquals(ImmutableList.of("id", "name", Column.DELETE_SIGN), sink.getColNames());
        Assertions.assertInstanceOf(LogicalProject.class, child);
        Assertions.assertFalse(child instanceof LogicalJoin);
    }

    // ==================== generateDeltaPlans tests ====================

    // ---------- Single scan ----------

    @Test
    void testGenSingleScanPendingDelta() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(1, plans.size());
        List<LogicalOlapScan> scans = collectScans(plans.get(0));
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(scans.get(0).isDelta());
    }

    @Test
    void testGenDeltaPlanClearsOldGroupExpression() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Plan scanInMemo = new GroupExpression(scan).getPlan();
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(
                scanInMemo, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(1, plans.size());
        Assertions.assertFalse(plans.get(0).getGroupExpression().isPresent());
        Assertions.assertFalse(collectScans(plans.get(0)).get(0).getGroupExpression().isPresent());
    }

    @Test
    void testGenSingleScanUpToDate() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 20, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertTrue(plans.isEmpty(), "Up-to-date scan should produce no delta plans");
    }

    @Test
    void testGenExplainBundleIncludesUpToDateScan() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 20, 20);

        List<IvmDeltaExplainBundle> bundles = new IvmDeltaRewriter()
                .generateDeltaExplainBundles(scan, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(1, bundles.size());
        IvmDeltaExplainBundle bundle = bundles.get(0);
        Assertions.assertEquals(1, bundle.getDeltaId());
        Assertions.assertEquals(1, bundle.getOccurrence());
        Assertions.assertEquals(20, bundle.getConsumedTso());
        Assertions.assertEquals(20, bundle.getLatestTso());
        Assertions.assertTrue(bundle.isNoOp());
        List<LogicalOlapScan> scans = collectScans(bundle.getDeltaPlan());
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(scans.get(0).isDelta());
    }

    // ---------- Two-table JOIN ----------

    @Test
    void testGenTwoTableJoinBothPending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(consumedTso_b=30)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertEquals(2, scans0.size());
        Assertions.assertTrue(scans0.get(0).isDelta(), "a should be delta in plan 0");
        Assertions.assertFalse(scans0.get(1).isDelta());
        Assertions.assertEquals(30, scans0.get(1).getTso(), "b should be bound to consumedTso=30");

        // Plan 1: a(latestTso_a=20) JOIN delta(b)
        List<LogicalOlapScan> scans1 = collectScans(plans.get(1));
        Assertions.assertEquals(2, scans1.size());
        Assertions.assertFalse(scans1.get(0).isDelta());
        Assertions.assertEquals(20, scans1.get(0).getTso(), "a should be bound to latestTso=20");
        Assertions.assertTrue(scans1.get(1).isDelta(), "b should be delta in plan 1");
    }

    @Test
    void testGenTwoTableJoinOnePending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);  // pending
        addStream(streams, scanB, 40, 40);  // up-to-date

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(1, plans.size());

        // Plan 0: delta(a) JOIN b(consumedTso_b=40)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertTrue(scans0.get(0).isDelta(), "a should be delta");
        Assertions.assertFalse(scans0.get(1).isDelta());
        Assertions.assertEquals(40, scans0.get(1).getTso(), "b bound to consumedTso=40");
    }

    @Test
    void testGenTwoTableJoinBothUpToDate() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 20, 20);
        addStream(streams, scanB, 40, 40);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertTrue(plans.isEmpty(), "Both up-to-date should produce no plans");
    }

    // ---------- Self-join ----------

    @Test
    void testGenSelfJoinBothOccurrencesPending() {
        LogicalOlapScan scanA1 = buildScanForTable(1, "a");
        LogicalOlapScan scanA2 = buildScanForTable(1, "a");
        Plan join = crossJoin(scanA1, scanA2);

        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scanA1, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a1) JOIN a2(consumedTso=10)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertTrue(scans0.get(0).isDelta());
        Assertions.assertFalse(scans0.get(1).isDelta());
        Assertions.assertEquals(10, scans0.get(1).getTso());

        // Plan 1: a1(latestTso=20) JOIN delta(a2)
        List<LogicalOlapScan> scans1 = collectScans(plans.get(1));
        Assertions.assertFalse(scans1.get(0).isDelta());
        Assertions.assertEquals(20, scans1.get(0).getTso());
        Assertions.assertTrue(scans1.get(1).isDelta());
    }

    // ---------- Three-table JOIN ----------

    @Test
    void testGenThreeTableJoinAllPending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        LogicalOlapScan scanC = buildScanForTable(3, "c");
        Plan abJoin = crossJoin(scanA, scanB);
        LogicalJoin<Plan, LogicalOlapScan> abcJoin = new LogicalJoin<>(
                JoinType.CROSS_JOIN, abJoin, scanC,
                new JoinReorderContext());

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);
        addStream(streams, scanC, 50, 60);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(3, plans.size());

        // Plan 0: delta(a) JOIN b(v1=30) JOIN c(v1=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertEquals(3, s0.size());
        Assertions.assertTrue(s0.get(0).isDelta());
        Assertions.assertEquals(30, s0.get(1).getTso(), "b bound to consumedTso");
        Assertions.assertEquals(50, s0.get(2).getTso(), "c bound to consumedTso");

        // Plan 1: a(v2=20) JOIN delta(b) JOIN c(v1=50)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso(), "a bound to latestTso");
        Assertions.assertTrue(s1.get(1).isDelta());
        Assertions.assertEquals(50, s1.get(2).getTso(), "c bound to consumedTso");

        // Plan 2: a(v2=20) JOIN b(v2=40) JOIN delta(c)
        List<LogicalOlapScan> s2 = collectScans(plans.get(2));
        Assertions.assertEquals(20, s2.get(0).getTso(), "a bound to latestTso");
        Assertions.assertEquals(40, s2.get(1).getTso(), "b bound to latestTso");
        Assertions.assertTrue(s2.get(2).isDelta());
    }

    @Test
    void testGenThreeTableJoinMiddleUpToDate() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        LogicalOlapScan scanC = buildScanForTable(3, "c");
        Plan abJoin = crossJoin(scanA, scanB);
        LogicalJoin<Plan, LogicalOlapScan> abcJoin = new LogicalJoin<>(
                JoinType.CROSS_JOIN, abJoin, scanC,
                new JoinReorderContext());

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 40, 40);  // up-to-date
        addStream(streams, scanC, 50, 60);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), NO_EXCLUSIONS);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(v1=40) JOIN c(v1=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertTrue(s0.get(0).isDelta());
        Assertions.assertEquals(40, s0.get(1).getTso());
        Assertions.assertEquals(50, s0.get(2).getTso());

        // Plan 1: a(v2=20) JOIN b(v2=40) JOIN delta(c)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso());
        Assertions.assertEquals(40, s1.get(1).getTso());
        Assertions.assertTrue(s1.get(2).isDelta());
    }

    // ---------- Excluded trigger table ----------

    @Test
    void testGenExcludedTriggerTableSkipped() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);

        // Exclude table with id=2 ("b")
        Predicate<LogicalOlapScan> excludeB = scan -> scan.getTable().getId() == 2;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), excludeB);

        // Only scanA is collected; scanB is excluded → 1 plan
        Assertions.assertEquals(1, plans.size());

        List<LogicalOlapScan> scans = collectScans(plans.get(0));
        Assertions.assertEquals(2, scans.size());
        // scanA (left) is delta
        Assertions.assertTrue(scans.get(0).isDelta());
        // scanB (right) is excluded — unchanged (isDelta=false, tso=-1)
        Assertions.assertFalse(scans.get(1).isDelta());
        Assertions.assertEquals(-1, scans.get(1).getTso());
    }

    @Test
    void testGenAllExcludedProducesNoPlan() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scanA, 10, 20);

        Predicate<LogicalOlapScan> excludeAll = scan -> true;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scanA, rewriteContext(streams), excludeAll);

        Assertions.assertTrue(plans.isEmpty());
    }

    @Test
    void testGenExcludedTableNotBoundToTso() {
        // In a 3-table join, if middle table is excluded, it should not get TSO binding
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        LogicalOlapScan scanC = buildScanForTable(3, "c");
        Plan abJoin = crossJoin(scanA, scanB);
        LogicalJoin<Plan, LogicalOlapScan> abcJoin = new LogicalJoin<>(
                JoinType.CROSS_JOIN, abJoin, scanC,
                new JoinReorderContext());

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);
        addStream(streams, scanC, 50, 60);

        // Exclude b (id=2)
        Predicate<LogicalOlapScan> excludeB = scan -> scan.getTable().getId() == 2;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), excludeB);

        // a and c are collected (both pending) → 2 plans
        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(unchanged) JOIN c(consumedTso=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertTrue(s0.get(0).isDelta());
        Assertions.assertFalse(s0.get(1).isDelta());
        Assertions.assertEquals(-1, s0.get(1).getTso(), "excluded b should not be bound");
        Assertions.assertEquals(50, s0.get(2).getTso(), "c bound to consumedTso");

        // Plan 1: a(latestTso=20) JOIN b(unchanged) JOIN delta(c)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso());
        Assertions.assertFalse(s1.get(1).isDelta());
        Assertions.assertEquals(-1, s1.get(1).getTso(), "excluded b should not be bound");
        Assertions.assertTrue(s1.get(2).isDelta());
    }

    // ---------- Missing stream ref ----------

    @Test
    void testGenMissingStreamRefThrows() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();

        Assertions.assertThrows(Exception.class,
                () -> new IvmDeltaRewriter().generateDeltaPlans(scanA, rewriteContext(streams), NO_EXCLUSIONS));
    }

    // ---------- TSO value correctness ----------

    @Test
    void testGenTsoBindingValues() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, IvmStreamRef> streams = new HashMap<>();
        addStream(streams, scanA, 100, 200);
        addStream(streams, scanB, 300, 400);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS);

        // Plan 0: delta(a) JOIN b(consumedTso_b=300)
        LogicalOlapScan b0 = collectScans(plans.get(0)).get(1);
        Assertions.assertEquals(300, b0.getTso());

        // Plan 1: a(latestTso_a=200) JOIN delta(b)
        LogicalOlapScan a1 = collectScans(plans.get(1)).get(0);
        Assertions.assertEquals(200, a1.getTso());
    }

    @Test
    void testGenDeltaScanHasDefaultTso() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS);

        LogicalOlapScan deltaScan = collectScans(plans.get(0)).get(0);
        Assertions.assertTrue(deltaScan.isDelta());
        Assertions.assertEquals(-1, deltaScan.getTso(), "Delta scan should not have TSO binding");
    }

    @Test
    void testGenLatestTsoLessThanConsumedTsoThrows() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        // latestTso (5) < consumedTso (100) — invalid lifecycle state
        Map<TableNameInfo, IvmStreamRef> streams = makeStreamsWithTso(scan, 100, 5);

        Assertions.assertThrows(IllegalStateException.class,
                () -> new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS));
    }
}
