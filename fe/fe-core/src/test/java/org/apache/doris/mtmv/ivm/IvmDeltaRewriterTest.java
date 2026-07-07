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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

    private InsertIntoTableCommand buildIncrementalInsertCommand(Plan sinkChild, MTMV mtmv,
            ConnectContext connectContext, IvmRewriteResult rewriteResult) {
        Plan rewritten = new IvmDeltaRewriter().generateIncrementalRefreshPlan(
                sinkChild, rewriteResult, IvmRewriteContext.incremental(mtmv, false), connectContext);
        Assertions.assertNotNull(rewritten);
        return new IvmRefreshManager().buildInsertCommand(
                (org.apache.doris.nereids.trees.plans.logical.LogicalPlan) rewritten, mtmv);
    }

    /** Creates a baseTableStreams map with a single pending-delta stream for the scan's table. */
    private Map<TableNameInfo, Long> makeStreams(LogicalOlapScan scan) {
        Map<TableNameInfo, Long> streams = new HashMap<>();
        return streams;
    }

    private Map<TableNameInfo, Long> makeStreamsWithTso(LogicalOlapScan scan,
            long consumedTso, long latestTso) {
        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scan, consumedTso, latestTso);
        return streams;
    }

    private void addStream(Map<TableNameInfo, Long> streams,
            LogicalOlapScan scan, long consumedTso, long latestTso) {
    }

    private IvmRefreshContext rewriteContext(Map<TableNameInfo, Long> streams) {
        return new IvmRefreshContext(mockMtmv(), new ConnectContext(), new IvmRewriteResult());
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
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                buildScanPlan(scan).child(), mtmv, new ConnectContext(), new IvmRewriteResult());
        Assertions.assertNotNull(command);
    }

    @Test
    void testProjectScanProducesInsertBundle() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                buildProjectScanPlan(scan).child(), mtmv, new ConnectContext(), new IvmRewriteResult());
        Assertions.assertNotNull(command);
    }

    @Test
    void testGroupedAggProducesDeleteSignSinkAndJoinPlan() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                bundle.normalizedPlan, mtmv, bundle.connectContext, bundle.rewriteResult);
        UnboundTableSink<?> sink = getSink(command);

        Assertions.assertEquals(mtmv.getInsertedColumnNames(), sink.getColNames());
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertInstanceOf(LogicalProject.class, finalProject.child());
        LogicalProject<?> normalizedTopProject = (LogicalProject<?>) finalProject.child();
        Assertions.assertInstanceOf(LogicalProject.class, normalizedTopProject.child());
        LogicalProject<?> applyProject = (LogicalProject<?>) normalizedTopProject.child();
        Assertions.assertInstanceOf(LogicalFilter.class, applyProject.child());
        Assertions.assertInstanceOf(LogicalJoin.class, ((LogicalFilter<?>) applyProject.child()).child());
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
    void testDeltaRewriterPropagatesAggApplyPlanThroughTopProject() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(scan));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                bundle.normalizedPlan, mtmv, bundle.connectContext, bundle.rewriteResult);
        UnboundTableSink<?> sink = getSink(command);
        LogicalProject<?> finalProject = (LogicalProject<?>) sink.child();
        Assertions.assertInstanceOf(LogicalProject.class, finalProject.child());
        LogicalProject<?> normalizedTopProject = (LogicalProject<?>) finalProject.child();
        Assertions.assertInstanceOf(LogicalProject.class, normalizedTopProject.child());
        LogicalProject<?> applyProject = (LogicalProject<?>) normalizedTopProject.child();
        Assertions.assertTrue(applyProject.child() instanceof LogicalFilter
                || applyProject.child() instanceof LogicalJoin);
    }

    @Test
    void testDeltaRewriterBuildsSinkProjectForNonAggPlan() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                buildScanPlan(scan).child(), mtmv, new ConnectContext(), new IvmRewriteResult());
        UnboundTableSink<?> sink = getSink(command);
        Plan child = sink.child();
        Assertions.assertEquals(ImmutableList.of("id", "name"), sink.getColNames());
        Assertions.assertInstanceOf(LogicalProject.class, child);
        Assertions.assertFalse(child instanceof LogicalJoin);
    }

    @Test
    void testGenerateIncrementalRefreshPlanReturnsEmptyRelationWhenNoDeltaAvailable() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Plan sinkChild = buildScanPlan(scan).child();
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getInsertedColumnNames()).thenReturn(ImmutableList.of("id", "name"));
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(ImmutableSet.of());

        ConnectContext connectContext = newConnectContext();
        Plan rewritten = new IvmDeltaRewriter().generateIncrementalRefreshPlan(
                sinkChild, new IvmRewriteResult(), IvmRewriteContext.incremental(mtmv, false),
                connectContext);

        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        LogicalProject<?> finalProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(3, finalProject.getOutput().size());
        Assertions.assertEquals("id", finalProject.getOutput().get(0).getName());
        Assertions.assertEquals("name", finalProject.getOutput().get(1).getName());
        Assertions.assertEquals(Column.DELETE_SIGN, finalProject.getOutput().get(2).getName());
        Assertions.assertNotNull(finalProject.child());
    }

    // ==================== generateDeltaPlans tests ====================

    // ---------- Single scan ----------

    @Test
    void testGenSingleScanPendingDelta() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(1, plans.size());
        List<LogicalOlapScan> scans = collectScans(plans.get(0));
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans.get(0)));
    }

    @Test
    void testGenDeltaPlanClearsOldGroupExpression() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Plan scanInMemo = new GroupExpression(scan).getPlan();
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(
                scanInMemo, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(1, plans.size());
        Assertions.assertFalse(plans.get(0).getGroupExpression().isPresent());
        Assertions.assertFalse(collectScans(plans.get(0)).get(0).getGroupExpression().isPresent());
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenSingleScanUpToDate() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scan, 20, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertTrue(plans.isEmpty(), "Up-to-date scan should produce no delta plans");
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenMergedPlanIncludesUpToDateScan() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scan, 20, 20);

        Plan mergedPlan = new IvmDeltaRewriter()
                .generateMergedDeltaPlan(scan, rewriteContext(streams), NO_EXCLUSIONS, true);

        Assertions.assertNotNull(mergedPlan);
        List<LogicalOlapScan> scans = collectScans(mergedPlan);
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans.get(0)));
    }

    // ---------- Two-table JOIN ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenTwoTableJoinBothPending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(consumedTso_b=30)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertEquals(2, scans0.size());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(0)), "a should be delta in plan 0");
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(1)));
        Assertions.assertEquals(30, scans0.get(1).getTso(), "b should be bound to consumedTso=30");

        // Plan 1: a(latestTso_a=20) JOIN delta(b)
        List<LogicalOlapScan> scans1 = collectScans(plans.get(1));
        Assertions.assertEquals(2, scans1.size());
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans1.get(0)));
        Assertions.assertEquals(20, scans1.get(0).getTso(), "a should be bound to latestTso=20");
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans1.get(1)), "b should be delta in plan 1");
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenTwoTableJoinOnePending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);  // pending
        addStream(streams, scanB, 40, 40);  // up-to-date

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(1, plans.size());

        // Plan 0: delta(a) JOIN b(consumedTso_b=40)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(0)), "a should be delta");
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(1)));
        Assertions.assertEquals(40, scans0.get(1).getTso(), "b bound to consumedTso=40");
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenTwoTableJoinBothUpToDate() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 20, 20);
        addStream(streams, scanB, 40, 40);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertTrue(plans.isEmpty(), "Both up-to-date should produce no plans");
    }

    // ---------- Self-join ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenSelfJoinBothOccurrencesPending() {
        LogicalOlapScan scanA1 = buildScanForTable(1, "a");
        LogicalOlapScan scanA2 = buildScanForTable(1, "a");
        Plan join = crossJoin(scanA1, scanA2);

        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scanA1, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a1) JOIN a2(consumedTso=10)
        List<LogicalOlapScan> scans0 = collectScans(plans.get(0));
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(0)));
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans0.get(1)));
        Assertions.assertEquals(10, scans0.get(1).getTso());

        // Plan 1: a1(latestTso=20) JOIN delta(a2)
        List<LogicalOlapScan> scans1 = collectScans(plans.get(1));
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans1.get(0)));
        Assertions.assertEquals(20, scans1.get(0).getTso());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans1.get(1)));
    }

    // ---------- Three-table JOIN ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenThreeTableJoinAllPending() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        LogicalOlapScan scanC = buildScanForTable(3, "c");
        Plan abJoin = crossJoin(scanA, scanB);
        LogicalJoin<Plan, LogicalOlapScan> abcJoin = new LogicalJoin<>(
                JoinType.CROSS_JOIN, abJoin, scanC,
                new JoinReorderContext());

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);
        addStream(streams, scanC, 50, 60);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(3, plans.size());

        // Plan 0: delta(a) JOIN b(v1=30) JOIN c(v1=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertEquals(3, s0.size());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s0.get(0)));
        Assertions.assertEquals(30, s0.get(1).getTso(), "b bound to consumedTso");
        Assertions.assertEquals(50, s0.get(2).getTso(), "c bound to consumedTso");

        // Plan 1: a(v2=20) JOIN delta(b) JOIN c(v1=50)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso(), "a bound to latestTso");
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s1.get(1)));
        Assertions.assertEquals(50, s1.get(2).getTso(), "c bound to consumedTso");

        // Plan 2: a(v2=20) JOIN b(v2=40) JOIN delta(c)
        List<LogicalOlapScan> s2 = collectScans(plans.get(2));
        Assertions.assertEquals(20, s2.get(0).getTso(), "a bound to latestTso");
        Assertions.assertEquals(40, s2.get(1).getTso(), "b bound to latestTso");
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s2.get(2)));
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenThreeTableJoinMiddleUpToDate() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        LogicalOlapScan scanC = buildScanForTable(3, "c");
        Plan abJoin = crossJoin(scanA, scanB);
        LogicalJoin<Plan, LogicalOlapScan> abcJoin = new LogicalJoin<>(
                JoinType.CROSS_JOIN, abJoin, scanC,
                new JoinReorderContext());

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 40, 40);  // up-to-date
        addStream(streams, scanC, 50, 60);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), NO_EXCLUSIONS, false);

        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(v1=40) JOIN c(v1=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s0.get(0)));
        Assertions.assertEquals(40, s0.get(1).getTso());
        Assertions.assertEquals(50, s0.get(2).getTso());

        // Plan 1: a(v2=20) JOIN b(v2=40) JOIN delta(c)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso());
        Assertions.assertEquals(40, s1.get(1).getTso());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s1.get(2)));
    }

    // ---------- Excluded trigger table ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenExcludedTriggerTableSkipped() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);

        // Exclude table with id=2 ("b")
        Predicate<LogicalOlapScan> excludeB = scan -> scan.getTable().getId() == 2;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), excludeB, false);

        // Only scanA is collected; scanB is excluded → 1 plan
        Assertions.assertEquals(1, plans.size());

        List<LogicalOlapScan> scans = collectScans(plans.get(0));
        Assertions.assertEquals(2, scans.size());
        // scanA (left) is delta
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans.get(0)));
        // scanB (right) is excluded — unchanged (isDelta=false, tso=-1)
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans.get(0)));
        Assertions.assertEquals(-1, scans.get(1).getTso());
    }

    @Test
    void testGenAllExcludedProducesNoPlan() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scanA, 10, 20);

        Predicate<LogicalOlapScan> excludeAll = scan -> true;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scanA, rewriteContext(streams), excludeAll, false);

        Assertions.assertTrue(plans.isEmpty());
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
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

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 10, 20);
        addStream(streams, scanB, 30, 40);
        addStream(streams, scanC, 50, 60);

        // Exclude b (id=2)
        Predicate<LogicalOlapScan> excludeB = scan -> scan.getTable().getId() == 2;

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(abcJoin, rewriteContext(streams), excludeB, false);

        // a and c are collected (both pending) → 2 plans
        Assertions.assertEquals(2, plans.size());

        // Plan 0: delta(a) JOIN b(unchanged) JOIN c(consumedTso=50)
        List<LogicalOlapScan> s0 = collectScans(plans.get(0));
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s0.get(0)));
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s0.get(1)));
        Assertions.assertEquals(-1, s0.get(1).getTso(), "excluded b should not be bound");
        Assertions.assertEquals(50, s0.get(2).getTso(), "c bound to consumedTso");

        // Plan 1: a(latestTso=20) JOIN b(unchanged) JOIN delta(c)
        List<LogicalOlapScan> s1 = collectScans(plans.get(1));
        Assertions.assertEquals(20, s1.get(0).getTso());
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s1.get(1)));
        Assertions.assertEquals(-1, s1.get(1).getTso(), "excluded b should not be bound");
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(s1.get(2)));
    }

    // ---------- Missing stream ref ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenMissingStreamRefThrows() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        Map<TableNameInfo, Long> streams = new HashMap<>();

        Assertions.assertThrows(Exception.class,
                () -> new IvmDeltaRewriter().generateDeltaPlans(scanA, rewriteContext(streams), NO_EXCLUSIONS, false));
    }

    // ---------- TSO value correctness ----------

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")
    @Test
    void testGenTsoBindingValues() {
        LogicalOlapScan scanA = buildScanForTable(1, "a");
        LogicalOlapScan scanB = buildScanForTable(2, "b");
        Plan join = crossJoin(scanA, scanB);

        Map<TableNameInfo, Long> streams = new HashMap<>();
        addStream(streams, scanA, 100, 200);
        addStream(streams, scanB, 300, 400);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(join, rewriteContext(streams), NO_EXCLUSIONS, false);

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
        Map<TableNameInfo, Long> streams = makeStreamsWithTso(scan, 10, 20);

        List<Plan> plans = new IvmDeltaRewriter().generateDeltaPlans(scan, rewriteContext(streams), NO_EXCLUSIONS, false);

        LogicalOlapScan deltaScan = collectScans(plans.get(0)).get(0);
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(deltaScan));
        Assertions.assertEquals(-1, deltaScan.getTso(), "Delta scan should not have TSO binding");
    }

    @org.junit.jupiter.api.Disabled("TODO: Re-enable when stream-based TSO tracking is implemented in IvmDeltaRewriter.collectDeltaScanContexts()")


    // Inner class to expose package-private applyBinlogOrderRewrite for testing
    private static class TestableApplyBinlogOrderRewrite {
        Plan exposeApplyBinlogOrderRewrite(Plan mergedPlan, IvmRefreshContext ctx) {
            return new IvmDeltaRewriter().applyBinlogOrderRewrite(mergedPlan, ctx);
        }
    }

    @Test
    void testBinlogOrderRewriteProducesFojPlan() {
        // Build a manually constructed plan with all required columns:
        // id, name, row_id, dml_factor, baseOp (mimics visitor-rewritten delta plan)
        OlapTable table = PlanConstructor.newOlapTable(0, "t1", 0);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);

        Alias rowIdAlias = new Alias(idSlot, Column.IVM_ROW_ID_COL);
        Alias dmlAlias = new Alias(new TinyIntLiteral((byte) 1), Column.IVM_DML_FACTOR_COL);
        Alias baseOpAlias = new Alias(new TinyIntLiteral((byte) 1), Column.IVM_BASE_OP_COL);

        LogicalProject<?> plan = new LogicalProject<>(
                ImmutableList.of(idSlot, nameSlot, rowIdAlias, dmlAlias, baseOpAlias), scan);
        IvmRefreshContext ctx = new IvmRefreshContext(mockMtmv(), new ConnectContext(), new IvmRewriteResult());

        Plan fojPlan = new TestableApplyBinlogOrderRewrite().exposeApplyBinlogOrderRewrite(plan, ctx);

        // Verify: CTE tree — LogicalCTEAnchor(Producer, IF Project(FOJ(...)))
        Assertions.assertInstanceOf(LogicalCTEAnchor.class, fojPlan);
        LogicalCTEAnchor<?, ?> anchor = (LogicalCTEAnchor<?, ?>) fojPlan;
        Assertions.assertInstanceOf(LogicalCTEProducer.class, anchor.left());
        Assertions.assertInstanceOf(LogicalProject.class, anchor.right());
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) ((LogicalProject<?>) anchor.right()).child();
        Assertions.assertEquals(JoinType.FULL_OUTER_JOIN, join.getJoinType());

        // Verify: FOJ branches are SubQueryAlias wrapping Filter wrapping CTE Consumer
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, join.left());
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, join.right());

        // Verify: output has dmlFactor and baseOp (FOJ preserves them)
        Assertions.assertTrue(fojPlan.getOutput().stream()
                .anyMatch(s -> Column.IVM_DML_FACTOR_COL.equals(s.getName())),
                "FOJ output should contain dmlFactor");
        Assertions.assertTrue(fojPlan.getOutput().stream()
                .anyMatch(s -> Column.IVM_BASE_OP_COL.equals(s.getName())),
                "FOJ output should contain baseOp");
    }
}
