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
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.analysis.IvmNormalizeMTMV;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class IvmDeltaRewriterTest extends IvmDeltaTestBase {

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getId()).thenReturn(0L);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Sets.newHashSet());
        Mockito.when(mtmv.getInsertedColumnNames()).thenReturn(ImmutableList.of("id", "name"));
        return mtmv;
    }

    private InsertIntoTableCommand buildIncrementalInsertCommand(Plan sinkChild, MTMV mtmv,
            ConnectContext connectContext, IvmRewriteResult rewriteResult) {
        ensureStatementContext(connectContext);
        Plan rewritten = new IvmDeltaRewriter().generateIncrRefreshPlan(
                sinkChild, rewriteResult, IvmRewriteContext.incremental(mtmv, false), connectContext);
        Assertions.assertNotNull(rewritten);
        return new IvmIncrRefreshManager().buildInsertCommand(
                (org.apache.doris.nereids.trees.plans.logical.LogicalPlan) rewritten, mtmv);
    }

    private PlanBundle normalizePlan(Plan plan) {
        ConnectContext connectContext = newConnectContext();
        JobContext jobContext = newJobContextForRoot(plan, connectContext);
        Plan normalizedPlan = new IvmNormalizeMTMV().rewriteRoot(plan, jobContext);
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        return new PlanBundle(connectContext, normalizedPlan, rewriteResult);
    }

    private Plan generateMergedDelta(Plan plan, boolean includeExhaustedStreams) {
        return generateMergedDelta(plan, includeExhaustedStreams, null);
    }

    private Plan generateMergedDelta(Plan plan, boolean includeExhaustedStreams, String excludedTriggerTables) {
        ConnectContext connectContext = newConnectContext();
        JobContext jobContext = newJobContextForRoot(plan, connectContext);
        Plan normalizedPlan = new IvmNormalizeMTMV().rewriteRoot(plan, jobContext);
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        MTMV mtmv = buildMtmvFromPlan(normalizedPlan.getOutput());
        if (excludedTriggerTables != null) {
            mtmv.setMvProperties(ImmutableMap.of(
                    PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, excludedTriggerTables));
        }
        return new IvmDeltaRewriter().generateIncrRefreshPlan(normalizedPlan, rewriteResult,
                IvmRewriteContext.incremental(mtmv, includeExhaustedStreams), connectContext);
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
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizePlan(buildScanPlan(scan).child());
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                bundle.normalizedPlan, mtmv, bundle.connectContext, bundle.rewriteResult);
        Assertions.assertNotNull(command);
    }

    @Test
    void testProjectScanProducesInsertBundle() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizePlan(buildProjectScanPlan(scan).child());
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                bundle.normalizedPlan, mtmv, bundle.connectContext, bundle.rewriteResult);
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
                () -> new IvmIncrRefreshContext(null, new ConnectContext(), null, false));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmIncrRefreshContext(mtmv, null, null, false));
        Assertions.assertTrue(new IvmIncrRefreshContext(mtmv, new ConnectContext(), new IvmRewriteResult(), true)
                .isIncludeExhaustedStreams());
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
    void testAggJoinWithTwoDeltaBranchesKeepsAggInputsBound() {
        LogicalOlapScan leftScan = buildScanForTable(201, "agg_join_left_tbl");
        LogicalOlapScan rightScan = buildScanForTable(202, "agg_join_right_tbl");
        LogicalProject<LogicalOlapScan> leftProject = new LogicalProject<>(
                ImmutableList.of(
                        new Alias(leftScan.getOutput().get(0), "left_id")),
                leftScan);
        LogicalProject<LogicalOlapScan> rightProject = new LogicalProject<>(
                ImmutableList.of(
                        new Alias(rightScan.getOutput().get(0), "right_id")),
                rightScan);
        LogicalJoin<LogicalProject<LogicalOlapScan>, LogicalProject<LogicalOlapScan>> join = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(leftProject.getOutput().get(0), rightProject.getOutput().get(0))),
                leftProject, rightProject, JoinReorderContext.EMPTY);
        Slot groupKey = join.getOutput().get(0);
        LogicalAggregate<LogicalJoin<LogicalProject<LogicalOlapScan>, LogicalProject<LogicalOlapScan>>> agg =
                new LogicalAggregate<>(
                ImmutableList.of(groupKey),
                ImmutableList.of(groupKey, new Alias(new Count(), "cnt"),
                        new Alias(new Sum(new Add(join.getOutput().get(0), join.getOutput().get(1))),
                                "total")),
                true, Optional.empty(), join);
        PlanBundle bundle = normalizeAggPlan(agg);
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        mtmv.setId(201_001L);
        registerTestStream(leftScan.getTable(), mtmv.getId());
        registerTestStream(rightScan.getTable(), mtmv.getId());
        bumpBaseTableTso(leftScan.getTable(), 20);
        setStreamOffset(leftScan.getTable(), getRegisteredStream(leftScan.getTable(), mtmv.getId()), 10);
        bumpBaseTableTso(rightScan.getTable(), 20);
        setStreamOffset(rightScan.getTable(), getRegisteredStream(rightScan.getTable(), mtmv.getId()), 10);

        Plan rewritten = new IvmDeltaRewriter().generateIncrRefreshPlan(
                bundle.normalizedPlan, bundle.rewriteResult,
                IvmRewriteContext.incremental(mtmv, false), bundle.connectContext);

        new CheckAfterRewrite().checkTreeAllSlotReferenceFromChildren(rewritten);
    }

    @Test
    void testDeltaRewriterBuildsSinkProjectForNonAggPlan() {
        LogicalOlapScan scan = buildScan();
        PlanBundle bundle = normalizePlan(buildScanPlan(scan).child());
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        InsertIntoTableCommand command = buildIncrementalInsertCommand(
                bundle.normalizedPlan, mtmv, bundle.connectContext, bundle.rewriteResult);
        UnboundTableSink<?> sink = getSink(command);
        Plan child = sink.child();
        Assertions.assertEquals(ImmutableList.of(Column.IVM_ROW_ID_COL, "id", "name"), sink.getColNames());
        Assertions.assertInstanceOf(LogicalProject.class, child);
        Assertions.assertFalse(child instanceof LogicalJoin);
    }

    @Test
    void testGenerateIncrementalRefreshPlanBuildsDeleteSignProjectWhenDeltaAvailable() {
        LogicalOlapScan scan = buildScanForTable(1, "a");
        PlanBundle bundle = normalizePlan(buildScanPlan(scan).child());
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        Plan rewritten = new IvmDeltaRewriter().generateIncrRefreshPlan(
                bundle.normalizedPlan, bundle.rewriteResult, IvmRewriteContext.incremental(mtmv, false),
                bundle.connectContext);

        Assertions.assertInstanceOf(LogicalProject.class, rewritten);
        LogicalProject<?> finalProject = (LogicalProject<?>) rewritten;
        Assertions.assertEquals(5, finalProject.getOutput().size());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, finalProject.getOutput().get(0).getName());
        Assertions.assertEquals("id", finalProject.getOutput().get(1).getName());
        Assertions.assertEquals("name", finalProject.getOutput().get(2).getName());
        Assertions.assertEquals(Column.SEQUENCE_COL, finalProject.getOutput().get(3).getName());
        Assertions.assertEquals(Column.DELETE_SIGN, finalProject.getOutput().get(4).getName());
        Assertions.assertNotNull(finalProject.child());
    }

    @Test
    void testSingleScanPendingDeltaUsesIncrementalStream() {
        LogicalOlapScan scan = buildScanForTable(101, "single_pending");
        bumpBaseTableTso(scan.getTable(), 20);
        setStreamOffset(scan.getTable(), getRegisteredStream(scan.getTable(), 1L), 10);

        Plan rewritten = generateMergedDelta(scan, false);

        List<LogicalOlapScan> scans = collectScans(rewritten);
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scans.get(0)));
    }

    @Test
    void testSingleScanUpToDateProducesEmptyRelation() {
        LogicalOlapScan scan = buildScanForTable(102, "single_up_to_date");
        bumpBaseTableTso(scan.getTable(), 20);
        advanceStreamToBaseTable(scan.getTable(), getRegisteredStream(scan.getTable(), 1L));

        Assertions.assertInstanceOf(LogicalEmptyRelation.class, generateMergedDelta(scan, false));
    }

    @Test
    void testIncludeUpToDateScanUsesIncrementalStream() {
        LogicalOlapScan scan = buildScanForTable(103, "single_include_up_to_date");
        bumpBaseTableTso(scan.getTable(), 20);
        advanceStreamToBaseTable(scan.getTable(), getRegisteredStream(scan.getTable(), 1L));

        Plan rewritten = generateMergedDelta(scan, true);

        Assertions.assertTrue(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(collectScans(rewritten).get(0)));
    }

    @Test
    void testRecursiveJoinDeltaMergesBothScanDeltas() {
        LogicalOlapScan left = buildScanForTable(301, "recursive_left");
        LogicalOlapScan right = buildScanForTable(302, "recursive_right");
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = crossJoin(left, right);
        bumpBaseTableTso(left.getTable(), 20);
        setStreamOffset(left.getTable(), getRegisteredStream(left.getTable(), 1L), 10);
        bumpBaseTableTso(right.getTable(), 20);
        setStreamOffset(right.getTable(), getRegisteredStream(right.getTable(), 1L), 10);

        Plan rewritten = generateMergedDelta(join, false);

        Assertions.assertEquals(2, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
        Assertions.assertEquals(1, rewritten.collectToList(node -> node instanceof LogicalUnion).size());
    }

    @Test
    void testTwoTableJoinWithOnePendingDeltaHasOneContribution() {
        LogicalOlapScan left = buildScanForTable(104, "join_left_pending");
        LogicalOlapScan right = buildScanForTable(105, "join_right_up_to_date");
        bumpBaseTableTso(left.getTable(), 20);
        setStreamOffset(left.getTable(), getRegisteredStream(left.getTable(), 1L), 10);
        bumpBaseTableTso(right.getTable(), 20);
        advanceStreamToBaseTable(right.getTable(), getRegisteredStream(right.getTable(), 1L));

        Plan rewritten = generateMergedDelta(crossJoin(left, right), false);

        Assertions.assertEquals(1, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
        Assertions.assertTrue(rewritten.collectToList(node -> node instanceof LogicalUnion).isEmpty());
    }

    @Test
    void testTwoTableJoinWithNoPendingDeltaProducesEmptyRelation() {
        LogicalOlapScan left = buildScanForTable(106, "join_left_up_to_date");
        LogicalOlapScan right = buildScanForTable(107, "join_right_no_pending");
        bumpBaseTableTso(left.getTable(), 20);
        advanceStreamToBaseTable(left.getTable(), getRegisteredStream(left.getTable(), 1L));
        bumpBaseTableTso(right.getTable(), 20);
        advanceStreamToBaseTable(right.getTable(), getRegisteredStream(right.getTable(), 1L));

        Assertions.assertInstanceOf(LogicalEmptyRelation.class, generateMergedDelta(crossJoin(left, right), false));
    }

    @Test
    void testSelfJoinProducesOneContributionPerScanOccurrence() {
        LogicalOlapScan left = buildScanForTable(108, "self_join");
        LogicalOlapScan right = buildScanForTable(108, "self_join");
        bumpBaseTableTso(left.getTable(), 20);
        setStreamOffset(left.getTable(), getRegisteredStream(left.getTable(), 1L), 10);

        Plan rewritten = generateMergedDelta(crossJoin(left, right), false);

        Assertions.assertEquals(2, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
        Assertions.assertEquals(1, rewritten.collectToList(node -> node instanceof LogicalUnion).size());
    }

    @Test
    void testThreeTableJoinWithAllPendingDeltas() {
        LogicalOlapScan first = buildScanForTable(109, "three_first");
        LogicalOlapScan second = buildScanForTable(110, "three_second");
        LogicalOlapScan third = buildScanForTable(111, "three_third");
        for (LogicalOlapScan scan : ImmutableList.of(first, second, third)) {
            bumpBaseTableTso(scan.getTable(), 20);
            setStreamOffset(scan.getTable(), getRegisteredStream(scan.getTable(), 1L), 10);
        }
        Plan plan = new LogicalJoin<>(JoinType.CROSS_JOIN, crossJoin(first, second), third,
                JoinReorderContext.EMPTY);

        Plan rewritten = generateMergedDelta(plan, false);

        Assertions.assertEquals(3, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
    }

    @Test
    void testBalancedFourTableJoinSharesOppositeSnapshotSubtree() {
        LogicalOlapScan first = buildScanForTable(115, "balanced_first");
        LogicalOlapScan second = buildScanForTable(116, "balanced_second");
        LogicalOlapScan third = buildScanForTable(117, "balanced_third");
        LogicalOlapScan fourth = buildScanForTable(118, "balanced_fourth");
        for (LogicalOlapScan scan : ImmutableList.of(first, second, third, fourth)) {
            bumpBaseTableTso(scan.getTable(), 20);
            setStreamOffset(scan.getTable(), getRegisteredStream(scan.getTable(), 1L), 10);
        }
        Plan plan = new LogicalJoin<>(JoinType.CROSS_JOIN, crossJoin(first, second), crossJoin(third, fourth),
                JoinReorderContext.EMPTY);

        Plan rewritten = generateMergedDelta(plan, false);

        Assertions.assertEquals(4, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
        // Two top-level contributions each contain one delta subtree, one opposite snapshot subtree, and the
        // top-level join. Rebuilding all four delta plans would create 12 joins instead of 8.
        Assertions.assertEquals(8, rewritten.collectToList(node -> node instanceof LogicalJoin).size());
    }

    @Test
    void testThreeTableJoinSkipsUpToDateMiddleScan() {
        LogicalOlapScan first = buildScanForTable(112, "three_pending_first");
        LogicalOlapScan second = buildScanForTable(113, "three_up_to_date_middle");
        LogicalOlapScan third = buildScanForTable(114, "three_pending_third");
        bumpBaseTableTso(first.getTable(), 20);
        setStreamOffset(first.getTable(), getRegisteredStream(first.getTable(), 1L), 10);
        bumpBaseTableTso(second.getTable(), 20);
        advanceStreamToBaseTable(second.getTable(), getRegisteredStream(second.getTable(), 1L));
        bumpBaseTableTso(third.getTable(), 20);
        setStreamOffset(third.getTable(), getRegisteredStream(third.getTable(), 1L), 10);
        Plan plan = new LogicalJoin<>(JoinType.CROSS_JOIN, crossJoin(first, second), third,
                JoinReorderContext.EMPTY);

        Plan rewritten = generateMergedDelta(plan, false);

        Assertions.assertEquals(2, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
    }

    @Test
    void testExcludedTriggerTableDoesNotContributeDelta() {
        LogicalOlapScan left = buildScanForTable(115, "excluded_left");
        LogicalOlapScan right = buildScanForTable(116, "excluded_right");
        for (LogicalOlapScan scan : ImmutableList.of(left, right)) {
            bumpBaseTableTso(scan.getTable(), 20);
            setStreamOffset(scan.getTable(), getRegisteredStream(scan.getTable(), 1L), 10);
        }

        Plan rewritten = generateMergedDelta(crossJoin(left, right), false, "excluded_right");

        Assertions.assertEquals(1, rewritten.collectToList(node -> node instanceof LogicalOlapScan
                && IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan((LogicalOlapScan) node)).size());
    }

    @Test
    void testAllExcludedTriggerTablesProduceEmptyRelation() {
        LogicalOlapScan scan = buildScanForTable(117, "all_excluded");
        bumpBaseTableTso(scan.getTable(), 20);
        setStreamOffset(scan.getTable(), getRegisteredStream(scan.getTable(), 1L), 10);

        Assertions.assertInstanceOf(LogicalEmptyRelation.class,
                generateMergedDelta(scan, false, "all_excluded"));
    }

}
