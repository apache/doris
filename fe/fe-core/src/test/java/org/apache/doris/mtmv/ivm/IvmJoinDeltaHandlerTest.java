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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class IvmJoinDeltaHandlerTest extends IvmDeltaTestBase {

    private final class TestableIvmJoinDeltaHandler {

        private IvmDeltaRewriteResult exposeRewritePlan(Plan plan, IvmIncrRefreshContext ctx) {
            return exposeRewritePlanOptional(plan, ctx).orElseThrow(AssertionError::new);
        }

        private Optional<IvmDeltaRewriteResult> exposeRewritePlanOptional(Plan plan, IvmIncrRefreshContext ctx) {
            Map<OlapTable, OlapTableStream> streams = new HashMap<>();
            plan.collectToList(LogicalOlapScan.class::isInstance).forEach(node -> {
                LogicalOlapScan scan = (LogicalOlapScan) node;
                if (!(scan instanceof LogicalOlapTableStreamScan)) {
                    OlapTable table = (OlapTable) scan.getTable();
                    OlapTableStream stream = getRegisteredStream(table, 0L);
                    if (stream != null) {
                        streams.put(table, stream);
                    }
                }
            });
            IvmDeltaRewriteVisitor visitor = new IvmDeltaRewriteVisitor(
                    new IvmLinearDeltaHandler(), new IvmJoinDeltaHandler(), new IvmAggDeltaHandler(),
                    new IvmDeltaRewriteState(streams, true, 0));
            return visitor.rewritePlan(plan, ctx);
        }
    }

    private LogicalOlapScan buildSnapshotScanForTable(long tableId, String tableName) {
        LogicalOlapScan scan = buildScanForTable(tableId, tableName);
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test_db");
        if (db != null) {
            db.unregisterTable(IvmUtil.streamName(0L, scan.getTable().getFullQualifiers()));
            db.unregisterTable(IvmUtil.streamName(1L, scan.getTable().getFullQualifiers()));
        }
        return scan;
    }

    private IvmIncrRefreshContext newRefreshContext(Plan plan) {
        return new IvmIncrRefreshContext(buildMtmvFromPlan(plan.getOutput()), new ConnectContext(), null, false);
    }

    private IvmIncrRefreshContext newRefreshContext(Plan plan, IvmRewriteResult rewriteResult) {
        return new IvmIncrRefreshContext(buildMtmvFromPlan(plan.getOutput()), new ConnectContext(), rewriteResult, false);
    }

    @Test
    void testInnerJoinDmlFactorPropagationLeft() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testInnerJoinDmlFactorPropagationRight() {
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(10, "t10");
        LogicalOlapScan scanDelta = buildScan();
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanSnapshot, scanDelta, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testCrossJoinDmlFactorPropagation() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.CROSS_JOIN,
                scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
    }

    @Test
    void testInnerJoinBothDeltaThrows() {
        LogicalOlapScan scanA = buildScanForTable(1, "t1");
        LogicalOlapScan scanB = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));
        Assertions.assertNotNull(result.dmlFactorSlot);
    }

    @Test
    void testInnerJoinWithoutDeltaReturnsNullDmlFactor() {
        LogicalOlapScan scanA = buildSnapshotScanForTable(101, "t101");
        LogicalOlapScan scanB = buildSnapshotScanForTable(102, "t102");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        Assertions.assertFalse(handler.exposeRewritePlanOptional(join, newRefreshContext(join)).isPresent());
    }

    @Test
    void testIncrementalRewriterFreshensExprIdsAcrossInnerJoinDeltaBundles() {
        LogicalProject<Plan> normalizedPlan = normalizedInnerJoin(
                rowIdProject(buildScanForTable(1011, "t1011")),
                rowIdProject(buildScanForTable(1012, "t1012")));
        IvmRewriteResult rewriteResult = deterministicRewriteResult(normalizedPlan);
        IvmIncrRefreshContext ctx = newRefreshContext(normalizedPlan, rewriteResult);

        Plan mergedPlan = new IvmDeltaRewriter().generateIncrRefreshPlan(normalizedPlan, rewriteResult,
                IvmRewriteContext.incremental(ctx.getMtmv(), true), ctx.getConnectContext());
        LogicalUnion union = findOnlyUnion(mergedPlan);

        Assertions.assertEquals(2, union.children().size());
        assertUnionChildrenAlign(union);
        assertUnionOutputsDoNotReuseChildExprIds(union);
        assertUnionChildrenDoNotShareExprIds(union);
    }

    @Test
    void testInnerJoinNonDetGuardAdded() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.addRowId(rowIdSlot, false);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmIncrRefreshContext rewriteCtx = newRefreshContext(join, rewriteResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertFalse(result.plan.toString().contains(
                IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX));
    }

    @Test
    void testInnerJoinDeterministicRowIdSkipsGuard() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.addRowId(rowIdSlot, true);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmIncrRefreshContext rewriteCtx = newRefreshContext(join, rewriteResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertFalse(result.plan.toString().contains(
                IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX));
    }

    @Test
    void testInnerJoinDoesNotAddRootGuard() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.addRowId(rowIdSlot, false);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmIncrRefreshContext rewriteCtx = newRefreshContext(join, rewriteResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertFalse(result.plan.toString().contains(
                IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX));
    }

    @Test
    void testInnerJoinDmlFactorWithHashConjuncts() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildSnapshotScanForTable(2, "t2");
        EqualTo condition = new EqualTo(scanDelta.getOutput().get(0), scanSnapshot.getOutput().get(0));
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(condition), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    private static final class NormalizedOuterJoinPlan {
        private final LogicalProject<Plan> topProject;
        private final int leftOutputSize;
        private final IvmRewriteResult rewriteResult;

        private NormalizedOuterJoinPlan(LogicalProject<Plan> topProject, int leftOutputSize,
                IvmRewriteResult rewriteResult) {
            this.topProject = topProject;
            this.leftOutputSize = leftOutputSize;
            this.rewriteResult = rewriteResult;
        }
    }

    @Test
    void testRetainedSideDeltaUsesLeftOuterJoinUnderTopProject() {
        LogicalOlapScan leftDelta = buildScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        assertSingleFinalRowId(topProject);
    }

    @Test
    void testNullSideDeltaBuildsRightEventsAndProbesLeftOnce() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        assertSingleFinalRowId(topProject);
        LogicalUnion union = findOnlyUnion(result.plan);
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertUnionOutputsDoNotReuseChildExprIds(union);

        LogicalProject<?> preNullProject = (LogicalProject<?>) union.child(1);
        LogicalProject<?> postNullProject = (LogicalProject<?>) union.child(2);
        assertNullSideRepairEvent(preNullProject, (byte) -1);
        assertNullSideRepairEvent(postNullProject, (byte) 1);
        assertContainsSubQueryAlias(preNullProject, "__DORIS_IVM_NULL_SIDE_KEY_DELTA__");
        assertContainsSubQueryAlias(preNullProject, "__DORIS_IVM_NULL_SIDE_PRE_SNAPSHOT__");
        assertContainsSubQueryAlias(postNullProject, "__DORIS_IVM_NULL_SIDE_KEY_DELTA__");
        assertContainsSubQueryAlias(postNullProject, "__DORIS_IVM_NULL_SIDE_POST_SNAPSHOT__");
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testRightOuterJoinNullSideDeltaBuildsNullSideEvents() {
        LogicalOlapScan leftDelta = buildScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoin(rowIdProject(leftDelta),
                rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        List<LogicalUnion> unions = result.plan.collectToList(node -> node instanceof LogicalUnion);
        if (!unions.isEmpty()) {
            LogicalUnion union = unions.get(0);
            Assertions.assertEquals(3, union.children().size());
            assertUnionChildrenAlign(union);
            assertNullSideRepairEvent((LogicalProject<?>) union.child(1), (byte) -1);
            assertNullSideRepairEvent((LogicalProject<?>) union.child(2), (byte) 1);
        }
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testFullOuterJoinLeftDeltaUsesLeftOuterJoinAndRepairsRightDanglingRows() {
        LogicalOlapScan leftDelta = buildScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedFullOuterJoin(rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        Assertions.assertTrue(union.child(0).collectToList(node -> node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType().isOuterJoin()).size() >= 1);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) -1);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(2), bundle.leftOutputSize, (byte) 1);
        assertUnionChildrenAlign(union);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testFullOuterJoinRightDeltaUsesRightOuterJoinAndRepairsLeftDanglingRows() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedFullOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        assertProjectedJoinBranch(union.child(0), JoinType.RIGHT_OUTER_JOIN);
        assertRightNullSideRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) -1);
        assertRightNullSideRowId((LogicalProject<?>) union.child(2), bundle.leftOutputSize, (byte) 1);
        assertUnionChildrenAlign(union);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testNullSideDeltaExtractsHashConjunctFromOtherConjuncts() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithOnlyOtherHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalUnion union = findOnlyUnion(result.plan);
        Assertions.assertEquals(3, union.children().size());
    }

    @Test
    void testNullSideDeltaWithNonHashOtherConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        assertProjectedJoinBranch(union.child(0), JoinType.INNER_JOIN);
        assertUnionChildrenAlign(union);
        LogicalProject<?> preNullProject = (LogicalProject<?>) union.child(1);
        LogicalProject<?> postNullProject = (LogicalProject<?>) union.child(2);
        assertRightNullSideRowId(preNullProject, bundle.leftOutputSize, (byte) -1);
        assertRightNullSideRowId(postNullProject, bundle.leftOutputSize, (byte) 1);
    }

    @Test
    void testRightOuterJoinNullSideDeltaWithNonHashOtherConjunctFillsLeftSideAsNull() {
        LogicalOlapScan leftDelta = buildScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(2, union.children().size());
        assertUnionChildrenAlign(union);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) 1);
    }

    @Test
    void testNullSideDeltaWithUniqueFunctionHashConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithUniqueFunctionHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        Assertions.assertEquals(3, ((LogicalUnion) joinOutputProject.child()).children().size());
    }

    @Test
    void testRewriteBuildsSinkWithFinalRowIdAndDeleteSign() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);
        IvmDeltaRewriteResult result = new TestableIvmJoinDeltaHandler().exposeRewritePlan(bundle.topProject, ctx);
        Plan finalQuery = IvmDeltaRewriteHelper.INSTANCE.finalizeQuery(
                Pair.of(result.plan, ImmutableList.of()), result, ctx);
        InsertIntoTableCommand command = new IvmIncrRefreshManager().buildInsertCommand(
                (org.apache.doris.nereids.trees.plans.logical.LogicalPlan) finalQuery, ctx.getMtmv());
        UnboundTableSink<?> sink = getSink(command);
        Assertions.assertTrue(sink.getColNames().contains(Column.IVM_ROW_ID_COL));
        Assertions.assertFalse(sink.getColNames().contains(Column.DELETE_SIGN));
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
    }

    @Test
    void testNullSideSnapshotOnlyReplacesDeltaScanInsideNullSidePlan() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildScanForTable(2, "t2");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(103, "t103");
        LogicalProject<Plan> nullSide = normalizedInnerJoin(rowIdProject(rightDelta), rowIdProject(rightSnapshot));
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), nullSide);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.rewriteResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);
        LogicalUnion union = findOnlyUnion(result.plan);

        assertSnapshotBranch(union.child(1), false);
        assertSnapshotBranch(union.child(2), true);
    }

    @Test
    void testLeftDeepOuterJoinChainPropagatesRetainedSideDelta() {
        LogicalOlapScan leftDelta = buildScanForTable(1, "t1");
        LogicalOlapScan middleSnapshot = buildSnapshotScanForTable(2, "t2");
        LogicalOlapScan rightSnapshot = buildSnapshotScanForTable(3, "t3");
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(topJoin.topProject);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(topJoin.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(2, result.plan.collectToList(node ->
                node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType() == JoinType.LEFT_OUTER_JOIN).size());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        assertSingleFinalRowId((LogicalProject<?>) result.plan);
    }

    @Test
    void testLeftDeepOuterJoinChainRewritesTopNullSideDelta() {
        LogicalOlapScan leftSnapshot = buildSnapshotScanForTable(1, "t1");
        LogicalOlapScan middleSnapshot = buildSnapshotScanForTable(2, "t2");
        LogicalOlapScan rightDelta = buildScanForTable(3, "t3");
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmIncrRefreshContext ctx = newRefreshContext(topJoin.topProject);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(topJoin.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(2, result.plan.collectToList(node ->
                node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType() == JoinType.LEFT_OUTER_JOIN).size());
        List<LogicalUnion> unions = result.plan.collectToList(node -> node instanceof LogicalUnion);
        Assertions.assertTrue(unions.stream().anyMatch(union -> union.children().size() == 3),
                "Expected at least one 3-way union in plan: " + result.plan);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    private void assertSnapshotBranch(Plan branch, boolean postSnapshot) {
        Assertions.assertInstanceOf(LogicalProject.class, branch);
        LogicalProject<?> project = (LogicalProject<?>) branch;
        Assertions.assertInstanceOf(LogicalJoin.class, project.child());
        LogicalJoin<?, ?> antiJoin = (LogicalJoin<?, ?>) project.child();
        List<LogicalOlapScan> affectedKeyScans = antiJoin.left()
                .collectToList(node -> node instanceof LogicalOlapScan);
        Assertions.assertTrue(affectedKeyScans.stream().anyMatch(IvmDeltaRewriteHelper.INSTANCE::isIncrementalDeltaScan),
                "Affected-key side should still read delta rows: " + affectedKeyScans);

        List<LogicalOlapScan> scans = antiJoin.right().collectToList(node -> node instanceof LogicalOlapScan);
        if (postSnapshot) {
            Assertions.assertTrue(scans.stream().noneMatch(scan -> scan instanceof LogicalOlapTableStreamScan),
                    "Post-snapshot side should not contain stream scans: " + scans);
        }

        Assertions.assertFalse(scans.isEmpty(), "Missing null-side snapshot scan");
        for (LogicalOlapScan otherSnapshot : scans) {
            if (postSnapshot) {
                Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(otherSnapshot));
                Assertions.assertFalse(otherSnapshot instanceof LogicalOlapTableStreamScan);
            }
        }
    }

    private void assertSingleFinalRowId(LogicalProject<?> topProject) {
        long rowIdCount = topProject.getOutput().stream()
                .filter(slot -> Column.IVM_ROW_ID_COL.equals(slot.getName()))
                .count();
        Assertions.assertEquals(1, rowIdCount);
    }

    private void assertUnionChildrenAlign(LogicalUnion union) {
        int outputSize = union.getOutput().size();
        for (Plan child : union.children()) {
            Assertions.assertEquals(outputSize, child.getOutput().size());
            for (int i = 0; i < outputSize; i++) {
                Assertions.assertEquals(union.getOutput().get(i).getName(), child.getOutput().get(i).getName());
            }
        }
    }

    private void assertRightNullSideRowId(LogicalProject<?> project, int rightRowIdIndex, byte expectedDmlFactor) {
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias leftRowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertFalse(leftRowIdAlias.child() instanceof NullLiteral);

        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(rightRowIdIndex));
        Alias rightRowIdAlias = (Alias) project.getProjects().get(rightRowIdIndex);
        Assertions.assertInstanceOf(NullLiteral.class, rightRowIdAlias.child());

        // dml_factor is second-to-last, sequence is last
        int size = project.getProjects().size();
        NamedExpression dmlFactorProject = project.getProjects().get(size - 2);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactorProject.getName());
        if (dmlFactorProject instanceof Alias) {
            Expression dmlFactor = ((Alias) dmlFactorProject).child();
            Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
            Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());
        } else {
            Assertions.assertInstanceOf(SlotReference.class, dmlFactorProject);
        }

        NamedExpression sequenceProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.SEQUENCE_COL, sequenceProject.getName());
    }

    private void assertLeftNullSideRowId(LogicalProject<?> project, int rightRowIdIndex, byte expectedDmlFactor) {
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, project.getProjects().get(0).getName());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, project.getProjects().get(rightRowIdIndex).getName());

        // dml_factor is second-to-last, sequence is last
        int size = project.getProjects().size();
        NamedExpression dmlFactorProject = project.getProjects().get(size - 2);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactorProject.getName());
        if (dmlFactorProject instanceof Alias) {
            Expression dmlFactor = ((Alias) dmlFactorProject).child();
            Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
            Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());
        } else {
            Assertions.assertInstanceOf(SlotReference.class, dmlFactorProject);
        }

        NamedExpression sequenceProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.SEQUENCE_COL, sequenceProject.getName());
    }

    private void assertNullSideRepairEvent(LogicalProject<?> project, byte expectedDmlFactor) {
        int eventKeyCount = 1;
        // value slots are between event keys and dml_factor; skip dml_factor and sequence
        for (int i = eventKeyCount; i < project.getProjects().size() - 2; i++) {
            Assertions.assertInstanceOf(Alias.class, project.getProjects().get(i));
            Alias nullSideValueAlias = (Alias) project.getProjects().get(i);
            Assertions.assertInstanceOf(NullLiteral.class, nullSideValueAlias.child());
        }

        // dml_factor is second-to-last
        int size = project.getProjects().size();
        NamedExpression dmlFactorProject = project.getProjects().get(size - 2);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactorProject.getName());
        Assertions.assertInstanceOf(Alias.class, dmlFactorProject);
        Expression dmlFactor = ((Alias) dmlFactorProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());

        // sequence is last and comes from the delta-side sub-sequence.
        NamedExpression sequenceProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.SEQUENCE_COL, sequenceProject.getName());
    }

    private LogicalUnion nullSideEventUnion(Plan plan) {
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) plan).child();
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        return (LogicalUnion) eventJoin.right();
    }

    private void assertContainsSubQueryAlias(Plan plan, String alias) {
        boolean found = plan.collectToList(node -> node instanceof LogicalSubQueryAlias).stream()
                .map(node -> (LogicalSubQueryAlias<?>) node)
                .anyMatch(node -> alias.equals(node.getAlias()));
        Assertions.assertTrue(found, "Missing internal alias: " + alias);
    }

    private void assertUnionOutputsDoNotReuseChildExprIds(LogicalUnion union) {
        Set<Integer> childExprIds = new HashSet<>();
        for (Plan child : union.children()) {
            for (Slot slot : child.getOutput()) {
                childExprIds.add(slot.getExprId().asInt());
            }
        }
        for (Slot slot : union.getOutput()) {
            Assertions.assertFalse(childExprIds.contains(slot.getExprId().asInt()),
                    "Union output reuses a child ExprId: " + slot);
        }
    }

    private void assertUnionChildrenDoNotShareExprIds(LogicalUnion union) {
        Set<Integer> childExprIds = new HashSet<>();
        for (Plan child : union.children()) {
            for (Slot slot : child.getOutput()) {
                Assertions.assertTrue(childExprIds.add(slot.getExprId().asInt()),
                        "Different union children reuse the same ExprId: " + slot);
            }
        }
    }

    private void assertNoDuplicateScanRelationIds(Plan plan) {
        Set<Integer> relationIds = new HashSet<>();
        for (LogicalOlapScan scan : plan.<LogicalOlapScan>collectToList(node -> node instanceof LogicalOlapScan)) {
            Assertions.assertTrue(relationIds.add(scan.getRelationId().asInt()),
                    "Duplicate scan relation id: " + scan.getRelationId());
        }
    }

    private void assertProjectedJoinBranch(Plan branch, JoinType joinType) {
        Assertions.assertInstanceOf(LogicalProject.class, branch);
        LogicalProject<?> project = (LogicalProject<?>) branch;
        Assertions.assertInstanceOf(LogicalJoin.class, project.child());
        Assertions.assertEquals(joinType, ((LogicalJoin<?, ?>) project.child()).getJoinType());
    }

    private LogicalUnion findOnlyUnion(Plan plan) {
        List<LogicalUnion> unions = plan.collectToList(node -> node instanceof LogicalUnion);
        Assertions.assertEquals(1, unions.size(), "Expected exactly one union in plan: " + plan);
        return unions.get(0);
    }

    private LogicalProject<Plan> rowIdProject(LogicalOlapScan scan) {
        Alias rowIdAlias = new Alias(scan.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        projects.add(rowIdAlias);
        scan.getOutput().forEach(slot -> projects.add((NamedExpression) slot));
        return new LogicalProject<>(projects.build(), (Plan) scan);
    }

    private LogicalProject<Plan> normalizedInnerJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = innerJoin(left, right);
        return normalizedJoinProject(join);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = leftOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = rightOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedFullOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = fullOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithOnlyOtherHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithUniqueFunctionHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(new Add(firstUserSlot(left), new Random()), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmRewriteResult rewriteResult = deterministicRewriteResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), rewriteResult);
    }

    private LogicalProject<Plan> normalizedJoinProject(LogicalJoin<?, ?> join) {
        Slot leftRowId = IvmUtil.findRowIdSlot(join.left().getOutput(), "left child of join");
        Slot rightRowId = IvmUtil.findRowIdSlot(join.right().getOutput(), "right child of join");
        Alias rowIdAlias = new Alias(IvmUtil.buildRowIdHash(ImmutableList.of(leftRowId, rightRowId)),
                Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        projects.add(rowIdAlias);
        for (Slot slot : join.getOutput()) {
            if (!Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                projects.add(slot);
            }
        }
        return new LogicalProject<>(projects.build(), (Plan) join);
    }

    private IvmRewriteResult deterministicRewriteResult(Plan plan) {
        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        List<Plan> nodes = plan.collectToList(node -> node instanceof Plan);
        for (Plan node : nodes) {
            for (Slot slot : node.getOutput()) {
                if (Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                    rewriteResult.addRowId(slot, true);
                }
            }
        }
        return rewriteResult;
    }

    private LogicalJoin<?, ?> leftOuterJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> rightOuterJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> fullOuterJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.FULL_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private LogicalJoin<?, ?> innerJoin(Plan left, Plan right) {
        return new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
    }

    private Slot firstUserSlot(Plan plan) {
        return plan.getOutput().stream()
                .filter(slot -> !Column.IVM_ROW_ID_COL.equals(slot.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing user slot in plan: " + plan));
    }
}
