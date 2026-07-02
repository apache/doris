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
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class IvmJoinDeltaHandlerTest extends IvmDeltaTestBase {

    private static final class TestableIvmJoinDeltaHandler {
        private final IvmDeltaRewriteVisitor visitor;

        private TestableIvmJoinDeltaHandler() {
            visitor = new IvmDeltaRewriteVisitor();
        }

        private IvmDeltaRewriteResult exposeRewritePlan(Plan plan, IvmRefreshContext ctx) {
            return visitor.rewritePlan(plan, ctx);
        }
    }

    private IvmRefreshContext newRefreshContext(Plan plan) {
        return new IvmRefreshContext(buildMtmvFromPlan(plan.getOutput()), new ConnectContext(), null);
    }

    private IvmRefreshContext newRefreshContext(Plan plan, IvmNormalizeResult normalizeResult) {
        return new IvmRefreshContext(buildMtmvFromPlan(plan.getOutput()), new ConnectContext(), normalizeResult);
    }

    @Test
    void testInnerJoinDmlFactorPropagationLeft() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testInnerJoinDmlFactorPropagationRight() {
        LogicalOlapScan scanSnapshot = buildScanForTable(1, "t1");
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanSnapshot, scanDelta, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testCrossJoinDmlFactorPropagation() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.CROSS_JOIN,
                scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
    }

    @Test
    void testInnerJoinBothDeltaThrows() {
        LogicalOlapTableStreamScan scanA = buildDeltaScan();
        LogicalOlapTableStreamScan scanB = buildDeltaScan();
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        Assertions.assertThrows(AnalysisException.class, () -> handler.exposeRewritePlan(join, newRefreshContext(join)));
    }

    @Test
    void testInnerJoinWithoutDeltaReturnsNullDmlFactor() {
        LogicalOlapScan scanA = buildScanForTable(1, "t1");
        LogicalOlapScan scanB = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, newRefreshContext(join));

        Assertions.assertNull(result.dmlFactorSlot);
    }

    @Test
    void testInnerJoinNonDetGuardAdded() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.addRowId(rowIdSlot, false);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmRefreshContext rewriteCtx = newRefreshContext(join, normalizeResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        String planString = result.plan.toString();
        Assertions.assertTrue(planString.contains("assert_true") || planString.contains("AssertTrue"),
                "Non-deterministic row_id should add assert_true guard, plan: " + planString);
    }

    @Test
    void testInnerJoinDeterministicRowIdSkipsGuard() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.addRowId(rowIdSlot, true);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmRefreshContext rewriteCtx = newRefreshContext(join, normalizeResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertInstanceOf(LogicalJoin.class, result.plan);
    }

    @Test
    void testInnerJoinGuardFallbackMessage() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");

        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.addRowId(rowIdSlot, false);

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmRefreshContext rewriteCtx = newRefreshContext(join, normalizeResult);
        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmDeltaRewriteResult result = handler.exposeRewritePlan(join, rewriteCtx);

        Assertions.assertTrue(result.plan.toString().contains(
                "IVM fallback: delete on non-deterministic row_id in INNER_JOIN"));
    }

    @Test
    void testInnerJoinDmlFactorWithHashConjuncts() {
        LogicalOlapTableStreamScan scanDelta = buildDeltaScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
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
        private final IvmNormalizeResult normalizeResult;

        private NormalizedOuterJoinPlan(LogicalProject<Plan> topProject, int leftOutputSize,
                IvmNormalizeResult normalizeResult) {
            this.topProject = topProject;
            this.leftOutputSize = leftOutputSize;
            this.normalizeResult = normalizeResult;
        }
    }

    @Test
    void testRetainedSideDeltaUsesLeftOuterJoinUnderTopProject() {
        LogicalOlapScan leftDelta = buildDeltaScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        Assertions.assertInstanceOf(LogicalJoin.class, topProject.child());
        Assertions.assertEquals(JoinType.LEFT_OUTER_JOIN, ((LogicalJoin<?, ?>) topProject.child()).getJoinType());
        assertSingleFinalRowId(topProject);
    }

    @Test
    void testNullSideDeltaBuildsRightEventsAndProbesLeftOnce() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        assertSingleFinalRowId(topProject);
        Assertions.assertInstanceOf(LogicalProject.class, topProject.child());
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) topProject.child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        LogicalUnion union = (LogicalUnion) eventJoin.right();
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
        LogicalOlapScan leftDelta = buildDeltaScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoin(rowIdProject(leftDelta),
                rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> topProject = (LogicalProject<?>) result.plan;
        LogicalProject<?> joinOutputProject = (LogicalProject<?>) topProject.child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        LogicalUnion union = (LogicalUnion) eventJoin.right();
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertNullSideRepairEvent((LogicalProject<?>) union.child(1), (byte) -1);
        assertNullSideRepairEvent((LogicalProject<?>) union.child(2), (byte) 1);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testFullOuterJoinLeftDeltaUsesLeftOuterJoinAndRepairsRightDanglingRows() {
        LogicalOlapScan leftDelta = buildDeltaScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedFullOuterJoin(rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        assertProjectedJoinBranch(union.child(0), JoinType.LEFT_OUTER_JOIN);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) -1);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(2), bundle.leftOutputSize, (byte) 1);
        assertUnionChildrenAlign(union);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    @Test
    void testFullOuterJoinRightDeltaUsesRightOuterJoinAndRepairsLeftDanglingRows() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedFullOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

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
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithOnlyOtherHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalJoin.class, joinOutputProject.child());
        LogicalJoin<?, ?> eventJoin = (LogicalJoin<?, ?>) joinOutputProject.child();
        Assertions.assertEquals(JoinType.INNER_JOIN, eventJoin.getJoinType());
        Assertions.assertInstanceOf(LogicalUnion.class, eventJoin.right());
        Assertions.assertEquals(3, ((LogicalUnion) eventJoin.right()).children().size());
    }

    @Test
    void testNullSideDeltaWithNonHashOtherConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

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
        LogicalOlapScan leftDelta = buildDeltaScanForTable(1, "t1");
        LogicalOlapScan rightSnapshot = buildScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedRightOuterJoinWithNonHashOtherConjunct(
                rowIdProject(leftDelta), rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        LogicalUnion union = (LogicalUnion) joinOutputProject.child();
        Assertions.assertEquals(3, union.children().size());
        assertProjectedJoinBranch(union.child(0), JoinType.INNER_JOIN);
        assertUnionChildrenAlign(union);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(1), bundle.leftOutputSize, (byte) -1);
        assertLeftNullSideRowId((LogicalProject<?>) union.child(2), bundle.leftOutputSize, (byte) 1);
    }

    @Test
    void testNullSideDeltaWithUniqueFunctionHashConjunctFallsBackToRepairBranches() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoinWithUniqueFunctionHashConjunct(
                rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);

        LogicalProject<?> joinOutputProject = (LogicalProject<?>) ((LogicalProject<?>) result.plan).child();
        Assertions.assertInstanceOf(LogicalUnion.class, joinOutputProject.child());
        Assertions.assertEquals(3, ((LogicalUnion) joinOutputProject.child()).children().size());
    }

    @Test
    void testRewriteBuildsSinkWithFinalRowIdAndDeleteSign() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(rightDelta));

        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        List<Command> commands = IvmDeltaCommandBuilder.INSTANCE.rewrite(bundle.topProject, ctx);

        Assertions.assertEquals(1, commands.size());
        UnboundTableSink<?> sink = getSink((InsertIntoTableCommand) commands.get(0));
        Assertions.assertTrue(sink.getColNames().contains(Column.IVM_ROW_ID_COL));
        Assertions.assertEquals(Column.DELETE_SIGN, sink.getColNames().get(sink.getColNames().size() - 1));
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());
    }

    @Test
    void testNullSideSnapshotOnlyReplacesDeltaScanInsideNullSidePlan() {
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan rightDelta = buildDeltaScanForTable(2, "t2");
        LogicalOlapScan rightSnapshot = buildScanForTable(3, "t3").withTso(77);
        LogicalProject<Plan> nullSide = normalizedInnerJoin(rowIdProject(rightDelta), rowIdProject(rightSnapshot));
        NormalizedOuterJoinPlan bundle = normalizedOuterJoin(rowIdProject(leftSnapshot), nullSide);

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(bundle.topProject, bundle.normalizeResult);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(bundle.topProject, ctx);
        LogicalUnion union = nullSideEventUnion(result.plan);

        // TSO values are placeholders (BE does not support TSO snapshot reads yet).
        // Delta snapshot scans get TSO=-1 (default) until stream TSO integration is complete.
        assertSnapshotBranch(union.child(1), -1);
        assertSnapshotBranch(union.child(2), -1);
    }

    @Test
    void testLeftDeepOuterJoinChainPropagatesRetainedSideDelta() {
        LogicalOlapScan leftDelta = buildDeltaScanForTable(1, "t1");
        LogicalOlapScan middleSnapshot = buildScanForTable(2, "t2");
        LogicalOlapScan rightSnapshot = buildScanForTable(3, "t3");
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftDelta), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightSnapshot));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(topJoin.topProject);

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
        LogicalOlapScan leftSnapshot = buildScanForTable(1, "t1");
        LogicalOlapScan middleSnapshot = buildScanForTable(2, "t2");
        LogicalOlapTableStreamScan rightDelta = buildDeltaScanForTable(3, "t3");
        NormalizedOuterJoinPlan firstJoin = normalizedOuterJoin(rowIdProject(leftSnapshot), rowIdProject(middleSnapshot));
        NormalizedOuterJoinPlan topJoin = normalizedOuterJoin(firstJoin.topProject, rowIdProject(rightDelta));

        TestableIvmJoinDeltaHandler handler = new TestableIvmJoinDeltaHandler();
        IvmRefreshContext ctx = newRefreshContext(topJoin.topProject);

        IvmDeltaRewriteResult result = handler.exposeRewritePlan(topJoin.topProject, ctx);

        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(1, result.plan.collectToList(node ->
                node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getJoinType() == JoinType.LEFT_OUTER_JOIN).size());
        LogicalUnion union = nullSideEventUnion(result.plan);
        Assertions.assertEquals(3, union.children().size());
        assertUnionChildrenAlign(union);
        assertNoDuplicateScanRelationIds(result.plan);
    }

    private void assertSnapshotBranch(Plan branch, long expectedDeltaSnapshotTso) {
        List<LogicalOlapScan> scans = branch.collectToList(node -> node instanceof LogicalOlapScan);
        LogicalOlapScan deltaSnapshot = scans.stream()
                .filter(scan -> scan.getTable().getId() == 2
                        && !IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(scan))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing copied null-side delta scan"));
        Assertions.assertEquals(expectedDeltaSnapshotTso, deltaSnapshot.getTso());

        LogicalOlapScan otherSnapshot = scans.stream()
                .filter(scan -> scan.getTable().getId() == 3)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing existing null-side snapshot scan"));
        Assertions.assertFalse(IvmDeltaRewriteHelper.INSTANCE.isIncrementalDeltaScan(otherSnapshot));
        Assertions.assertEquals(77, otherSnapshot.getTso());
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

        // dml_factor is second-to-last, baseOp is last
        int size = project.getProjects().size();
        NamedExpression dmlFactorProject = project.getProjects().get(size - 2);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactorProject.getName());
        Assertions.assertInstanceOf(Alias.class, dmlFactorProject);
        Expression dmlFactor = ((Alias) dmlFactorProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());

        NamedExpression baseOpProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.IVM_BASE_OP_COL, baseOpProject.getName());
    }

    private void assertLeftNullSideRowId(LogicalProject<?> project, int rightRowIdIndex, byte expectedDmlFactor) {
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias leftRowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertInstanceOf(NullLiteral.class, leftRowIdAlias.child());

        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(rightRowIdIndex));
        Alias rightRowIdAlias = (Alias) project.getProjects().get(rightRowIdIndex);
        Assertions.assertFalse(rightRowIdAlias.child() instanceof NullLiteral);

        // dml_factor is second-to-last, baseOp is last
        int size = project.getProjects().size();
        NamedExpression dmlFactorProject = project.getProjects().get(size - 2);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactorProject.getName());
        Assertions.assertInstanceOf(Alias.class, dmlFactorProject);
        Expression dmlFactor = ((Alias) dmlFactorProject).child();
        Assertions.assertInstanceOf(TinyIntLiteral.class, dmlFactor);
        Assertions.assertEquals(expectedDmlFactor, ((TinyIntLiteral) dmlFactor).getValue());

        NamedExpression baseOpProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.IVM_BASE_OP_COL, baseOpProject.getName());
    }

    private void assertNullSideRepairEvent(LogicalProject<?> project, byte expectedDmlFactor) {
        int eventKeyCount = 1;
        // value slots are between event keys and dml_factor; skip dml_factor and baseOp
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

        // baseOp is last, always +1 for repair events
        NamedExpression baseOpProject = project.getProjects().get(size - 1);
        Assertions.assertEquals(Column.IVM_BASE_OP_COL, baseOpProject.getName());
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
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = rightOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedFullOuterJoin(Plan left, Plan right) {
        LogicalJoin<?, ?> join = fullOuterJoin(left, right);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithOnlyOtherHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedRightOuterJoinWithNonHashOtherConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(firstUserSlot(left), firstUserSlot(right))),
                ImmutableList.of(new GreaterThan(firstUserSlot(left), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
    }

    private NormalizedOuterJoinPlan normalizedOuterJoinWithUniqueFunctionHashConjunct(Plan left, Plan right) {
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(new Add(firstUserSlot(left), new Random()), firstUserSlot(right))),
                left, right, JoinReorderContext.EMPTY);
        LogicalProject<Plan> topProject = normalizedJoinProject(join);
        IvmNormalizeResult normalizeResult = deterministicNormalizeResult(topProject);
        return new NormalizedOuterJoinPlan(topProject, left.getOutput().size(), normalizeResult);
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

    private IvmNormalizeResult deterministicNormalizeResult(Plan plan) {
        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        List<Plan> nodes = plan.collectToList(node -> node instanceof Plan);
        for (Plan node : nodes) {
            for (Slot slot : node.getOutput()) {
                if (Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                    normalizeResult.addRowId(slot, true);
                }
            }
        }
        return normalizeResult;
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
