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
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class IvmSimpleScanDeltaStrategyTest extends IvmDeltaTestBase {

    private static final class TestableIvmSimpleScanDeltaStrategy extends IvmSimpleScanDeltaStrategy {
        TestableIvmSimpleScanDeltaStrategy(IvmDeltaRewriteContext ctx) {
            super(ctx);
        }

        private RewriteResult exposeRewritePlan(org.apache.doris.nereids.trees.plans.Plan plan) {
            return rewritePlan(plan);
        }

        private org.apache.doris.nereids.trees.plans.Plan exposeStripResultSink(
                org.apache.doris.nereids.trees.plans.Plan plan) {
            return stripResultSink(plan);
        }

        private Slot exposeFindSlotByName(List<Slot> slots, String name) {
            return findSlotByName(slots, name);
        }

        private Command exposeBuildInsertCommand(org.apache.doris.nereids.trees.plans.Plan plan) {
            return buildInsertCommandWithDeleteSign(plan);
        }
    }

    private static IvmDeltaRewriteContext dummyCtx() {
        return new IvmDeltaRewriteContext(mockMtmv(), new ConnectContext(), null);
    }

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        return mtmv;
    }

    @Test
    void testRewriteProducesInsertBundle() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmSimpleScanDeltaStrategy(ctx)
                .rewrite(buildScanPlan(scan)).get(0);
        UnboundTableSink<?> sink = getSink(command);
        Assertions.assertTrue(sink.getColNames().contains(Column.DELETE_SIGN));
    }

    @Test
    void testRewritePlanInjectsDmlFactorAtScan() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(scan);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        Assertions.assertEquals(scan.getOutput().size() + 1, result.plan.getOutput().size());
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertEquals(1,
                result.plan.getOutput().stream().filter(slot -> Column.IVM_DML_FACTOR_COL.equals(slot.getName())).count());
    }

    @Test
    void testVisitLogicalProjectAppendsDmlFactor() {
        LogicalOlapScan scan = buildScan();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(project);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL,
                result.plan.getOutput().get(result.plan.getOutput().size() - 1).getName());
    }

    @Test
    void testVisitLogicalProjectPreservesExistingDmlFactor() {
        LogicalOlapScan scan = buildScan();
        ImmutableList<NamedExpression> outputs = ImmutableList.<NamedExpression>builder()
                .addAll(scan.getOutput())
                .add(new Alias(new TinyIntLiteral((byte) 1), Column.IVM_DML_FACTOR_COL))
                .build();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(outputs, scan);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(project);
        Assertions.assertEquals(1,
                result.plan.getOutput().stream().filter(slot -> Column.IVM_DML_FACTOR_COL.equals(slot.getName())).count());
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testVisitLogicalFilterPropagatesDmlFactor() {
        LogicalOlapScan scan = buildScan();
        Expression predicate = new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0));
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(
                new LogicalFilter<>(com.google.common.collect.ImmutableSet.of(predicate), scan));
        Assertions.assertInstanceOf(LogicalFilter.class, result.plan);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testVisitUnsupportedPlanThrows() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan left = buildScan();
        LogicalOlapScan right = buildScan();
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN, ImmutableList.of(), left, right, JoinReorderContext.EMPTY);
        LogicalResultSink<?> plan = new LogicalResultSink<>(ImmutableList.copyOf(left.getOutput()), join);

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmSimpleScanDeltaStrategy(ctx).rewrite(plan));
    }

    @Test
    void testStripResultSinkReturnsInnerPlan() {
        LogicalOlapScan scan = buildScan();
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(ImmutableList.copyOf(scan.getOutput()), scan);
        LogicalResultSink<LogicalProject<LogicalOlapScan>> sink = new LogicalResultSink<>(
                ImmutableList.copyOf(scan.getOutput()), project);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        Assertions.assertSame(project, strategy.exposeStripResultSink(sink));
    }

    @Test
    void testFindSlotByNameReturnsMatchingSlot() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        Slot slot = strategy.exposeFindSlotByName(scan.getOutput(), scan.getOutput().get(0).getName());
        Assertions.assertEquals(scan.getOutput().get(0), slot);
    }

    @Test
    void testFindSlotByNameThrowsWhenMissing() {
        LogicalOlapScan scan = buildScan();
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeFindSlotByName(scan.getOutput(), "missing_slot"));
    }

    @Test
    void testBuildInsertCommandWithDeleteSignAddsDeleteSignColumn() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getInsertedColumnNames()).thenReturn(ImmutableList.of("id"));

        LogicalOlapScan scan = buildScan();
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(ctx);
        Command command = strategy.exposeBuildInsertCommand(
                new LogicalProject<>(ImmutableList.of((NamedExpression) scan.getOutput().get(0)), scan));

        UnboundTableSink<?> sink = getSink((InsertIntoTableCommand) command);
        Assertions.assertEquals(ImmutableList.of("id", Column.DELETE_SIGN), sink.getColNames());
    }

    @Test
    void testRewriteBuildsDeleteSignIfExpression() {
        MTMV mtmv = mockMtmv();
        LogicalOlapScan scan = buildScan();
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, new ConnectContext(), null);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmSimpleScanDeltaStrategy(ctx)
                .rewrite(buildScanPlan(scan))
                .get(0);
        UnboundTableSink<?> sink = getSink(command);
        LogicalProject<?> sinkProject = (LogicalProject<?>) sink.child();
        NamedExpression lastExpr = sinkProject.getProjects().get(sinkProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());
    }

    // ---- Tests for op-based dml_factor ----

    @Test
    void testRewritePlanWithoutOpColumnUsesLiteralOne() {
        LogicalOlapScan scan = buildScan(); // table without op column
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(scan);
        // The injected project should have scan columns + dml_factor
        LogicalProject<?> project = (LogicalProject<?>) result.plan;
        NamedExpression factorExpr = project.getProjects().get(project.getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, factorExpr.getName());
        Assertions.assertInstanceOf(Alias.class, factorExpr);
        // Should be literal TinyIntLiteral(1) — not an IF expression
        Assertions.assertInstanceOf(TinyIntLiteral.class, ((Alias) factorExpr).child());
        Assertions.assertEquals((byte) 1, ((TinyIntLiteral) ((Alias) factorExpr).child()).getValue());
    }

    @Test
    void testRewritePlanWithOpColumnUsesIfExpression() {
        LogicalOlapScan scan = buildScanWithOpColumn(); // table WITH op column
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(scan);
        LogicalProject<?> project = (LogicalProject<?>) result.plan;
        NamedExpression factorExpr = project.getProjects().get(project.getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, factorExpr.getName());
        Assertions.assertInstanceOf(Alias.class, factorExpr);
        // Should be IF(op = 0, 1, -1)
        Expression ifExpr = ((Alias) factorExpr).child();
        Assertions.assertInstanceOf(If.class, ifExpr);
        If ifFunc = (If) ifExpr;
        // Condition: EqualTo(opSlot, TinyIntLiteral(0))
        Assertions.assertInstanceOf(EqualTo.class, ifFunc.getArgument(0));
        // Then: TinyIntLiteral(1)
        Assertions.assertInstanceOf(TinyIntLiteral.class, ifFunc.getArgument(1));
        Assertions.assertEquals((byte) 1, ((TinyIntLiteral) ifFunc.getArgument(1)).getValue());
        // Else: TinyIntLiteral(-1)
        Assertions.assertInstanceOf(TinyIntLiteral.class, ifFunc.getArgument(2));
        Assertions.assertEquals((byte) -1, ((TinyIntLiteral) ifFunc.getArgument(2)).getValue());
    }

    @Test
    void testRewritePlanWithOpColumnDmlFactorSlotPropagates() {
        LogicalOlapScan scan = buildScanWithOpColumn();
        // Wrap in project + result sink (simulating normalized plan)
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalOlapScan> userProject = new LogicalProject<>(exprs, scan);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());

        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(userProject);
        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        // The outer project should propagate dml_factor from the scan-level project
        Assertions.assertTrue(
                result.plan.getOutput().stream().anyMatch(s -> Column.IVM_DML_FACTOR_COL.equals(s.getName())));
    }

    // ---- Join tests ----

    @Test
    void testJoinDmlFactorPropagationLeft() {
        LogicalOlapScan scanDelta = buildScan(); // isDelta=true
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2"); // isDelta=false
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from delta (left) side");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testJoinDmlFactorPropagationRight() {
        LogicalOlapScan scanSnapshot = buildScanForTable(1, "t1"); // isDelta=false
        LogicalOlapScan scanDelta = buildScan(); // isDelta=true
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanSnapshot, scanDelta, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from delta (right) side");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    @Test
    void testJoinCrossJoinDmlFactor() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.CROSS_JOIN,
                scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNotNull(result.dmlFactorSlot);
        // With null normalizeResult, conservative default adds a non-det guard (Project wrapping Join)
        Assertions.assertInstanceOf(LogicalProject.class, result.plan);
    }

    @Test
    void testJoinBothDeltaThrows() {
        LogicalOlapScan scanA = buildScan(); // isDelta=true
        LogicalOlapScan scanB = buildScan(); // isDelta=true
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeRewritePlan(join),
                "Both sides having dml_factor should throw");
    }

    @Test
    void testJoinNeitherDeltaReturnsNullDmlFactor() {
        LogicalOlapScan scanA = buildScanForTable(1, "t1"); // isDelta=false
        LogicalOlapScan scanB = buildScanForTable(2, "t2"); // isDelta=false
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNull(result.dmlFactorSlot,
                "Neither side having dml_factor should return null dml_factor");
    }

    @Test
    void testJoinUnsupportedOuterJoinThrows() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeRewritePlan(join));
    }

    @Test
    void testJoinNonDetGuardAdded() {
        // Build a join where the snapshot side has a normalized row_id slot (non-deterministic)
        LogicalOlapScan scanDelta = buildScan(); // isDelta=true
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2"); // isDelta=false

        // Simulate normalization: wrap snapshot in a project with row_id slot
        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0); // the row_id slot

        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.addRowId(rowIdSlot, false); // non-deterministic

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmDeltaRewriteContext rewriteCtx = new IvmDeltaRewriteContext(mockMtmv(), new ConnectContext(),
                normalizeResult);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(rewriteCtx);
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        // The result should have assert_true guard wrapping dml_factor
        Assertions.assertNotNull(result.dmlFactorSlot);
        Assertions.assertInstanceOf(LogicalProject.class, result.plan,
                "Non-det guard should wrap with a Project containing assert_true");
        String planString = result.plan.toString();
        Assertions.assertTrue(planString.contains("assert_true") || planString.contains("AssertTrue"),
                "Non-deterministic row_id should add assert_true guard, plan: " + planString);
    }

    @Test
    void testJoinDetNoGuard() {
        LogicalOlapScan scanDelta = buildScan(); // isDelta=true
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2"); // isDelta=false

        // Simulate normalization: wrap snapshot in a project with row_id slot
        Alias rowIdAlias = new Alias(scanSnapshot.getOutput().get(0), Column.IVM_ROW_ID_COL);
        ImmutableList.Builder<NamedExpression> snapshotOutputs = ImmutableList.builder();
        snapshotOutputs.add(rowIdAlias);
        scanSnapshot.getOutput().forEach(s -> snapshotOutputs.add((NamedExpression) s));
        LogicalProject<?> normalizedSnapshot = new LogicalProject<>(snapshotOutputs.build(), scanSnapshot);
        Slot rowIdSlot = normalizedSnapshot.getOutput().get(0);

        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.addRowId(rowIdSlot, true); // deterministic

        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanDelta, normalizedSnapshot, JoinReorderContext.EMPTY);

        IvmDeltaRewriteContext rewriteCtx = new IvmDeltaRewriteContext(mockMtmv(), new ConnectContext(),
                normalizeResult);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(rewriteCtx);
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNotNull(result.dmlFactorSlot);
        // Join result should NOT have an extra guard project
        Assertions.assertInstanceOf(LogicalJoin.class, result.plan,
                "Deterministic row_id should not add guard project");
    }

    @Test
    void testJoinMarkJoinThrows() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        // Construct a proper mark join: set markJoinSlotReference so isMarkJoin() returns true
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.of(new MarkJoinSlotReference("$mark")),
                scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeRewritePlan(join),
                "Mark join conjuncts should throw AnalysisException");
    }

    @Test
    void testJoinGuardFallbackMessage() {
        LogicalOlapScan scanDelta = buildScan();
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

        IvmDeltaRewriteContext rewriteCtx = new IvmDeltaRewriteContext(mockMtmv(), new ConnectContext(),
                normalizeResult);
        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(rewriteCtx);
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        String planString = result.plan.toString();
        Assertions.assertTrue(planString.contains("IVM fallback: delete on non-deterministic row_id in INNER_JOIN"),
                "Guard should contain fallback message, plan: " + planString);
    }

    @Test
    void testJoinDmlFactorWithHashConjuncts() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        EqualTo condition = new EqualTo(scanDelta.getOutput().get(0), scanSnapshot.getOutput().get(0));
        LogicalJoin<?, ?> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(condition), scanDelta, scanSnapshot, JoinReorderContext.EMPTY);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(join);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from delta side with hash conjuncts");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
    }

    // ---- UNION ALL strategy tests ----

    private LogicalUnion buildUnionAll(Plan... children) {
        List<Slot> firstOutput = children[0].getOutput();
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (Slot slot : firstOutput) {
            outputs.add(new SlotReference(
                    StatementScopeIdGenerator.newExprId(),
                    slot.getName(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : children) {
            ImmutableList.Builder<SlotReference> mapping = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                mapping.add((SlotReference) slot);
            }
            childrenOutputs.add(mapping.build());
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, ImmutableList.copyOf(children));
    }

    @Test
    void testUnionDeltaInFirstArm() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalUnion union = buildUnionAll(scanDelta, scanSnapshot);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(union);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from delta arm");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan,
                "Non-delta arms should be eliminated, leaving a column-mapping Project");
    }

    @Test
    void testUnionDeltaInSecondArm() {
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalOlapScan scanDelta = buildScan();
        LogicalUnion union = buildUnionAll(scanSnapshot, scanDelta);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(union);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from delta arm");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan,
                "Non-delta arms should be eliminated, leaving a column-mapping Project");
    }

    @Test
    void testUnionAllSnapshotReturnsNullDmlFactor() {
        LogicalOlapScan scanA = buildScanForTable(2, "t2");
        LogicalOlapScan scanB = buildScanForTable(3, "t3");
        LogicalUnion union = buildUnionAll(scanA, scanB);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(union);

        Assertions.assertNull(result.dmlFactorSlot,
                "All-snapshot union should have null dmlFactorSlot");
        Assertions.assertInstanceOf(LogicalUnion.class, result.plan,
                "All-snapshot union should rebuild as LogicalUnion");
    }

    @Test
    void testUnionBothDeltaThrows() {
        LogicalOlapScan scanA = buildScan();
        LogicalOlapScan scanB = (LogicalOlapScan) buildScanForTable(2, "t2").withIsDelta(true);
        LogicalUnion union = buildUnionAll(scanA, scanB);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        Assertions.assertThrows(AnalysisException.class,
                () -> strategy.exposeRewritePlan(union),
                "Multiple delta arms in union should throw AnalysisException");
    }

    @Test
    void testUnionThreeWayDeltaInMiddle() {
        LogicalOlapScan scanA = buildScanForTable(2, "t2");
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanC = buildScanForTable(3, "t3");
        LogicalUnion union = buildUnionAll(scanA, scanDelta, scanC);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(union);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from the middle delta arm");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan,
                "Non-delta arms should be eliminated, leaving a column-mapping Project");
    }

    @Test
    void testUnionOutputMappingPreservesExprIds() {
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanSnapshot = buildScanForTable(2, "t2");
        LogicalUnion union = buildUnionAll(scanDelta, scanSnapshot);

        List<ExprId> unionExprIds = union.getOutputs().stream()
                .map(NamedExpression::getExprId)
                .collect(ImmutableList.toImmutableList());

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(union);

        LogicalProject<?> project = (LogicalProject<?>) result.plan;
        List<Slot> projectOutput = project.getOutput();
        for (int i = 0; i < unionExprIds.size(); i++) {
            Assertions.assertEquals(unionExprIds.get(i), projectOutput.get(i).getExprId(),
                    "Output slot " + i + " should preserve union's ExprId");
        }
    }

    @Test
    void testNestedUnionDeltaInInnerArm() {
        // (a_delta UNION ALL b_snapshot) UNION ALL c_snapshot
        // Delta is in the inner union's first arm.
        // Strategy should: inner union → eliminate b, return a as delta;
        // outer union → eliminate c, return mapped project with dml_factor.
        LogicalOlapScan scanDelta = buildScan();
        LogicalOlapScan scanB = buildScanForTable(2, "t2");
        LogicalOlapScan scanC = buildScanForTable(3, "t3");

        LogicalUnion innerUnion = buildUnionAll(scanDelta, scanB);
        LogicalUnion outerUnion = buildUnionAll(innerUnion, scanC);

        TestableIvmSimpleScanDeltaStrategy strategy = new TestableIvmSimpleScanDeltaStrategy(dummyCtx());
        IvmSimpleScanDeltaStrategy.RewriteResult result = strategy.exposeRewritePlan(outerUnion);

        Assertions.assertNotNull(result.dmlFactorSlot,
                "dml_factor should propagate from the nested delta arm");
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, result.dmlFactorSlot.getName());
        Assertions.assertInstanceOf(LogicalProject.class, result.plan,
                "Non-delta arms should be eliminated, leaving a column-mapping Project");

        // Verify output preserves outer union's ExprIds
        LogicalProject<?> project = (LogicalProject<?>) result.plan;
        List<NamedExpression> outerOutputs = outerUnion.getOutputs();
        List<Slot> projectOutput = project.getOutput();
        for (int i = 0; i < outerOutputs.size(); i++) {
            Assertions.assertEquals(outerOutputs.get(i).getExprId(), projectOutput.get(i).getExprId(),
                    "Nested union output slot " + i + " should preserve outer union's ExprId");
        }
    }
}
