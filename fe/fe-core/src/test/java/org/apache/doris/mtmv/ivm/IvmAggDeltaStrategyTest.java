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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class IvmAggDeltaStrategyTest extends IvmDeltaTestBase {

    private AggRewriteResult rewriteAgg(LogicalAggregate<LogicalOlapScan> agg) {
        PlanBundle bundle = normalizeAggPlan(agg);
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, bundle.connectContext, bundle.normalizeResult);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmAggDeltaStrategy(ctx)
                .rewrite(bundle.normalizedPlan).get(0).getCommand();
        UnboundTableSink<?> sink = getSink(command);
        return new AggRewriteResult(bundle, mtmv, sink, (LogicalProject<?>) sink.child());
    }

    private LogicalJoin<?, ?> getJoin(AggRewriteResult result) {
        if (result.finalProject.child() instanceof LogicalFilter) {
            return (LogicalJoin<?, ?>) ((LogicalFilter<?>) result.finalProject.child()).child();
        }
        return (LogicalJoin<?, ?>) result.finalProject.child();
    }

    private LogicalProject<?> getDeltaTopProject(AggRewriteResult result) {
        return (LogicalProject<?>) getJoin(result).right();
    }

    @Test
    void testGroupedAggUsesRightOuterJoinAndDeleteSignSink() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));
        LogicalJoin<?, ?> join = getJoin(result);

        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, join.getJoinType());
        Assertions.assertInstanceOf(LogicalProject.class, join.right());
        Assertions.assertEquals(result.mtmv.getInsertedColumnNames().size() + 1, result.sink.getColNames().size());
        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
        List<String> outputNames = result.finalProject.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
        Assertions.assertEquals(result.mtmv.getInsertedColumnNames(),
                outputNames.subList(0, result.mtmv.getInsertedColumnNames().size()));
    }

    @Test
    void testScalarAggSkipsNetZeroFilterButKeepsDeleteSignSink() {
        AggRewriteResult result = rewriteAgg(buildScalarAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalJoin.class, result.finalProject.child());
        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
    }

    @Test
    void testAggWithMaxProducesValidPlan() {
        AggRewriteResult result = rewriteAgg(buildMaxAgg(buildScan()));
        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
    }

    @Test
    void testAggWithMinProducesValidPlan() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));
        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
    }

    @Test
    void testMaxDeltaSubPlanHasDeleteExtremeSlot() {
        AggRewriteResult result = rewriteAgg(buildMaxAgg(buildScan()));
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();
        // Expected: [group_key, delta_group_count, delta_max, delta_del_max, delta_count]
        List<String> outputNames = deltaAgg.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
        Assertions.assertTrue(outputNames.stream().anyMatch(n -> n.contains("DELMAX") || n.contains("transient")));
        Assertions.assertTrue(outputNames.stream().anyMatch(n -> n.contains("MAX")));
    }

    @Test
    void testMinDeltaSubPlanHasDeleteExtremeSlot() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();
        List<String> outputNames = deltaAgg.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
        Assertions.assertTrue(outputNames.stream().anyMatch(n -> n.contains("DELMIN") || n.contains("transient")));
        Assertions.assertTrue(outputNames.stream().anyMatch(n -> n.contains("MIN")));
    }

    @Test
    void testMaxApplyPlanContainsAssertTrueGuard() {
        AggRewriteResult result = rewriteAgg(buildMaxAgg(buildScan()));
        // The final project should contain an AssertTrue expression nested in an If for the guard
        boolean hasAssertTrue = result.finalProject.getProjects().stream()
                .anyMatch(expr -> containsAssertTrue(expr));
        Assertions.assertTrue(hasAssertTrue, "Expected assert_true guard in MAX apply plan");
    }

    @Test
    void testMinApplyPlanContainsAssertTrueGuard() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));
        boolean hasAssertTrue = result.finalProject.getProjects().stream()
                .anyMatch(expr -> containsAssertTrue(expr));
        Assertions.assertTrue(hasAssertTrue, "Expected assert_true guard in MIN apply plan");
    }

    private boolean containsAssertTrue(org.apache.doris.nereids.trees.expressions.Expression expr) {
        if (expr instanceof AssertTrue) {
            return true;
        }
        for (org.apache.doris.nereids.trees.expressions.Expression child : expr.children()) {
            if (containsAssertTrue(child)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void testGroupedAggDeleteSignIsIfExpression() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        NamedExpression lastExpr = result.finalProject.getProjects().get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());
    }

    @Test
    void testScalarAggDeleteSignIsConstantZero() {
        AggRewriteResult result = rewriteAgg(buildScalarAgg(buildScan()));

        NamedExpression lastExpr = result.finalProject.getProjects().get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(TinyIntLiteral.class, ((Alias) lastExpr).child());
    }

    @Test
    void testGroupedAggDeltaSubPlanHasCorrectAggregateOutputCount() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();
        Assertions.assertEquals(6, deltaAgg.getOutputExpressions().size());
    }

    @Test
    void testGroupedAggJoinConditionUsesRowId() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));
        LogicalJoin<?, ?> join = getJoin(result);

        Assertions.assertFalse(join.getHashJoinConjuncts().isEmpty());
        Expression joinCondition = join.getHashJoinConjuncts().get(0);
        Assertions.assertInstanceOf(EqualTo.class, joinCondition);
        Assertions.assertTrue(joinCondition.toSql().contains(Column.IVM_ROW_ID_COL));
    }

    @Test
    void testCountOnlyAggProducesValidPlan() {
        LogicalOlapScan scan = buildScan();
        Slot idSlot = scan.getOutput().get(0);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<LogicalOlapScan> agg = new LogicalAggregate<>(
                ImmutableList.of(idSlot), ImmutableList.of(idSlot, countAlias),
                true, java.util.Optional.empty(), scan);

        AggRewriteResult result = rewriteAgg(agg);
        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
    }

    @Test
    void testCountExprAggProducesHiddenCountState() {
        AggRewriteResult result = rewriteAgg(buildCountExprAgg(buildScan()));
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();

        Assertions.assertEquals(3, deltaAgg.getOutputExpressions().size());
        List<String> outputNames = deltaAgg.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
        Assertions.assertTrue(outputNames.contains(Column.IVM_DELTA_GROUP_COUNT_COL));
        Assertions.assertTrue(outputNames.stream().anyMatch(name -> name.contains("COUNT")));
    }

    @Test
    void testGroupedAggOutputColumnsMatchMtmvInsertedPlusDeleteSign() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        List<String> expectedSinkCols = new ArrayList<>(result.mtmv.getInsertedColumnNames());
        expectedSinkCols.add(Column.DELETE_SIGN);
        Assertions.assertEquals(expectedSinkCols, result.sink.getColNames());
    }

    @Test
    void testAggMissingNormalizeResultThrows() {
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(buildScan()));
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());

        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv, bundle.connectContext, null);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new IvmAggDeltaStrategy(ctx).rewrite(bundle.normalizedPlan));
        Assertions.assertTrue(ex.getMessage().contains("normalize result"));
    }

    @Test
    void testNetZeroFilterPredicateStructure() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalFilter.class, result.finalProject.child());
        LogicalFilter<?> filter = (LogicalFilter<?>) result.finalProject.child();
        Assertions.assertEquals(1, filter.getConjuncts().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Not.class,
                filter.getConjuncts().iterator().next());
    }

    @Test
    void testDeltaTopProjectCoalescesNullableAggOutputs() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));
        LogicalProject<?> deltaTopProject = getDeltaTopProject(result);

        Assertions.assertTrue(deltaTopProject.getProjects().stream()
                .filter(Alias.class::isInstance)
                .map(Alias.class::cast)
                .anyMatch(alias -> alias.child() instanceof Coalesce));
    }

    // ---- B1: Scalar MIN / MAX plan structure ----

    @Test
    void testScalarMinAggSkipsNetZeroFilterAndHasGuard() {
        AggRewriteResult result = rewriteAgg(buildScalarMinAgg(buildScan()));

        // Scalar agg: no net-zero filter — join is direct child of finalProject
        Assertions.assertInstanceOf(LogicalJoin.class, result.finalProject.child(),
                "Scalar MIN should have no net-zero filter");
        // Delete sign must be constant 0 for scalar
        NamedExpression lastExpr = result.finalProject.getProjects()
                .get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(TinyIntLiteral.class, ((Alias) lastExpr).child());
        // Must contain assert_true guard for MIN boundary protection
        boolean hasAssertTrue = result.finalProject.getProjects().stream()
                .anyMatch(this::containsAssertTrue);
        Assertions.assertTrue(hasAssertTrue, "Scalar MIN should have assert_true guard");
    }

    @Test
    void testScalarMaxAggSkipsNetZeroFilterAndHasGuard() {
        AggRewriteResult result = rewriteAgg(buildScalarMaxAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalJoin.class, result.finalProject.child(),
                "Scalar MAX should have no net-zero filter");
        NamedExpression lastExpr = result.finalProject.getProjects()
                .get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(TinyIntLiteral.class, ((Alias) lastExpr).child());
        boolean hasAssertTrue = result.finalProject.getProjects().stream()
                .anyMatch(this::containsAssertTrue);
        Assertions.assertTrue(hasAssertTrue, "Scalar MAX should have assert_true guard");
    }

    // ---- B2: Combined MIN+MAX in single MV ----

    @Test
    void testCombinedMinMaxAggHasTwoAssertTrueGuards() {
        AggRewriteResult result = rewriteAgg(buildMinMaxAgg(buildScan()));

        // Count distinct assert_true-containing project expressions
        long guardCount = result.finalProject.getProjects().stream()
                .filter(this::containsAssertTrue).count();
        // MIN guard + MAX guard = at least 2 expressions containing assert_true
        // (visible value expressions also reference the guarded hidden state, so count >= 2)
        Assertions.assertTrue(guardCount >= 2,
                "Expected at least 2 assert_true guards (MIN + MAX), got " + guardCount);

        // Net-zero filter should be present (grouped agg)
        Assertions.assertInstanceOf(LogicalFilter.class, result.finalProject.child(),
                "Combined MIN+MAX grouped agg should have net-zero filter");
    }

    // ---- B3: Assert_true predicate semantics ----

    @Test
    void testAssertTrueGuardPredicateIsCompoundOrExpression() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));

        // Find the boundary guard AssertTrue — the one whose condition is an Or expression.
        // (assertNonNegative also uses AssertTrue but with GreaterThanEqual; skip those)
        List<AssertTrue> allAssertTrue = new ArrayList<>();
        for (NamedExpression expr : result.finalProject.getProjects()) {
            collectAssertTrue(expr, allAssertTrue);
        }
        Assertions.assertFalse(allAssertTrue.isEmpty(), "Should find AssertTrue expressions in final project");

        AssertTrue boundaryGuard = null;
        for (AssertTrue at : allAssertTrue) {
            if (at.child(0) instanceof org.apache.doris.nereids.trees.expressions.Or) {
                boundaryGuard = at;
                break;
            }
        }
        Assertions.assertNotNull(boundaryGuard,
                "Should find a boundary guard AssertTrue with Or condition. Found "
                + allAssertTrue.size() + " AssertTrue(s) with conditions: "
                + allAssertTrue.stream().map(at -> at.child(0).getClass().getSimpleName())
                        .collect(Collectors.joining(", "))
                + ". Projects: " + result.finalProject.getProjects().stream()
                        .map(e -> e.getName()).collect(Collectors.joining(", ")));

        // Guard should be: OR(IS_NULL(deltaDel), OR(IS_NULL(old), deltaDel > old))
        Expression guardCondition = boundaryGuard.child(0);
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Or.class, guardCondition,
                "Guard predicate should be a compound Or expression");
        // The second child of AssertTrue should be the error message (StringLiteral)
        Assertions.assertInstanceOf(
                org.apache.doris.nereids.trees.expressions.literal.StringLiteral.class,
                boundaryGuard.child(1),
                "AssertTrue second argument should be error message StringLiteral");
        // Verify the message mentions MIN
        String msg = ((org.apache.doris.nereids.trees.expressions.literal.StringLiteral)
                boundaryGuard.child(1)).getStringValue();
        Assertions.assertTrue(msg.contains("MIN"),
                "Guard message should mention MIN, got: " + msg);
    }

    private void collectAssertTrue(Expression expr, List<AssertTrue> result) {
        if (expr instanceof AssertTrue) {
            result.add((AssertTrue) expr);
        }
        for (Expression child : expr.children()) {
            collectAssertTrue(child, result);
        }
    }

    // ---- B4: Composite group keys ----

    @Test
    void testCompositeGroupKeysAggPlan() {
        AggRewriteResult result = rewriteAgg(buildCompositeGroupAgg(buildScan()));

        // Should have net-zero filter (grouped agg)
        Assertions.assertInstanceOf(LogicalFilter.class, result.finalProject.child(),
                "Composite group agg should have net-zero filter");

        LogicalJoin<?, ?> join = getJoin(result);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, join.getJoinType());

        // Join condition should use row_id (which is hash of composite keys)
        Assertions.assertFalse(join.getHashJoinConjuncts().isEmpty());
        Expression joinCondition = join.getHashJoinConjuncts().get(0);
        Assertions.assertInstanceOf(EqualTo.class, joinCondition);
        Assertions.assertTrue(joinCondition.toSql().contains(Column.IVM_ROW_ID_COL),
                "Join should be on row_id column");

        // Delta sub-plan aggregate should have both group keys
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();
        // Group keys: k1, k2; agg outputs: delta_group_count, delta_sum_count, delta_sum
        // Total: 2 (group keys) + 4 (delta_group_count + count_star_hidden_count + sum_hidden_sum + sum_hidden_count) = 6
        Assertions.assertTrue(deltaAgg.getGroupByExpressions().size() >= 2,
                "Expected at least 2 group-by keys, got " + deltaAgg.getGroupByExpressions().size());

        // Verify delete sign is IF expression (grouped, not scalar)
        NamedExpression lastExpr = result.finalProject.getProjects()
                .get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child(),
                "Grouped agg delete sign should be IF expression");
    }

    @Test
    void testExpressionAggArgumentSumProducesValidPlan() {
        // SUM(id + name) — expression aggregate argument should be accepted
        AggRewriteResult result = rewriteAgg(buildExprAgg(buildScan()));

        // Should have net-zero filter (grouped agg)
        Assertions.assertInstanceOf(LogicalFilter.class, result.finalProject.child(),
                "Expr agg should have net-zero filter");

        LogicalJoin<?, ?> join = getJoin(result);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, join.getJoinType());

        // Delta sub-plan should compile without error
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();
        Assertions.assertEquals(1, deltaAgg.getGroupByExpressions().size());
    }

    @Test
    void testExpressionAggArgumentMinMaxProducesValidPlan() {
        // MIN(id + name), MAX(id + name) — expression args for extremal aggs
        AggRewriteResult result = rewriteAgg(buildExprMinMaxAgg(buildScan()));

        // Should have net-zero filter (grouped agg)
        Assertions.assertInstanceOf(LogicalFilter.class, result.finalProject.child(),
                "Expr min/max agg should have net-zero filter");

        LogicalJoin<?, ?> join = getJoin(result);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, join.getJoinType());

        // assert_true guards should be present in the plan
        Assertions.assertTrue(result.finalProject.getProjects().stream()
                .anyMatch(expr -> expr.toSql().contains("assert_true")),
                "MIN/MAX with expression args should still have assert_true guards");
    }

    private static final class AggRewriteResult {
        private final PlanBundle bundle;
        private final MTMV mtmv;
        private final UnboundTableSink<?> sink;
        private final LogicalProject<?> finalProject;

        private AggRewriteResult(PlanBundle bundle, MTMV mtmv,
                UnboundTableSink<?> sink, LogicalProject<?> finalProject) {
            this.bundle = bundle;
            this.mtmv = mtmv;
            this.sink = sink;
            this.finalProject = finalProject;
        }
    }
}
