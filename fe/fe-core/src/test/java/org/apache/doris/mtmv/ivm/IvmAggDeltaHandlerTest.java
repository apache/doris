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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class IvmAggDeltaHandlerTest extends IvmDeltaTestBase {

    private AggRewriteResult rewriteAgg(LogicalAggregate<? extends Plan> agg) {
        PlanBundle bundle = normalizeAggPlan(agg);
        MTMV mtmv = buildMtmvFromPlan(bundle.normalizedPlan.getOutput());
        IvmRefreshContext ctx = new IvmRefreshContext(mtmv, bundle.connectContext, bundle.rewriteResult);
        // Generate delta plans via the rewriter (handles stream fallback for test environments)
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmDeltaRewriter()
                .rewrite(bundle.normalizedPlan, ctx).get(0);
        UnboundTableSink<?> sink = getSink(command);
        return new AggRewriteResult(bundle, mtmv, sink, (LogicalProject<?>) sink.child());
    }

    private LogicalJoin<?, ?> getJoin(AggRewriteResult result) {
        Plan applyInput = getApplyProject(result).child();
        if (applyInput instanceof LogicalFilter) {
            return (LogicalJoin<?, ?>) ((LogicalFilter<?>) applyInput).child();
        }
        return (LogicalJoin<?, ?>) applyInput;
    }

    private LogicalProject<?> getDeltaTopProject(AggRewriteResult result) {
        return (LogicalProject<?>) getJoin(result).right();
    }

    private LogicalProject<?> getNormalizedTopProject(AggRewriteResult result) {
        return (LogicalProject<?>) result.finalProject.child();
    }

    private LogicalProject<?> getApplyProject(AggRewriteResult result) {
        return (LogicalProject<?>) getNormalizedTopProject(result).child();
    }

    private void assertSinkProjectMatchesSinkColumns(AggRewriteResult result) {
        List<String> expectedSinkColumns = new ArrayList<>(result.mtmv.getInsertedColumnNames());
        expectedSinkColumns.add(Column.DELETE_SIGN);
        Assertions.assertEquals(expectedSinkColumns, result.sink.getColNames());
        Assertions.assertEquals(expectedSinkColumns, result.finalProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList()));
    }

    private LogicalOlapScan buildMowScan(long tableId, String tableName, boolean delta) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, tableName, 0, KeysType.UNIQUE_KEYS);
        table.setEnableUniqueKeyMergeOnWrite(true);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        return delta ? scan : scan;
    }

    private LogicalAggregate<LogicalOlapScan> buildScalarMixedAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        return new LogicalAggregate<>(
                ImmutableList.of(),
                ImmutableList.of(
                        new Alias(new Count(nameSlot), "cnt_name"),
                        new Alias(new Sum(idSlot), "sum_id"),
                        new Alias(new Avg(idSlot), "avg_id"),
                        new Alias(new Min(nameSlot), "min_name"),
                        new Alias(new Max(nameSlot), "max_name")),
                true, Optional.empty(), scan);
    }

    private String transientDeltaColumnName(int ordinal, String suffix) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "TRANSIENT_" + ordinal + "_" + suffix + "_COL__";
    }

    private void assertDeltaTopProjectCoalesce(LogicalProject<?> deltaTopProject,
            String outputName, boolean expected) {
        List<NamedExpression> matches = deltaTopProject.getProjects().stream()
                .filter(expr -> outputName.equals(expr.getName()))
                .collect(Collectors.toList());
        Assertions.assertEquals(1, matches.size(), "Expected one delta top project output: " + outputName);
        NamedExpression output = matches.get(0);
        boolean actual = output instanceof Alias && ((Alias) output).child() instanceof Coalesce;
        Assertions.assertEquals(expected, actual, "Unexpected COALESCE state for " + outputName
                + ", expression: " + output.toSql());
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
        List<String> outputNames = result.finalProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList());
        Assertions.assertEquals(result.mtmv.getInsertedColumnNames(),
                outputNames.subList(0, result.mtmv.getInsertedColumnNames().size()));
    }

    @Test
    void testGroupedAggTopProjectAddsRowIdAndSinkProjectMatchesSinkColumns() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        List<String> applyOutputNames = getApplyProject(result).getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList());
        Assertions.assertFalse(applyOutputNames.contains(Column.IVM_ROW_ID_COL));
        Assertions.assertTrue(applyOutputNames.containsAll(result.mtmv.getInsertedColumnNames().stream()
                .filter(name -> !Column.IVM_ROW_ID_COL.equals(name)).collect(Collectors.toList())));
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL,
                applyOutputNames.get(applyOutputNames.size() - 1));
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, getNormalizedTopProject(result).getOutput().get(0).getName());
        assertSinkProjectMatchesSinkColumns(result);
    }

    @Test
    void testRootAggAboveLeftOuterJoinUsesOuterJoinDeltaRewrite() {
        LogicalOlapScan leftDelta = buildMowScan(1, "a", true);
        LogicalOlapScan rightSnapshot = buildMowScan(2, "b", false);
        LogicalJoin<?, ?> outerJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), leftDelta, rightSnapshot, JoinReorderContext.EMPTY);
        Slot groupSlot = outerJoin.getOutput().get(0);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(groupSlot), ImmutableList.of(groupSlot, countAlias),
                true, Optional.empty(), outerJoin);

        AggRewriteResult result = rewriteAgg(agg);

        Assertions.assertEquals(Column.DELETE_SIGN,
                result.sink.getColNames().get(result.sink.getColNames().size() - 1));
        Assertions.assertInstanceOf(LogicalProject.class, result.finalProject);
    }

    @Test
    void testScalarAggSkipsNetZeroFilterButKeepsDeleteSignSink() {
        AggRewriteResult result = rewriteAgg(buildScalarAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalJoin.class, getApplyProject(result).child());
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
        boolean hasAssertTrue = getApplyProject(result).getProjects().stream()
                .anyMatch(expr -> containsAssertTrue(expr));
        Assertions.assertTrue(hasAssertTrue, "Expected assert_true guard in MAX apply plan");
    }

    @Test
    void testMinApplyPlanContainsAssertTrueGuard() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));
        boolean hasAssertTrue = getApplyProject(result).getProjects().stream()
                .anyMatch(expr -> containsAssertTrue(expr));
        Assertions.assertTrue(hasAssertTrue, "Expected assert_true guard in MIN apply plan");
    }

    private boolean containsAssertTrue(Expression expr) {
        if (expr instanceof AssertTrue) {
            return true;
        }
        for (Expression child : expr.children()) {
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
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());

        NamedExpression dmlFactor = getApplyProject(result).getProjects()
                .get(getApplyProject(result).getProjects().size() - 1);
        Assertions.assertEquals(Column.IVM_DML_FACTOR_COL, dmlFactor.getName());
        Assertions.assertInstanceOf(TinyIntLiteral.class, ((Alias) dmlFactor).child());
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
    void testMixedAggDeltaSubPlanExposesHandlerOwnedOutputs() {
        AggRewriteResult result = rewriteAgg(buildScalarMixedAgg(buildScan()));
        LogicalAggregate<?> deltaAgg = (LogicalAggregate<?>) getDeltaTopProject(result).child();

        List<String> outputNames = deltaAgg.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
        Assertions.assertEquals(12, outputNames.size());
        Assertions.assertTrue(outputNames.contains(Column.IVM_DELTA_GROUP_COUNT_COL));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "SUM")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(2, "SUM")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(2, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(3, "MIN")));
        Assertions.assertTrue(outputNames.contains(transientDeltaColumnName(3, "DELMIN")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(3, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(4, "MAX")));
        Assertions.assertTrue(outputNames.contains(transientDeltaColumnName(4, "DELMAX")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(4, "COUNT")));
    }

    @Test
    void testScalarDeltaTopProjectCoalescesOnlyZeroDefaultOutputs() {
        AggRewriteResult result = rewriteAgg(buildScalarMixedAgg(buildScan()));
        LogicalProject<?> deltaTopProject = getDeltaTopProject(result);

        assertDeltaTopProjectCoalesce(deltaTopProject, Column.IVM_DELTA_GROUP_COUNT_COL, true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(0, "COUNT"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(1, "SUM"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(1, "COUNT"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(2, "SUM"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(2, "COUNT"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(3, "MIN"), false);
        assertDeltaTopProjectCoalesce(deltaTopProject, transientDeltaColumnName(3, "DELMIN"), false);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(3, "COUNT"), true);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(4, "MAX"), false);
        assertDeltaTopProjectCoalesce(deltaTopProject, transientDeltaColumnName(4, "DELMAX"), false);
        assertDeltaTopProjectCoalesce(deltaTopProject, IvmUtil.ivmAggHiddenColumnName(4, "COUNT"), true);
    }

    @Test
    void testGroupedAggOutputColumnsMatchMtmvInsertedPlusDeleteSign() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        List<String> expectedSinkCols = new ArrayList<>(result.mtmv.getInsertedColumnNames());
        expectedSinkCols.add(Column.DELETE_SIGN);
        Assertions.assertEquals(expectedSinkCols, result.sink.getColNames());
    }

    @Test
    void testAggMissingRewriteResultThrows() {
        LogicalAggregate<?> agg = buildGroupedAgg(buildScan());
        MTMV mtmv = buildMtmvFromPlan(agg.getOutput());
        IvmRefreshContext ctx = new IvmRefreshContext(mtmv, new ConnectContext(), null);
        IvmDeltaRewriteResult childResult = new IvmDeltaRewriteResult(agg.child(0), (Slot) null, (Slot) null);

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> new IvmAggDeltaHandler().rewriteAggregate(agg, childResult, ctx));
        Assertions.assertTrue(ex.getMessage().contains("normalize result"));
    }

    @Test
    void testNetZeroFilterPredicateStructure() {
        AggRewriteResult result = rewriteAgg(buildGroupedAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalFilter.class, getApplyProject(result).child());
        LogicalFilter<?> filter = (LogicalFilter<?>) getApplyProject(result).child();
        Assertions.assertEquals(1, filter.getConjuncts().size());
        Assertions.assertInstanceOf(Not.class, filter.getConjuncts().iterator().next());
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
        Assertions.assertInstanceOf(LogicalJoin.class, getApplyProject(result).child(),
                "Scalar MIN should have no net-zero filter");
        // Delete sign is produced by the common sink project from the aggregate-level dml_factor.
        NamedExpression lastExpr = result.finalProject.getProjects()
                .get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());
        // Must contain assert_true guard for MIN boundary protection
        boolean hasAssertTrue = getApplyProject(result).getProjects().stream()
                .anyMatch(this::containsAssertTrue);
        Assertions.assertTrue(hasAssertTrue, "Scalar MIN should have assert_true guard");
    }

    @Test
    void testScalarMaxAggSkipsNetZeroFilterAndHasGuard() {
        AggRewriteResult result = rewriteAgg(buildScalarMaxAgg(buildScan()));

        Assertions.assertInstanceOf(LogicalJoin.class, getApplyProject(result).child(),
                "Scalar MAX should have no net-zero filter");
        NamedExpression lastExpr = result.finalProject.getProjects()
                .get(result.finalProject.getProjects().size() - 1);
        Assertions.assertEquals(Column.DELETE_SIGN, lastExpr.getName());
        Assertions.assertInstanceOf(Alias.class, lastExpr);
        Assertions.assertInstanceOf(If.class, ((Alias) lastExpr).child());
        boolean hasAssertTrue = getApplyProject(result).getProjects().stream()
                .anyMatch(this::containsAssertTrue);
        Assertions.assertTrue(hasAssertTrue, "Scalar MAX should have assert_true guard");
    }

    // ---- B2: Combined MIN+MAX in single MV ----

    @Test
    void testCombinedMinMaxAggHasTwoAssertTrueGuards() {
        AggRewriteResult result = rewriteAgg(buildMinMaxAgg(buildScan()));

        // Count distinct assert_true-containing project expressions
        long guardCount = getApplyProject(result).getProjects().stream()
                .filter(this::containsAssertTrue).count();
        // MIN guard + MAX guard = at least 2 expressions containing assert_true
        // (visible value expressions also reference the guarded hidden state, so count >= 2)
        Assertions.assertTrue(guardCount >= 2,
                "Expected at least 2 assert_true guards (MIN + MAX), got " + guardCount);

        // Net-zero filter should be present (grouped agg)
        Assertions.assertInstanceOf(LogicalFilter.class, getApplyProject(result).child(),
                "Combined MIN+MAX grouped agg should have net-zero filter");
    }

    // ---- B3: Assert_true predicate semantics ----

    @Test
    void testAssertTrueGuardPredicateIsCompoundOrExpression() {
        AggRewriteResult result = rewriteAgg(buildMinAgg(buildScan()));

        // Find the boundary guard AssertTrue — the one whose condition is an Or expression.
        // (assertNonNegative also uses AssertTrue but with GreaterThanEqual; skip those)
        List<AssertTrue> allAssertTrue = new ArrayList<>();
        for (NamedExpression expr : getApplyProject(result).getProjects()) {
            collectAssertTrue(expr, allAssertTrue);
        }
        Assertions.assertFalse(allAssertTrue.isEmpty(), "Should find AssertTrue expressions in final project");

        AssertTrue boundaryGuard = null;
        for (AssertTrue at : allAssertTrue) {
            if (at.child(0) instanceof Or) {
                boundaryGuard = at;
                break;
            }
        }
        Assertions.assertNotNull(boundaryGuard,
                "Should find a boundary guard AssertTrue with Or condition. Found "
                + allAssertTrue.size() + " AssertTrue(s) with conditions: "
                + allAssertTrue.stream().map(at -> at.child(0).getClass().getSimpleName())
                        .collect(Collectors.joining(", "))
                + ". Projects: " + getApplyProject(result).getProjects().stream()
                        .map(e -> e.getName()).collect(Collectors.joining(", ")));

        // Guard should be: OR(IS_NULL(deltaDel), OR(IS_NULL(old), deltaDel > old))
        Expression guardCondition = boundaryGuard.child(0);
        Assertions.assertInstanceOf(Or.class, guardCondition,
                "Guard predicate should be a compound Or expression");
        // The second child of AssertTrue should be the error message (StringLiteral)
        Assertions.assertInstanceOf(
                StringLiteral.class,
                boundaryGuard.child(1),
                "AssertTrue second argument should be error message StringLiteral");
        // Verify the message mentions MIN
        String msg = ((StringLiteral) boundaryGuard.child(1)).getStringValue();
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
        Assertions.assertInstanceOf(LogicalFilter.class, getApplyProject(result).child(),
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
        // Total: 2 group keys plus 4 aggregate outputs.
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
        Assertions.assertInstanceOf(LogicalFilter.class, getApplyProject(result).child(),
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
        Assertions.assertInstanceOf(LogicalFilter.class, getApplyProject(result).child(),
                "Expr min/max agg should have net-zero filter");

        LogicalJoin<?, ?> join = getJoin(result);
        Assertions.assertEquals(JoinType.RIGHT_OUTER_JOIN, join.getJoinType());

        // assert_true guards should be present in the plan
        Assertions.assertTrue(getApplyProject(result).getProjects().stream()
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
