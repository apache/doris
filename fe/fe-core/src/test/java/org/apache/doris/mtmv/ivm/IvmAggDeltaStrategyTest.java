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
        InsertIntoTableCommand command = (InsertIntoTableCommand) new IvmAggDeltaStrategy()
                .rewrite(bundle.normalizedPlan, ctx).get(0).getCommand();
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
    void testAggWithMaxThrows() {
        LogicalOlapScan scan = buildScan();
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> normalizeAggPlan(buildMaxAgg(scan)));
        Assertions.assertTrue(ex.getMessage().contains("min/max"));
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
                () -> new IvmAggDeltaStrategy().rewrite(bundle.normalizedPlan, ctx));
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
