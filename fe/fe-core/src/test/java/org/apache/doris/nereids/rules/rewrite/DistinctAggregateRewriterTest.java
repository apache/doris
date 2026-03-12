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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DistinctAggregateRewriterTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.distinct_agg_split_t(a int null, b int not null,"
                + "c varchar(10) null, d date, dt datetime)\n"
                + "distributed by hash(a) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        new SessionVariableMockUp();
    }

    private static class SessionVariableMockUp extends MockUp<SessionVariable> {
        @Mock
        private int getParallelExecInstanceNum(String clusterName) {
            return 24;
        }
    }

    private void applyMock() {
        new MockUp<DistinctAggregateRewriter>() {
            @Mock
            boolean shouldUseMultiDistinct(LogicalAggregate<? extends Plan> aggregate) {
                return false;
            }
        };
    }

    @Test
    void testSplitSingleDistinctAgg() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2
                                        && agg.getAggregateFunctions().isEmpty())
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().iterator().next() instanceof Count
                        )
                );
    }

    @Test
    void testSplitSingleDistinctAggOtherFunctionCount() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), count(a) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2
                                                && agg.getAggregateFunctions().iterator().next() instanceof Count)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().anyMatch(f -> f instanceof Sum0)
                        )
                );
    }

    @Test
    void testSplitSingleDistinctWithOtherAgg() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), sum(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 2)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().noneMatch(AggregateFunction::isDistinct)
                        ));
    }

    @Test
    void testNotSplitWhenNoGroupBy() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select count(distinct a) from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate(logicalAggregate()));
    }

    @Test
    void testSplitWhenNoGroupByHasGroupConcatDistinctOrderBy() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select group_concat(distinct a, '' order by b) from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate()
                        .when(agg ->
                                agg.getAggregateFunctions().iterator().next() instanceof MultiDistinctGroupConcat));
    }

    @Test
    void testSplitWhenNoGroupByHasGroupConcatDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select group_concat(distinct a, '') from test.distinct_agg_split_t")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate()
                        .when(agg ->
                                agg.getAggregateFunctions().iterator().next() instanceof MultiDistinctGroupConcat));
    }

    @Test
    void testMultiExprDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, sum(a), count(distinct a,c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate()
                                        .when(agg -> agg.getGroupByExpressions().size() == 3
                                                && agg.getAggregateFunctions().size() == 1)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getAggregateFunctions().stream().anyMatch(
                                        f -> f instanceof Count && f.child(0) instanceof If
                                                && !f.isDistinct()))
                );
    }

    @Test
    void testNotSplitWhenNoDistinct() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, sum(a), count(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalAggregate(logicalAggregate()));
    }

    @Test
    void testSplitWithComplexExpression() {
        applyMock();
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a + 1) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 2)
                        ).when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")));
    }

    @Test
    void testMultiDistinct() {
        connectContext.getSessionVariable().setAggPhase(2);
        PlanChecker.from(connectContext)
                .analyze("select b, count(distinct a), sum(c) from test.distinct_agg_split_t group by b")
                .rewrite()
                .printlnTree()
                .matches(
                        logicalAggregate().when(agg -> agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("b")
                                && agg.getAggregateFunctions().stream().noneMatch(AggregateFunction::isDistinct)
                                && agg.getAggregateFunctions().stream().anyMatch(f -> f instanceof MultiDistinctCount)
                        ));
        connectContext.getSessionVariable().setAggPhase(0);
    }

    @Test
    void testShouldUseMultiDistinctWithoutStatsSatisfyDistribution() throws Exception {
        DistinctAggregateRewriter rewriter = DistinctAggregateRewriter.INSTANCE;
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select bb, count(distinct aa) from "
                        + "(select a as aa, b as bb from test.distinct_agg_split_t where b > 1) t "
                        + "group by bb"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, unknownColumnStats()));
        ((AbstractPlan) child).setStatistics(new Statistics(10000, colStats));
        aggregate.setStatistics(new Statistics(100, ImmutableMap.of()));

        Assertions.assertFalse(rewriter.shouldUseMultiDistinct(aggregate));
    }

    @Test
    void testShouldUseMultiDistinctWithStatsSelected() throws Exception {
        DistinctAggregateRewriter rewriter = new DistinctAggregateRewriter();
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select b, count(distinct a) from test.distinct_agg_split_t group by b"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, buildColumnStats(240, false)));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, buildColumnStats(10000.0, false)));
        ((AbstractPlan) child).setStatistics(new Statistics(100000, colStats));
        aggregate.setStatistics(new Statistics(240, ImmutableMap.of()));

        Assertions.assertTrue(rewriter.shouldUseMultiDistinct(aggregate));
    }

    @Test
    void testShouldUseMultiDistinctWithStatsNotSelected() throws Exception {
        DistinctAggregateRewriter rewriter = new DistinctAggregateRewriter();
        LogicalAggregate<? extends Plan> aggregate = getLogicalAggregate(
                "select b, count(distinct a) from test.distinct_agg_split_t group by b"
        );
        Plan child = aggregate.child();
        Map<org.apache.doris.nereids.trees.expressions.Expression, ColumnStatistic> colStats = new HashMap<>();
        aggregate.getGroupByExpressions().forEach(expr ->
                colStats.put(expr, buildColumnStats(1000.0, false)));
        aggregate.getDistinctArguments().forEach(expr ->
                colStats.put(expr, buildColumnStats(10000.0, false)));
        ((AbstractPlan) child).setStatistics(new Statistics(100000, colStats));
        aggregate.setStatistics(new Statistics(1000.0, ImmutableMap.of()));

        Assertions.assertFalse(rewriter.shouldUseMultiDistinct(aggregate));
    }

    private LogicalAggregate<? extends Plan> getLogicalAggregate(String sql) {
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .getPlan();
        Optional<LogicalAggregate<? extends Plan>> aggregate = findAggregate(plan);
        Assertions.assertTrue(aggregate.isPresent());
        return aggregate.get();
    }

    private Optional<LogicalAggregate<? extends Plan>> findAggregate(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            return Optional.of((LogicalAggregate<? extends Plan>) plan);
        }
        for (Plan child : plan.children()) {
            Optional<LogicalAggregate<? extends Plan>> found = findAggregate(child);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private ColumnStatistic unknownColumnStats() {
        return buildColumnStats(0.0, true);
    }

    private ColumnStatistic buildColumnStats(double ndv, boolean isUnknown) {
        return new ColumnStatisticBuilder(1)
                .setNdv(ndv)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100)
                .setIsUnknown(isUnknown)
                .setUpdatedTime("")
                .build();
    }
}
