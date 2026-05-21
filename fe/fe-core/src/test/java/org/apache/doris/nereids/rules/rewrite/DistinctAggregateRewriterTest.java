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

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

public class DistinctAggregateRewriterTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.distinct_agg_split_t(a int null, b int not null,"
                + "c varchar(10) null, d date, dt datetime)\n"
                + "distributed by hash(a) properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
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
    }
}
