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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class EagerSplitTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
            PlanConstructor.student, ImmutableList.of(""));
    private final LogicalOlapScan scan2 = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
            PlanConstructor.score, ImmutableList.of(""));

    @Test
    void singleSum() {
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0, 4),
                        ImmutableList.of(
                                new Alias(new Sum(scan1.getOutput().get(3)), "lsum0"),
                                new Alias(new Sum(scan2.getOutput().get(2)), "rsum0")
                        ))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyExploration(EagerSplit.INSTANCE.build())
                .printlnOrigin()
                .printlnExploration()
                .matchesExploration(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate().when(
                                                a -> a.getOutputExprsSql().equals("id, sum(age) AS `left_sum0`, count(*) AS `left_cnt`")),
                                        logicalAggregate().when(
                                                a -> a.getOutputExprsSql().equals("sid, sum(grade) AS `right_sum0`, count(*) AS `right_cnt`"))
                                )
                        ).when(newAgg ->
                                newAgg.getGroupByExpressions().equals(((Aggregate) agg).getGroupByExpressions())
                                        && newAgg.getOutputExprsSql().equals("sum((left_sum0 * right_cnt)) AS `lsum0`, sum((right_sum0 * left_cnt)) AS `rsum0`"))
                );
    }

    @Test
    void multiSum() {
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0, 4),
                        ImmutableList.of(
                                new Alias(new Sum(scan1.getOutput().get(1)), "lsum0"),
                                new Alias(new Sum(scan1.getOutput().get(2)), "lsum1"),
                                new Alias(new Sum(scan1.getOutput().get(3)), "lsum2"),
                                new Alias(new Sum(scan2.getOutput().get(1)), "rsum0"),
                                new Alias(new Sum(scan2.getOutput().get(2)), "rsum1")
                        ))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyExploration(EagerSplit.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate().when(a -> a.getOutputExprsSql()
                                                .equals("id, sum(gender) AS `left_sum0`, sum(name) AS `left_sum1`, sum(age) AS `left_sum2`, count(*) AS `left_cnt`")),
                                        logicalAggregate().when(a -> a.getOutputExprsSql()
                                                .equals("sid, sum(cid) AS `right_sum0`, sum(grade) AS `right_sum1`, count(*) AS `right_cnt`"))
                                )
                        ).when(newAgg ->
                                newAgg.getGroupByExpressions().equals(((Aggregate) agg).getGroupByExpressions())
                                        && newAgg.getOutputExprsSql()
                                        .equals("sum((left_sum0 * right_cnt)) AS `lsum0`, sum((left_sum1 * right_cnt)) AS `lsum1`, sum((left_sum2 * right_cnt)) AS `lsum2`, sum((right_sum0 * left_cnt)) AS `rsum0`, sum((right_sum1 * left_cnt)) AS `rsum1`"))
                );
    }
}
