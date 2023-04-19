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

class EagerCountTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
            PlanConstructor.student, ImmutableList.of(""));
    private final LogicalOlapScan scan2 = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
            PlanConstructor.score, ImmutableList.of(""));

    @Test
    void singleSum() {
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0, 4),
                        ImmutableList.of(new Alias(new Sum(scan1.getOutput().get(1)), "sum")))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyExploration(new EagerCount().buildRules())
                .printlnExploration()
                .matchesExploration(
                    logicalAggregate(
                        logicalJoin(
                          logicalOlapScan(),
                          logicalAggregate().when(cntAgg -> cntAgg.getOutputExprsSql().equals("sid, count(*) AS `cnt`"))
                        )
                    ).when(newAgg -> newAgg.getGroupByExpressions().equals(((Aggregate) agg).getGroupByExpressions())
                                        && newAgg.getOutputExprsSql().equals("sum((gender * cnt)) AS `sum`"))
                );
    }

    @Test
    void multiSum() {
        LogicalPlan agg = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0, 4),
                        ImmutableList.of(
                                new Alias(new Sum(scan1.getOutput().get(1)), "sum0"),
                                new Alias(new Sum(scan1.getOutput().get(2)), "sum1"),
                                new Alias(new Sum(scan1.getOutput().get(3)), "sum2")
                        ))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), agg)
                .applyExploration(new EagerCount().buildRules())
                .printlnOrigin()
                .matchesExploration(
                    logicalAggregate(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalAggregate().when(cntAgg -> cntAgg.getOutputExprsSql().equals("sid, count(*) AS `cnt`"))
                        )
                    ).when(newAgg ->
                        newAgg.getGroupByExpressions().equals(((Aggregate) agg).getGroupByExpressions())
                            && newAgg.getOutputExprsSql().equals("sum((gender * cnt)) AS `sum0`, sum((name * cnt)) AS `sum1`, sum((age * cnt)) AS `sum2`"))
                );
    }
}
