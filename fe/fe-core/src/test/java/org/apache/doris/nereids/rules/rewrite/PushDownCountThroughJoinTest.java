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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PushDownCountThroughJoinTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void testSingleCount() {
        Alias count = new Count(scan1.getOutput().get(0)).alias("count");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), count))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }

    @Test
    void testMultiCount() {
        Alias leftCnt1 = new Count(scan1.getOutput().get(0)).alias("leftCnt1");
        Alias leftCnt2 = new Count(scan1.getOutput().get(1)).alias("leftCnt2");
        Alias rightCnt1 = new Count(scan2.getOutput().get(1)).alias("rightCnt1");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(scan1.getOutput().get(0), leftCnt1, leftCnt2, rightCnt1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }

    /**
     * verify that after applying PushDownAggThroughJoin rule, agg func has correct dataType
     */
    @Test
    void testSumAndDataType() {
        LogicalOlapScan salary1 = new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.salary, ImmutableList.of(""));
        Alias sum = new Sum(salary1.getOutput().get(2)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(salary1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(salary1.getOutput().get(0), sum))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(logicalAggregate(
                        logicalJoin(
                                logicalAggregate(),
                                logicalAggregate()
                        )
                ).when(agg -> {
                    Multiply multiply = null;
                    for (Expression expr : agg.getOutputExpressions()) {
                        if (expr instanceof Alias
                                && expr.child(0) instanceof Sum
                                && expr.child(0).child(0) instanceof Multiply) {
                            multiply = (Multiply) expr.child(0).child(0);
                            break;
                        }
                    }
                    if (multiply == null) {
                        return false;
                    }
                    Assertions.assertInstanceOf(DecimalV3Type.class, multiply.child(0).getDataType());
                    Assertions.assertInstanceOf(DecimalV3Type.class, multiply.child(1).getDataType());
                    return true;
                }));
    }

    @Test
    void testSingleCountStar() {
        Alias count = new Count().alias("countStar");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), count))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }

    @Test
    void testSingleCountStarEmptyGroupBy() {
        Alias count = new Count().alias("countStar");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(), ImmutableList.of(count))
                .build();

        // shouldn't rewrite.
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBothSideCountAndCountStar() {
        Alias leftCnt = new Count(scan1.getOutput().get(0)).alias("leftCnt");
        Alias rightCnt = new Count(scan2.getOutput().get(0)).alias("rightCnt");
        Alias countStar = new Count().alias("countStar");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(scan1.getOutput().get(0), leftCnt, rightCnt, countStar))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoin())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalAggregate()
                                )
                        )
                );
    }
}
