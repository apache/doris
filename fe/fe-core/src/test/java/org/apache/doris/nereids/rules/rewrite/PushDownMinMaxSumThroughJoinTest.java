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
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Set;

class PushDownMinMaxSumThroughJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
    private MockUp<SessionVariable> mockUp = new MockUp<SessionVariable>() {
        @Mock
        public Set<Integer> getEnableNereidsRules() {
            return ImmutableSet.of(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE.type());
        }
    };

    @Test
    void testSingleJoin() {
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testMultiJoin() {
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(
                                                                logicalJoin(
                                                                        logicalAggregate(),
                                                                        logicalOlapScan()
                                                                )
                                                        ),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testAggNotOutputGroupBy() {
        // agg don't output group by
        Alias min = new Min(scan1.getOutput().get(0)).alias("min");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(min))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testBothSideSingleJoin() {
        Alias min = new Min(scan1.getOutput().get(1)).alias("min");
        Alias max = new Max(scan2.getOutput().get(1)).alias("max");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), min, max))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyTopDown(new PushDownAggThroughJoinOneSide())
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
    void testBothSide() {
        Alias min = new Min(scan1.getOutput().get(1)).alias("min");
        Alias max = new Max(scan3.getOutput().get(1)).alias("max");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(min, max))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(
                                                logicalJoin(
                                                        logicalAggregate(),
                                                        logicalOlapScan()
                                                )
                                        ),
                                        logicalAggregate()
                                )
                        )
                );
    }

    @Test
    void testSingleJoinLeftSum() {
        Alias sum = new Sum(scan1.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testSingleJoinRightSum() {
        Alias sum = new Sum(scan2.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalAggregate()
                                )
                        )
                );
    }

    @Test
    void testSumAggNotOutputGroupBy() {
        // agg don't output group by
        Alias sum = new Sum(scan1.getOutput().get(1)).alias("sum");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(sum))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    void testMultiSum() {
        Alias leftSum1 = new Sum(scan1.getOutput().get(0)).alias("leftSum1");
        Alias leftSum2 = new Sum(scan1.getOutput().get(1)).alias("leftSum2");
        Alias rightSum1 = new Sum(scan2.getOutput().get(1)).alias("rightSum1");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0),
                        ImmutableList.of(scan1.getOutput().get(0), leftSum1, leftSum2, rightSum1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
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
    void testSingleCount() {
        Alias count = new Count(scan1.getOutput().get(0)).alias("count");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), count))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .printlnTree()
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalAggregate(),
                                        logicalOlapScan()
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
                .applyTopDown(new PushDownAggThroughJoinOneSide())
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
    void testSingleCountStar() {
        Alias count = new Count().alias("countStar");
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .aggGroupUsingIndex(ImmutableList.of(0), ImmutableList.of(scan1.getOutput().get(0), count))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .printlnTree()
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
                .applyTopDown(new PushDownAggThroughJoinOneSide())
                .matches(
                        logicalAggregate(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalOlapScan()
                                )
                        )
                );
    }
}
