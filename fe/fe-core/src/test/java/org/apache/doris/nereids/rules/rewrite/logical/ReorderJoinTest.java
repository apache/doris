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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

class ReorderJoinTest implements PatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);

    @Test
    public void testLeftOuterJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .hashJoinUsing(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .hashJoinUsing(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testRightOuterJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .hashJoinUsing(scan2, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .hashJoinUsing(scan2, JoinType.RIGHT_OUTER_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testLeftSemiJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .hashJoinUsing(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .hashJoinUsing(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testRightSemiJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .hashJoinUsing(scan2, JoinType.RIGHT_SEMI_JOIN, Pair.of(0, 0))
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan2.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .hashJoinUsing(scan2, JoinType.RIGHT_SEMI_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    @Test
    public void testFullOuterJoin() {
        ImmutableList<LogicalPlan> plans = ImmutableList.of(
                new LogicalPlanBuilder(scan1)
                        .hashJoinUsing(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build(),
                new LogicalPlanBuilder(scan1)
                        .hashJoinEmptyOn(scan3, JoinType.CROSS_JOIN)
                        .hashJoinUsing(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(0, 0))
                        .filter(new EqualTo(scan3.getOutput().get(0), scan1.getOutput().get(0)))
                        .build()
        );

        check(plans);
    }

    public void check(List<LogicalPlan> plans) {
        for (LogicalPlan plan : plans) {
            PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                    .applyBottomUp(new ReorderJoin())
                    .matchesFromRoot(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).whenNot(join -> join.getJoinType().isCrossJoin())
                    )
                    .printlnTree();
        }
    }

    /**
     *                                  join
     *      crossjoin                   /  \
     *       /     \                  join  D
     * innerjoin  innerjoin  ──►      /  \
     *   /   \     /   \            join  C
     *  A     B   C     D           /  \
     *                             A    B
     */
    @Test
    public void testInnerOrCrossJoin() {
        LogicalPlan leftJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        LogicalPlan rightJoin = new LogicalPlanBuilder(scan3)
                .hashJoinUsing(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(leftJoin)
                .hashJoinEmptyOn(rightJoin, JoinType.CROSS_JOIN)
                .filter(new EqualTo(scan1.getOutput().get(0), scan3.getOutput().get(0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyBottomUp(new ReorderJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalJoin(
                                        logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                        leafPlan()
                                ).whenNot(join -> join.getJoinType().isCrossJoin()),
                                leafPlan()
                        ).whenNot(join -> join.getJoinType().isCrossJoin())
                )
                .printlnTree();
    }
}
