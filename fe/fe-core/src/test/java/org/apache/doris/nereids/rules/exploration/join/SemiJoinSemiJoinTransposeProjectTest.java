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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class SemiJoinSemiJoinTransposeProjectTest implements MemoPatternMatchSupported {
    public static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    public static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    public static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSemiProjectSemiCommute() {
        /*
         *     t1.name=t3.name              t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /        \
         *    project   t3        t1.name=t3.name    t2
         *    t1.name       -->    newBottomJoin
         *      |                     /    \
         * t1.id=t2.id             t1      t3
         * bottomJoin
         *   /    \
         * t1      t2
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(1))
                .join(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 1))
                .projectAll()
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(logicalJoin(
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                        ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)),
                                        logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_ANTI_JOIN)
                        )
                );
    }

    @Test
    public void testSemiProjectSemiCommuteMarkJoin() {
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .markJoinWithMarkConjuncts(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(0, 2))
                .markJoinWithMarkConjuncts(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 1))
                .project(ImmutableList.of(1, 2))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinSemiJoinTransposeProject.INSTANCE.build())
                .matchesExploration(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                                        ).when(project -> project.getProjects().size() == 2),
                                        logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                                ).when(join -> join.getJoinType() == JoinType.LEFT_SEMI_JOIN)
                        )
                );
    }
}
