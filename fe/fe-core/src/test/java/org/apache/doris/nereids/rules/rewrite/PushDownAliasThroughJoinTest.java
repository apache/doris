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

class PushDownAliasThroughJoinTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    void testPushdown() {
        // condition don't use alias slot
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(1, 3), ImmutableList.of("1name", "2name"))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject().when(project -> project.getProjects().get(1).toSql().equals("name") && project.getProjects().get(2).toSql().equals("name AS `1name`")),
                            logicalProject().when(project -> project.getProjects().get(1).toSql().equals("name") && project.getProjects().get(2).toSql().equals("name AS `2name`"))
                        )
                    ).when(project -> project.getProjects().get(0).toSql().equals("1name") && project.getProjects().get(1).toSql().equals("2name"))
                );
    }

    @Test
    void testCondition() {
        // condition use alias slot
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(0, 1, 3), ImmutableList.of("1id", "1name", "2name"))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject().when(
                                    project -> project.getProjects().get(2).toSql().equals("id AS `1id`")
                                            && project.getProjects().get(3).toSql().equals("name AS `1name`")),
                            logicalProject().when(
                                    project -> project.getProjects().get(2).toSql().equals("name AS `2name`"))
                        ).when(join -> join.getHashJoinConjuncts().get(0).toSql().equals("(1id = id)"))
                    ).when(project -> project.getProjects().get(0).toSql().equals("1id")
                        && project.getProjects().get(1).toSql().equals("1name")
                        && project.getProjects().get(2).toSql().equals("2name"))
                );
    }

    @Test
    void testJustRightSide() {
        // condition use alias slot
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(2, 3), ImmutableList.of("2id", "2name"))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalProject()
                        ).when(join -> join.getHashJoinConjuncts().get(0).toSql().equals("(id = 2id)"))
                    ).when(project -> project.getProjects().get(0).toSql().equals("2id")
                            && project.getProjects().get(1).toSql().equals("2name"))
                );
    }

    @Test
    void testNoPushdownMarkJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .markJoin(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(2), ImmutableList.of("fake")).build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .matches(logicalProject(logicalJoin(logicalOlapScan(), logicalOlapScan())));
    }
}
