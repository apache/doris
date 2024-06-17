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
import org.apache.doris.nereids.rules.rewrite.PushDownAliasThroughJoin;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class OuterJoinLAsscomProjectTest implements MemoPatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    void testJoinLAsscomProject() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .project(ImmutableList.of(0, 1, 2))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(1, 1))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnTree()
                .applyExploration(OuterJoinLAsscomProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                    logicalProject(
                        logicalJoin(
                            logicalProject(logicalJoin(
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                            )),
                            logicalProject(
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                            ).when(project -> project.getProjects().size() == 1)
                        )
                    )
                );
    }

    @Test
    void testAlias() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .alias(ImmutableList.of(0, 2), ImmutableList.of("t1.id", "t2.id"))
                .join(scan3, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0))
                .projectAll()
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new PushDownAliasThroughJoin())
                .printlnTree()
                .applyExploration(OuterJoinLAsscomProject.INSTANCE.build())
                .printlnExploration()
                .matchesExploration(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalJoin(
                                    logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1"))),
                                    logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                )
                            ).when(project -> project.getProjects().size() == 3), // t1.id Add t3.id, t3.name
                            logicalProject(
                                logicalProject(logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2")))
                            ).when(project -> project.getProjects().size() == 1)
                        )
                    )
                );
    }

    @Test
    void testAliasTopMultiHashJoin() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)) // t1.id=t2.id
                .alias(ImmutableList.of(0, 2), ImmutableList.of("t1.id", "t2.id"))
                // t1.id=t3.id t2.id = t3.id
                .join(scan3, JoinType.LEFT_OUTER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(1, 0)))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(OuterJoinLAsscomProject.INSTANCE.build())
                .printlnOrigin()
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }

    @Test
    void testAliasTopMultiHashJoinLeftOuterInner() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)) // t1.id=t2.id
                .alias(ImmutableList.of(0, 2), ImmutableList.of("t1.id", "t2.id"))
                // t1.id=t3.id t2.id=t3.id
                .join(scan3, JoinType.INNER_JOIN, ImmutableList.of(Pair.of(0, 0), Pair.of(1, 0)))
                .build();

        // transform failed.
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(OuterJoinLAsscomProject.INSTANCE.build())
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }

    @Test
    public void testHashAndOther() {
        List<Expression> bottomHashJoinConjunct = ImmutableList.of(
                new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0)));
        List<Expression> bottomOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(scan1.getOutput().get(1), scan2.getOutput().get(1)));
        LogicalPlan project = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, bottomHashJoinConjunct, bottomOtherJoinConjunct)
                .alias(ImmutableList.of(0, 1, 2, 3), ImmutableList.of("t1.id", "t1.name", "t2.id", "t2.name"))
                .build();

        List<Expression> topHashJoinConjunct = ImmutableList.of(
                new EqualTo(project.getOutput().get(0), scan3.getOutput().get(0)),
                new EqualTo(project.getOutput().get(2), scan3.getOutput().get(0)));
        List<Expression> topOtherJoinConjunct = ImmutableList.of(
                new GreaterThan(project.getOutput().get(1), scan3.getOutput().get(1)),
                new GreaterThan(project.getOutput().get(3), scan3.getOutput().get(1)));
        LogicalPlan plan = new LogicalPlanBuilder(project)
                .join(scan3, JoinType.LEFT_OUTER_JOIN, topHashJoinConjunct, topOtherJoinConjunct)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .printlnOrigin()
                .applyExploration(OuterJoinLAsscomProject.INSTANCE.build())
                .checkMemo(memo -> {
                    Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size());
                });
    }
}
