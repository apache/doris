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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
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

class PushDownProjectThroughInnerOuterJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void pushBothSide() {
        // project (t1.id + 1) as alias, t1.name, (t2.id + 1) as alias, t2.name
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), Literal.of(1)), "alias"),
                scan1.getOutput().get(1),
                new Alias(new Add(scan2.getOutput().get(0), Literal.of(1)), "alias"),
                scan2.getOutput().get(1)
        );
        // complex projection contain ti.id, which isn't in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(1, 1))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .printlnOrigin()
                .printlnExploration()
                .matchesExploration(
                        logicalJoin(
                                logicalProject(logicalJoin(
                                        logicalProject().when(project -> project.getProjects().size() == 2),
                                        logicalProject().when(project -> project.getProjects().size() == 2)
                                )),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    public void pushRightSide() {
        // project (t1.id + 1) as alias, t1.name, (t2.id + 1) as alias, t2.name
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), Literal.of(1)), "alias"),
                scan1.getOutput().get(1),
                scan2.getOutput().get(1)
        );
        // complex projection contain ti.id, which isn't in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(1, 1))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .printlnOrigin()
                .printlnExploration()
                .matchesExploration(
                        logicalJoin(
                                logicalProject(
                                    logicalJoin(
                                            logicalProject().when(project -> project.getProjects().size() == 2),
                                            logicalOlapScan()
                                    )
                                ),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    public void pushNoSide() {
        // project (t1.id + 1) as alias, t1.name, (t2.id + 1) as alias, t2.name
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), Literal.of(1)), "alias"),
                scan1.getOutput().get(1),
                scan2.getOutput().get(1)
        );
        // complex projection contain ti.id, which isn't in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.FULL_OUTER_JOIN, Pair.of(1, 1))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        int plansNumber = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .plansNumber();
        Assertions.assertEquals(1, plansNumber);
    }

    @Test
    public void pushdownProjectInCondition() {
        // project (t1.id + 1) as alias, t1.name, (t2.id + 1) as alias, t2.name
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), Literal.of(1)), "alias"),
                scan1.getOutput().get(1),
                new Alias(new Add(scan2.getOutput().get(0), Literal.of(1)), "alias"),
                scan2.getOutput().get(1)
        );
        // complex projection contain ti.id, which is in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .printlnOrigin()
                .printlnExploration()
                .matchesExploration(
                        logicalJoin(
                                logicalProject(
                                        logicalJoin(
                                                logicalProject().when(project -> project.getProjects().size() == 3),
                                                logicalProject().when(project -> project.getProjects().size() == 3)
                                        )
                                ),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void pushComplexProject() {
        // project (t1.id + t1.name) as complex1, (t2.id + t2.name) as complex2
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), scan1.getOutput().get(1)), "complex1"),
                new Alias(new Add(scan2.getOutput().get(0), scan2.getOutput().get(1)), "complex2")
        );
        // complex projection contain ti.id, which is in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .printlnOrigin()
                .printlnExploration()
                .matchesExploration(
                    logicalJoin(
                        logicalProject(
                            logicalJoin(
                                logicalProject()
                                    .when(project ->
                                            project.getProjects().get(0).toSql().equals("(id + name) AS `complex1`")
                                            && project.getProjects().get(1).toSql().equals("id")),
                                logicalProject()
                                    .when(project ->
                                            project.getProjects().get(0).toSql().equals("(id + name) AS `complex2`")
                                            && project.getProjects().get(1).toSql().equals("id"))
                            )
                        ).when(project -> project.getProjects().get(0).toSql().equals("complex1")
                                && project.getProjects().get(1).toSql().equals("complex2")),
                        logicalOlapScan()
                    )
                );
    }

    @Test
    void rejectHyperEdgeProject() {
        // project (t1.id + t2.id) as alias
        List<NamedExpression> projectExprs = ImmutableList.of(
                new Alias(new Add(scan1.getOutput().get(0), scan2.getOutput().get(0)), "alias")
        );
        // complex projection contain ti.id, which is in Join Condition
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .projectExprs(projectExprs)
                .join(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(PushDownProjectThroughInnerOuterJoin.INSTANCE.buildRules())
                .checkMemo(memo -> Assertions.assertEquals(1, memo.getRoot().getLogicalExpressions().size()));
    }
}
