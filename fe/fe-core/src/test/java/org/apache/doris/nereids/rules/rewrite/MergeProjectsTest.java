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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Objects;

/**
 * MergeConsecutiveProjects ut
 */
public class MergeProjectsTest implements MemoPatternMatchSupported {
    LogicalOlapScan score = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);

    @Test
    public void testMergeConsecutiveProjects() {
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .project(ImmutableList.of(0, 1, 2))
                .project(ImmutableList.of(0, 1))
                .project(ImmutableList.of(0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new MergeProjects())
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(project -> project.getProjects().size() == 1)
                );
    }

    /**
     * project2(X + 2) -> project1(B, C, A+1 as X)
     * transform to :
     * project2((A + 1) + 2)
     */
    @Test
    public void testMergeConsecutiveProjectsWithAlias() {
        Alias alias = new Alias(new Add(score.getOutput().get(0), Literal.of(1)), "X");
        LogicalProject<LogicalOlapScan> bottomProject = new LogicalProject<>(
                Lists.newArrayList(score.getOutput().get(1), score.getOutput().get(2), alias),
                score);

        LogicalProject<LogicalProject<LogicalOlapScan>> topProject = new LogicalProject<>(
                Lists.newArrayList(
                        new Alias(new Add(bottomProject.getOutput().get(2), Literal.of(2)), "Y")),
                bottomProject);

        PlanChecker.from(MemoTestUtils.createConnectContext(), topProject)
                .applyTopDown(new MergeProjects())
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(project -> Objects.equals(project.getProjects().toString(),
                                "[((sid#0 + 1) + 2) AS `Y`#4]"))
                );
    }

    @Test
    void testAlias() {
        // project(b+1 as c)
        // -> project(a+1 as b)
        LogicalProject<LogicalOlapScan> bottomProject = new LogicalProject<>(
                ImmutableList.of(new Alias(new Add(score.getOutput().get(0), Literal.of(1)), "b")), score);
        LogicalProject<LogicalProject<LogicalOlapScan>> topProject = new LogicalProject<>(
                ImmutableList.of(new Alias(new Add(bottomProject.getOutput().get(0), Literal.of(1)), "b")),
                bottomProject);
        PlanChecker.from(MemoTestUtils.createConnectContext(), topProject)
                .applyTopDown(new MergeProjects())
                .matches(
                        logicalProject(
                                logicalOlapScan()
                        ).when(project -> Objects.equals(project.getProjects().get(0).toSql(),
                                "((sid + 1) + 1) AS `b`"))
                );
    }
}
