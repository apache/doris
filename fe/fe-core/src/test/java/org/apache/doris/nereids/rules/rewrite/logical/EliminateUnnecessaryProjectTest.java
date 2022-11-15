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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * test ELIMINATE_UNNECESSARY_PROJECT rule.
 */
public class EliminateUnnecessaryProjectTest {

    @Test
    public void testEliminateNonTopUnnecessaryProject() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(1, 0))
                .filter(BooleanLiteral.FALSE)
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(unnecessaryProject);
        List<Rule> rules = Lists.newArrayList(new EliminateUnnecessaryProject().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual.child(0) instanceof LogicalOlapScan);
    }

    @Test
    public void testEliminateTopUnnecessaryProject() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(0, 1))
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(unnecessaryProject);
        List<Rule> rules = Lists.newArrayList(new EliminateUnnecessaryProject().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual instanceof LogicalOlapScan);
    }

    @Test
    public void testNotEliminateTopProjectWhenOutputNotEquals() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(1, 0))
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(unnecessaryProject);
        List<Rule> rules = Lists.newArrayList(new EliminateUnnecessaryProject().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual instanceof LogicalProject);
    }
}
