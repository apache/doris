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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LogicalProjectToPhysicalProjectTest {
    private final Map<String, Rule> rulesMap
            = ImmutableMap.<String, Rule>builder()
            .put(LogicalProject.class.getName(), (new LogicalProjectToPhysicalProject()).build())
            .put(LogicalAggregate.class.getName(), (new LogicalAggToPhysicalHashAgg()).build())
            .put(LogicalJoin.class.getName(), (new LogicalJoinToHashJoin()).build())
            .put(LogicalOlapScan.class.getName(), (new LogicalOlapScanToPhysicalOlapScan()).build())
            .put(LogicalFilter.class.getName(), (new LogicalFilterToPhysicalFilter()).build())
            .put(LogicalSort.class.getName(), (new LogicalSortToPhysicalHeapSort()).build())
            .build();

    private PhysicalPlan rewriteLogicalToPhysical(Group group, PlannerContext plannerContext) {
        List<Plan> children = Lists.newArrayList();
        for (Group child : group.getLogicalExpression().children()) {
            children.add(rewriteLogicalToPhysical(child, plannerContext));
        }

        Rule rule = rulesMap.get(group.getLogicalExpression().getPlan().getClass().getName());
        List<Plan> transform = rule.transform(group.getLogicalExpression().getPlan(), plannerContext);
        Assertions.assertEquals(1, transform.size());
        Assertions.assertTrue(transform.get(0) instanceof PhysicalPlan);
        PhysicalPlan implPlanNode = (PhysicalPlan) transform.get(0);

        return (PhysicalPlan) implPlanNode.withChildren(children);
    }

    @Test
    public void projectionImplTest() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan("a");
        LogicalPlan project = new LogicalProject<>(Lists.newArrayList(), scan);

        PlannerContext plannerContext = new Memo(project)
                .newPlannerContext(new ConnectContext())
                .setDefaultJobContext();

        PhysicalPlan physicalProject = rewriteLogicalToPhysical(plannerContext.getMemo().getRoot(), plannerContext);
        Assertions.assertEquals(PlanType.PHYSICAL_PROJECT, physicalProject.getType());
        PhysicalPlan physicalScan = (PhysicalPlan) physicalProject.child(0);
        Assertions.assertEquals(PlanType.PHYSICAL_OLAP_SCAN, physicalScan.getType());
    }
}
