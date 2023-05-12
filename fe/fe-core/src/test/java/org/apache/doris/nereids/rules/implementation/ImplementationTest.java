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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "unused"})
public class ImplementationTest {
    private static final Map<String, Rule> rulesMap
            = ImmutableMap.<String, Rule>builder()
            .put(LogicalProject.class.getName(), (new LogicalProjectToPhysicalProject()).build())
            .put(LogicalJoin.class.getName(), (new LogicalJoinToHashJoin()).build())
            .put(LogicalOlapScan.class.getName(), (new LogicalOlapScanToPhysicalOlapScan()).build())
            .put(LogicalFilter.class.getName(), (new LogicalFilterToPhysicalFilter()).build())
            .put(LogicalSort.class.getName(), (new LogicalSortToPhysicalQuickSort()).build())
            .put(LogicalTopN.class.getName(), (new LogicalTopNToPhysicalTopN()).build())
            .put(LogicalLimit.class.getName(), (new LogicalLimitToPhysicalLimit()).build())
            .build();
    @Mocked
    private CascadesContext cascadesContext;
    @Mocked
    private GroupPlan groupPlan;

    public PhysicalPlan executeImplementationRule(LogicalPlan plan) {
        Rule rule = rulesMap.get(plan.getClass().getName());
        List<Plan> transform = rule.transform(plan, cascadesContext);
        Assertions.assertTrue(transform.get(0) instanceof PhysicalPlan);
        return (PhysicalPlan) transform.get(0);
    }

    @Test
    public void toPhysicalProjectTest() {
        SlotReference col1 = new SlotReference("col1", BigIntType.INSTANCE);
        SlotReference col2 = new SlotReference("col2", IntegerType.INSTANCE);
        LogicalPlan project = new LogicalProject<>(Lists.newArrayList(col1, col2), groupPlan);

        PhysicalPlan physicalPlan = executeImplementationRule(project);
        Assertions.assertEquals(PlanType.PHYSICAL_PROJECT, physicalPlan.getType());
        PhysicalProject<GroupPlan> physicalProject = (PhysicalProject<GroupPlan>) physicalPlan;
        Assertions.assertEquals(2, physicalProject.getExpressions().size());
        Assertions.assertEquals(col1, physicalProject.getExpressions().get(0));
        Assertions.assertEquals(col2, physicalProject.getExpressions().get(1));
    }

    @Test
    public void toPhysicalTopNTest() {
        int limit = 10;
        int offset = 100;
        OrderKey key1 = new OrderKey(new SlotReference("col1", IntegerType.INSTANCE), true, true);
        OrderKey key2 = new OrderKey(new SlotReference("col2", IntegerType.INSTANCE), true, true);
        LogicalTopN<GroupPlan> topN = new LogicalTopN<>(Lists.newArrayList(key1, key2), limit, offset, groupPlan);

        PhysicalPlan physicalPlan = executeImplementationRule(topN);
        Assertions.assertEquals(PlanType.PHYSICAL_TOP_N, physicalPlan.getType());
        PhysicalTopN<GroupPlan> physicalTopN = (PhysicalTopN<GroupPlan>) physicalPlan;
        Assertions.assertEquals(limit, physicalTopN.getLimit());
        Assertions.assertEquals(offset, physicalTopN.getOffset());
        Assertions.assertEquals(2, physicalTopN.getOrderKeys().size());
        Assertions.assertEquals(key1, physicalTopN.getOrderKeys().get(0));
        Assertions.assertEquals(key2, physicalTopN.getOrderKeys().get(1));
    }

    @Test
    public void toPhysicalLimitTest() {
        int limit = 10;
        int offset = 100;
        LogicalLimit<? extends Plan> logicalLimit = new LogicalLimit<>(limit, offset, LimitPhase.LOCAL, groupPlan);
        PhysicalPlan physicalPlan = executeImplementationRule(logicalLimit);
        Assertions.assertEquals(PlanType.PHYSICAL_LIMIT, physicalPlan.getType());
        PhysicalLimit<GroupPlan> physicalLimit = (PhysicalLimit<GroupPlan>) physicalPlan;
        Assertions.assertEquals(limit, physicalLimit.getLimit());
        Assertions.assertEquals(offset, physicalLimit.getOffset());
    }
}
