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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ChildrenPropertiesRegulatorTest {

    private JobContext mockedJobContext;
    private List<PhysicalProperties> originOutputChildrenProperties
            = Lists.newArrayList(PhysicalProperties.MUST_SHUFFLE);

    @BeforeEach
    public void setUp() {
        mockedJobContext = Mockito.mock(JobContext.class);
        Mockito.when(mockedJobContext.getCascadesContext()).thenReturn(Mockito.mock(CascadesContext.class));
    }

    @Test
    public void testMustShuffleProjectProjectCanNotMerge() {
        testMustShuffleProject(PhysicalProject.class, DistributionSpecExecutionAny.class, false);
    }

    @Test
    public void testMustShuffleProjectProjectCanMerge() {
        testMustShuffleProject(PhysicalProject.class, DistributionSpecMustShuffle.class, true);
    }

    @Test
    public void testMustShuffleProjectFilter() {
        testMustShuffleProject(PhysicalFilter.class, DistributionSpecMustShuffle.class, true);
    }

    @Test
    public void testMustShuffleProjectLimit() {
        testMustShuffleProject(PhysicalLimit.class, DistributionSpecExecutionAny.class, true);
    }

    public void testMustShuffleProject(Class<? extends Plan> childClazz,
            Class<? extends DistributionSpec> distributeClazz,
            boolean canMergeChildProject) {
        try (MockedStatic<CostCalculator> mockedCostCalculator = Mockito.mockStatic(CostCalculator.class)) {
            mockedCostCalculator.when(() -> CostCalculator.calculateCost(Mockito.any(), Mockito.any(),
                    Mockito.anyList())).thenReturn(Cost.zero());
            mockedCostCalculator.when(() -> CostCalculator.addChildCost(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.anyInt())).thenReturn(Cost.zero());

            // project, cannot merge
            Plan mockedChild = Mockito.mock(childClazz);
            Mockito.when(mockedChild.withGroupExpression(Mockito.any())).thenReturn(mockedChild);
            Group mockedGroup = Mockito.mock(Group.class);
            List<GroupExpression> physicalExpressions = Lists.newArrayList(new GroupExpression(mockedChild));
            Mockito.when(mockedGroup.getPhysicalExpressions()).thenReturn(physicalExpressions);
            GroupPlan mockedGroupPlan = Mockito.mock(GroupPlan.class);
            Mockito.when(mockedGroupPlan.getGroup()).thenReturn(mockedGroup);
            // let AbstractTreeNode's init happy
            Mockito.when(mockedGroupPlan.getAllChildrenTypes()).thenReturn(new BitSet());

            List<GroupExpression> children;
            Group childGroup = Mockito.mock(Group.class);
            Mockito.when(childGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
            GroupPlan childGroupPlan = new GroupPlan(childGroup);
            Mockito.when(childGroup.getGroupPlan()).thenReturn(childGroupPlan);
            GroupExpression child = Mockito.mock(GroupExpression.class);
            Mockito.when(child.getOutputProperties(Mockito.any())).thenReturn(PhysicalProperties.MUST_SHUFFLE);
            Mockito.when(child.getOwnerGroup()).thenReturn(childGroup);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lct = Maps.newHashMap();
            lct.put(PhysicalProperties.MUST_SHUFFLE, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(child.getLowestCostTable()).thenReturn(lct);
            Mockito.when(child.getPlan()).thenReturn(mockedChild);
            children = Lists.newArrayList(child);

            PhysicalProject parentPlan = new PhysicalProject<>(Lists.newArrayList(), null, mockedGroupPlan);
            GroupExpression parent = new GroupExpression(parentPlan);
            parentPlan = parentPlan.withGroupExpression(Optional.of(parent));
            parentPlan = Mockito.spy(parentPlan);
            Mockito.doReturn(canMergeChildProject).when(parentPlan).canMergeChildProjections(Mockito.any());
            parent = Mockito.spy(parent);
            Mockito.doReturn(parentPlan).when(parent).getPlan();
            ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent, children,
                    new ArrayList<>(originOutputChildrenProperties), null, mockedJobContext);
            PhysicalProperties result = regulator.adjustChildrenProperties().get(0).get(0);
            Assertions.assertInstanceOf(distributeClazz, result.getDistributionSpec());
        }
    }

    @Test
    public void testMustShuffleFilterProject() {
        testMustShuffleFilter(PhysicalProject.class);
    }

    @Test
    public void testMustShuffleFilterFilter() {
        testMustShuffleFilter(PhysicalFilter.class);
    }

    @Test
    public void testMustShuffleFilterLimit() {
        testMustShuffleFilter(PhysicalLimit.class);
    }

    private void testMustShuffleFilter(Class<? extends Plan> childClazz) {
        try (MockedStatic<CostCalculator> mockedCostCalculator = Mockito.mockStatic(CostCalculator.class)) {
            mockedCostCalculator.when(() -> CostCalculator.calculateCost(Mockito.any(), Mockito.any(),
                    Mockito.anyList())).thenReturn(Cost.zero());
            mockedCostCalculator.when(() -> CostCalculator.addChildCost(Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.anyInt())).thenReturn(Cost.zero());

            // project, cannot merge
            Plan mockedChild = Mockito.mock(childClazz);
            Mockito.when(mockedChild.withGroupExpression(Mockito.any())).thenReturn(mockedChild);
            Group mockedGroup = Mockito.mock(Group.class);
            List<GroupExpression> physicalExpressions = Lists.newArrayList(new GroupExpression(mockedChild));
            Mockito.when(mockedGroup.getPhysicalExpressions()).thenReturn(physicalExpressions);
            GroupPlan mockedGroupPlan = Mockito.mock(GroupPlan.class);
            Mockito.when(mockedGroupPlan.getGroup()).thenReturn(mockedGroup);
            // let AbstractTreeNode's init happy
            Mockito.when(mockedGroupPlan.getAllChildrenTypes()).thenReturn(new BitSet());

            List<GroupExpression> children;
            Group childGroup = Mockito.mock(Group.class);
            Mockito.when(childGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
            GroupPlan childGroupPlan = new GroupPlan(childGroup);
            Mockito.when(childGroup.getGroupPlan()).thenReturn(childGroupPlan);
            GroupExpression child = Mockito.mock(GroupExpression.class);
            Mockito.when(child.getOutputProperties(Mockito.any())).thenReturn(PhysicalProperties.MUST_SHUFFLE);
            Mockito.when(child.getOwnerGroup()).thenReturn(childGroup);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lct = Maps.newHashMap();
            lct.put(PhysicalProperties.MUST_SHUFFLE, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(child.getLowestCostTable()).thenReturn(lct);
            Mockito.when(child.getPlan()).thenReturn(mockedChild);
            children = Lists.newArrayList(child);

            GroupExpression parent = new GroupExpression(new PhysicalFilter<>(Sets.newHashSet(), null, mockedGroupPlan));
            ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent, children,
                    new ArrayList<>(originOutputChildrenProperties), null, mockedJobContext);
            PhysicalProperties result = regulator.adjustChildrenProperties().get(0).get(0);
            Assertions.assertInstanceOf(DistributionSpecExecutionAny.class, result.getDistributionSpec());
        }
    }
}
