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
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

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

    /**
     * Test that isBucketShuffleDownGrade returns true when oneSidePlan is not a GroupPlan (line 284 change).
     * Previously this returned false (no downgrade). Now it returns true (conservative downgrade).
     * With NATURAL+NATURAL distribution on both sides, the left check is triggered and since
     * hashJoin.child(0) is a plain Plan mock (not GroupPlan), both sides must be downgraded to
     * EXECUTION_BUCKETED.
     */
    @Test
    public void testBucketShuffleDownGradeWhenOneSidePlanNotGroupPlan() {
        try (MockedStatic<CostCalculator> mockedCostCalculator = Mockito.mockStatic(CostCalculator.class);
                MockedStatic<ConnectContext> mockedConnectContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedCostCalculator.when(() -> CostCalculator.calculateCost(Mockito.any(), Mockito.any(),
                    Mockito.anyList())).thenReturn(Cost.zero());
            mockedCostCalculator.when(() -> CostCalculator.addChildCost(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(Cost.zero());

            ConnectContext connectContext = Mockito.mock(ConnectContext.class);
            SessionVariable sv = Mockito.mock(SessionVariable.class);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(sv);
            Mockito.when(sv.isEnableBucketShuffleJoin()).thenReturn(true);
            Mockito.when(sv.isDisableColocatePlan()).thenReturn(true);
            mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);

            ExprId leftExprId = new ExprId(0);
            ExprId rightExprId = new ExprId(1);

            DistributionSpecHash leftHashSpec = new DistributionSpecHash(
                    Lists.newArrayList(leftExprId), ShuffleType.NATURAL, 1L, Sets.newHashSet(100L));
            DistributionSpecHash rightHashSpec = new DistributionSpecHash(
                    Lists.newArrayList(rightExprId), ShuffleType.NATURAL, 2L, Sets.newHashSet(200L));

            PhysicalProperties leftOriginProp = new PhysicalProperties(leftHashSpec);
            PhysicalProperties rightOriginProp = new PhysicalProperties(rightHashSpec);
            List<PhysicalProperties> originProps = Lists.newArrayList(leftOriginProp, rightOriginProp);
            List<PhysicalProperties> requiredProps = Lists.newArrayList(
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(leftExprId), ShuffleType.NATURAL, 1L, Sets.newHashSet(100L))),
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(rightExprId), ShuffleType.NATURAL, 2L, Sets.newHashSet(200L))));

            // hashJoin.child(0) is NOT a GroupPlan — triggers the "not GroupPlan" branch in isBucketShuffleDownGrade
            Plan nonGroupPlanChild = Mockito.mock(Plan.class);
            Mockito.when(nonGroupPlanChild.getAllChildrenTypes()).thenReturn(new BitSet());

            // Right child of the hash join is a normal GroupPlan
            Group rightInnerGroup = Mockito.mock(Group.class);
            Mockito.when(rightInnerGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
            GroupPlan rightGroupPlan = new GroupPlan(rightInnerGroup);
            Mockito.when(rightInnerGroup.getGroupPlan()).thenReturn(rightGroupPlan);

            // Left GroupExpression for ChildrenPropertiesRegulator's children list
            Group leftChildGroup = Mockito.mock(Group.class);
            Mockito.when(leftChildGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
            GroupPlan leftChildGroupPlan = new GroupPlan(leftChildGroup);
            Mockito.when(leftChildGroup.getGroupPlan()).thenReturn(leftChildGroupPlan);
            GroupExpression leftChildGE = Mockito.mock(GroupExpression.class);
            Mockito.when(leftChildGE.getOwnerGroup()).thenReturn(leftChildGroup);
            Mockito.when(leftChildGE.getOutputProperties(Mockito.any())).thenReturn(leftOriginProp);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> leftLCT = Maps.newHashMap();
            leftLCT.put(leftOriginProp, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(leftChildGE.getLowestCostTable()).thenReturn(leftLCT);
            Mockito.when(leftChildGE.getPlan()).thenReturn(Mockito.mock(Plan.class));

            // Right GroupExpression for ChildrenPropertiesRegulator's children list
            GroupExpression rightChildGE = Mockito.mock(GroupExpression.class);
            Mockito.when(rightChildGE.getOwnerGroup()).thenReturn(rightInnerGroup);
            Mockito.when(rightChildGE.getOutputProperties(Mockito.any())).thenReturn(rightOriginProp);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> rightLCT = Maps.newHashMap();
            rightLCT.put(rightOriginProp, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(rightChildGE.getLowestCostTable()).thenReturn(rightLCT);
            Mockito.when(rightChildGE.getPlan()).thenReturn(Mockito.mock(Plan.class));

            // Construct hash join with nonGroupPlanChild as first child
            LogicalProperties logicalProps = Mockito.mock(LogicalProperties.class);
            PhysicalHashJoin<Plan, GroupPlan> hashJoin = new PhysicalHashJoin<>(
                    JoinType.INNER_JOIN,
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    new DistributeHint(DistributeType.NONE),
                    Optional.empty(),
                    logicalProps,
                    nonGroupPlanChild,
                    rightGroupPlan);

            GroupExpression parent = new GroupExpression(hashJoin);
            ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(
                    parent,
                    Lists.newArrayList(leftChildGE, rightChildGE),
                    originProps,
                    requiredProps,
                    mockedJobContext);

            List<List<PhysicalProperties>> result = regulator.adjustChildrenProperties();

            // Both sides should be downgraded to EXECUTION_BUCKETED because left child is not GroupPlan
            Assertions.assertEquals(1, result.size());
            Assertions.assertInstanceOf(DistributionSpecHash.class, result.get(0).get(0).getDistributionSpec());
            Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED,
                    ((DistributionSpecHash) result.get(0).get(0).getDistributionSpec()).getShuffleType());
        }
    }

    /**
     * Test that isBucketShuffleDownGrade returns true when the candidate OlapScan is null (line 289 change).
     * Previously this returned false (no downgrade). Now it returns true (conservative downgrade).
     * hashJoin.child(0) IS a GroupPlan, but its group's physical expression is a join node (not an
     * OlapScan), so findDownGradeBucketShuffleCandidate returns null and downgrade is triggered.
     */
    @Test
    public void testBucketShuffleDownGradeWhenCandidateIsNull() {
        try (MockedStatic<CostCalculator> mockedCostCalculator = Mockito.mockStatic(CostCalculator.class);
                MockedStatic<ConnectContext> mockedConnectContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedCostCalculator.when(() -> CostCalculator.calculateCost(Mockito.any(), Mockito.any(),
                    Mockito.anyList())).thenReturn(Cost.zero());
            mockedCostCalculator.when(() -> CostCalculator.addChildCost(Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(Cost.zero());

            ConnectContext connectContext = Mockito.mock(ConnectContext.class);
            SessionVariable sv = Mockito.mock(SessionVariable.class);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(sv);
            Mockito.when(sv.isEnableBucketShuffleJoin()).thenReturn(true);
            Mockito.when(sv.isDisableColocatePlan()).thenReturn(true);
            mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);

            ExprId leftExprId = new ExprId(0);
            ExprId rightExprId = new ExprId(1);

            DistributionSpecHash leftHashSpec = new DistributionSpecHash(
                    Lists.newArrayList(leftExprId), ShuffleType.NATURAL, 1L, Sets.newHashSet(100L));
            DistributionSpecHash rightHashSpec = new DistributionSpecHash(
                    Lists.newArrayList(rightExprId), ShuffleType.NATURAL, 2L, Sets.newHashSet(200L));

            PhysicalProperties leftOriginProp = new PhysicalProperties(leftHashSpec);
            PhysicalProperties rightOriginProp = new PhysicalProperties(rightHashSpec);
            List<PhysicalProperties> originProps = Lists.newArrayList(leftOriginProp, rightOriginProp);
            List<PhysicalProperties> requiredProps = Lists.newArrayList(
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(leftExprId), ShuffleType.NATURAL, 1L, Sets.newHashSet(100L))),
                    new PhysicalProperties(new DistributionSpecHash(
                            Lists.newArrayList(rightExprId), ShuffleType.NATURAL, 2L, Sets.newHashSet(200L))));

            // hashJoin.child(0) IS a GroupPlan, but its group's physical expression is a join (not OlapScan)
            // so findDownGradeBucketShuffleCandidate returns null → isBucketShuffleDownGrade returns true
            Group innerGroupWithJoin = Mockito.mock(Group.class);
            GroupExpression joinExpr = new GroupExpression(Mockito.mock(PhysicalHashJoin.class));
            Mockito.when(innerGroupWithJoin.getPhysicalExpressions()).thenReturn(
                    Lists.newArrayList(joinExpr));
            Mockito.when(innerGroupWithJoin.getLogicalProperties()).thenReturn(
                    Mockito.mock(LogicalProperties.class));
            GroupPlan leftGroupPlanWithJoin = new GroupPlan(innerGroupWithJoin);
            Mockito.when(innerGroupWithJoin.getGroupPlan()).thenReturn(leftGroupPlanWithJoin);

            // Right child of the hash join is a normal GroupPlan
            Group rightInnerGroup = Mockito.mock(Group.class);
            Mockito.when(rightInnerGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
            GroupPlan rightGroupPlan = new GroupPlan(rightInnerGroup);
            Mockito.when(rightInnerGroup.getGroupPlan()).thenReturn(rightGroupPlan);

            // Left GroupExpression for ChildrenPropertiesRegulator's children list
            GroupExpression leftChildGE = Mockito.mock(GroupExpression.class);
            Mockito.when(leftChildGE.getOwnerGroup()).thenReturn(innerGroupWithJoin);
            Mockito.when(leftChildGE.getOutputProperties(Mockito.any())).thenReturn(leftOriginProp);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> leftLCT = Maps.newHashMap();
            leftLCT.put(leftOriginProp, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(leftChildGE.getLowestCostTable()).thenReturn(leftLCT);
            Mockito.when(leftChildGE.getPlan()).thenReturn(Mockito.mock(Plan.class));

            // Right GroupExpression for ChildrenPropertiesRegulator's children list
            GroupExpression rightChildGE = Mockito.mock(GroupExpression.class);
            Mockito.when(rightChildGE.getOwnerGroup()).thenReturn(rightInnerGroup);
            Mockito.when(rightChildGE.getOutputProperties(Mockito.any())).thenReturn(rightOriginProp);
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> rightLCT = Maps.newHashMap();
            rightLCT.put(rightOriginProp, Pair.of(Cost.zero(), Lists.newArrayList()));
            Mockito.when(rightChildGE.getLowestCostTable()).thenReturn(rightLCT);
            Mockito.when(rightChildGE.getPlan()).thenReturn(Mockito.mock(Plan.class));

            // Construct hash join with leftGroupPlanWithJoin (GroupPlan wrapping a join) as first child
            LogicalProperties logicalProps = Mockito.mock(LogicalProperties.class);
            PhysicalHashJoin<GroupPlan, GroupPlan> hashJoin = new PhysicalHashJoin<>(
                    JoinType.INNER_JOIN,
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    new DistributeHint(DistributeType.NONE),
                    Optional.empty(),
                    logicalProps,
                    leftGroupPlanWithJoin,
                    rightGroupPlan);

            GroupExpression parent = new GroupExpression(hashJoin);
            ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(
                    parent,
                    Lists.newArrayList(leftChildGE, rightChildGE),
                    originProps,
                    requiredProps,
                    mockedJobContext);

            List<List<PhysicalProperties>> result = regulator.adjustChildrenProperties();

            // Both sides should be downgraded to EXECUTION_BUCKETED because no OlapScan candidate found
            Assertions.assertEquals(1, result.size());
            Assertions.assertInstanceOf(DistributionSpecHash.class, result.get(0).get(0).getDistributionSpec());
            Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED,
                    ((DistributionSpecHash) result.get(0).get(0).getDistributionSpec()).getShuffleType());
        }
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
