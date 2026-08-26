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
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableMap;
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
    private CascadesContext mockedCascadesContext;
    private List<PhysicalProperties> originOutputChildrenProperties
            = Lists.newArrayList(PhysicalProperties.MUST_SHUFFLE);

    private enum ActualShuffleShape {
        SINGLE,
        OVERLAPPING_EQUIVALENCE,
        EQUIVALENT_REPLACEMENT
    }

    private enum RequiredShuffleShape {
        PARENT_SINGLE_KEY,
        PARENT_TWO_KEYS,
        FULL_GROUP_BY
    }

    private static class ShuffleAdjustment {
        private final PhysicalProperties properties;
        private final List<ExprId> requiredKeys;
        private final List<ExprId> fullKeys;

        private ShuffleAdjustment(PhysicalProperties properties,
                List<ExprId> requiredKeys, List<ExprId> fullKeys) {
            this.properties = properties;
            this.requiredKeys = requiredKeys;
            this.fullKeys = fullKeys;
        }
    }

    @BeforeEach
    public void setUp() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setBeNumberForTest(3);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        mockedCascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(mockedCascadesContext.getConnectContext()).thenReturn(connectContext);
        mockedJobContext = Mockito.mock(JobContext.class);
        Mockito.when(mockedJobContext.getCascadesContext()).thenReturn(mockedCascadesContext);
        Mockito.when(mockedJobContext.getRequiredProperties()).thenReturn(PhysicalProperties.ANY);
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

    @Test
    public void testSingleExecutionInstanceAllowsOnePhaseAggWithDistribute() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setBeNumberForTest(1);
        ctx.getSessionVariable().parallelPipelineTaskNum = 1;
        Mockito.when(mockedCascadesContext.getConnectContext()).thenReturn(ctx);

        GroupPlan mockedGroupPlan = Mockito.mock(GroupPlan.class);
        Mockito.when(mockedGroupPlan.getAllChildrenTypes()).thenReturn(new BitSet());
        Mockito.when(mockedGroupPlan.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
        PhysicalDistribute<GroupPlan> distribute = new PhysicalDistribute<>(
                DistributionSpecGather.INSTANCE, mockedGroupPlan);
        GroupExpression child = new GroupExpression(distribute);
        SlotReference output = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(), Lists.<NamedExpression>newArrayList(output),
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT),
                false, null, false, mockedGroupPlan);
        GroupExpression parent = new GroupExpression(aggregate);

        ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent,
                Lists.newArrayList(child), Lists.newArrayList(PhysicalProperties.GATHER),
                Lists.newArrayList(PhysicalProperties.GATHER), mockedJobContext);
        Assertions.assertFalse(regulator.adjustChildrenProperties().isEmpty());
    }

    @Test
    public void testDistinctGlobalAggWithDistributeIsNotBannedAsOnePhase() {
        GroupPlan aggregateChild = Mockito.mock(GroupPlan.class);
        Mockito.when(aggregateChild.getAllChildrenTypes()).thenReturn(new BitSet());
        SlotReference output = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(), Lists.<NamedExpression>newArrayList(output),
                new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT),
                false, null, false, aggregateChild);
        GroupExpression parent = new GroupExpression(aggregate);

        GroupExpression child = Mockito.mock(GroupExpression.class);
        Mockito.when(child.getPlan()).thenReturn(Mockito.mock(PhysicalDistribute.class));
        Group distributeInput = Mockito.mock(Group.class);
        Mockito.when(child.children()).thenReturn(Lists.newArrayList(distributeInput));
        GroupExpression nonCteInput = Mockito.mock(GroupExpression.class);
        Mockito.when(nonCteInput.getPlan()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(distributeInput.getPhysicalExpressions()).thenReturn(Lists.newArrayList(nonCteInput));

        ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent,
                Lists.newArrayList(child), Lists.newArrayList(PhysicalProperties.GATHER),
                Lists.newArrayList(PhysicalProperties.GATHER), mockedJobContext);
        Assertions.assertFalse(regulator.adjustChildrenProperties().isEmpty());
    }

    @Test
    public void testLowNdvSkewBansOnePhaseAggWithoutHotValueStats() {
        SlotReference key = new SlotReference("key", IntegerType.INSTANCE);
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(key, new ColumnStatisticBuilder(10000)
                        .setNdv(1)
                        .build())
                .build();
        Statistics aggregateStatistics = new StatisticsBuilder()
                .setRowCount(1)
                .build();

        Assertions.assertTrue(adjustOnePhaseAggWithCte(
                key, inputStatistics, aggregateStatistics).isEmpty());
    }

    @Test
    public void testHotNullSkewBansOnePhaseAggWithoutHotValueStats() {
        SlotReference key = new SlotReference("key", IntegerType.INSTANCE);
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(key, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .setNumNulls(5000)
                        .build())
                .build();
        Statistics aggregateStatistics = new StatisticsBuilder()
                .setRowCount(2000)
                .build();

        Assertions.assertTrue(adjustOnePhaseAggWithCte(
                key, inputStatistics, aggregateStatistics).isEmpty());
    }

    @Test
    public void testRoundedZeroHotValueDoesNotBanOnePhaseAgg() {
        SlotReference key = new SlotReference("key", IntegerType.INSTANCE);
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(1_000_000_000)
                .putColumnStatistics(key, new ColumnStatisticBuilder(1_000_000_000)
                        .setNdv(2_000_000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.0f))
                        .build())
                .build();
        Statistics aggregateStatistics = new StatisticsBuilder()
                .setRowCount(2_000_000)
                .build();

        Assertions.assertFalse(adjustOnePhaseAggWithCte(
                key, inputStatistics, aggregateStatistics, 3000).isEmpty());
    }

    @Test
    public void testNullableOtherKeyDoesNotProveOnePhaseAggSkew() {
        SlotReference hotKey = new SlotReference("hot_key", IntegerType.INSTANCE);
        SlotReference nullableKey = new SlotReference("nullable_key", IntegerType.INSTANCE);
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(1_000_000)
                .putColumnStatistics(hotKey, new ColumnStatisticBuilder(1_000_000)
                        .setNdv(2000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.2f))
                        .build())
                .putColumnStatistics(nullableKey, new ColumnStatisticBuilder(1_000_000)
                        .setNdv(1024)
                        .setNumNulls(500_000)
                        .setHotValues(ImmutableMap.of())
                        .build())
                .build();
        Statistics aggregateStatistics = new StatisticsBuilder()
                .setRowCount(2000)
                .build();

        Assertions.assertFalse(adjustOnePhaseAggWithCte(
                Lists.newArrayList(hotKey, nullableKey), inputStatistics, aggregateStatistics, 3).isEmpty());
    }

    @Test
    public void testConfiguredHotValueThresholdControlsOnePhaseAgg() {
        SlotReference key = new SlotReference("key", IntegerType.INSTANCE);
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(10000)
                .putColumnStatistics(key, new ColumnStatisticBuilder(10000)
                        .setNdv(2000)
                        .setHotValues(ImmutableMap.of(Literal.of(1), 0.06f))
                        .build())
                .build();
        Statistics aggregateStatistics = new StatisticsBuilder()
                .setRowCount(2000)
                .build();

        Assertions.assertTrue(adjustOnePhaseAggWithCte(
                key, inputStatistics, aggregateStatistics).isEmpty());
        Assertions.assertFalse(adjustOnePhaseAggWithCte(
                Lists.newArrayList(key), inputStatistics, aggregateStatistics,
                3, 0.2, 1000).isEmpty());
    }

    @Test
    public void testUnsafeActualSubsetFallsBackToFullGroupBy() {
        PhysicalProperties result = adjustParentShuffleReuse(100, 3000, false);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testUnsafeActualSubsetWithFullParentRequirementIsEnforced() {
        PhysicalProperties result = adjustParentShuffleReuse(100, 3000, true);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testBalancedActualSubsetForParentShuffleIsReused() {
        PhysicalProperties result = adjustParentShuffleReuse(2000, 3, false);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, resultHash.getShuffleType());
        Assertions.assertEquals(1, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testParentShuffleReuseAllowsJoinDerivedKeyWithUnknownHotValues() {
        double rowCount = 2_870_000_000D;
        ColumnStatistic joinDerivedKey = new ColumnStatisticBuilder(rowCount)
                .setNdv(10_000_000)
                .setNumNulls(67_000_000)
                .build();

        PhysicalProperties result = adjustParentShuffleReuse(
                joinDerivedKey, 10_000_000, 10_000_000, 100,
                RequiredShuffleShape.PARENT_SINGLE_KEY, ActualShuffleShape.SINGLE);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, resultHash.getShuffleType());
        Assertions.assertEquals(1, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testFurtherSubsetStillRequiresKnownHotValueStatistics() {
        double rowCount = 2_870_000_000D;
        ColumnStatistic joinDerivedKey = new ColumnStatisticBuilder(rowCount)
                .setNdv(10_000_000)
                .setNumNulls(67_000_000)
                .build();

        PhysicalProperties result = adjustParentShuffleReuse(
                joinDerivedKey, 10_000_000, 10_000_000, 100,
                RequiredShuffleShape.PARENT_TWO_KEYS, ActualShuffleShape.SINGLE);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testUnsafeFurtherSubsetFallsBackToSafeRequiredKeys() {
        ShuffleAdjustment adjustment = adjustParentShuffleReuseWithKeys(
                100, 2_000_000, 2_000_000, 3,
                RequiredShuffleShape.PARENT_TWO_KEYS, ActualShuffleShape.SINGLE,
                AggPhase.GLOBAL);

        PhysicalProperties result = adjustment.properties;
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(adjustment.requiredKeys, resultHash.getOrderedShuffledColumns());
    }

    @Test
    public void testForcedOnePhasePreservesSatisfyingSubset() {
        PhysicalProperties result = adjustParentShuffleReuse(
                100, 100, 100, 3, RequiredShuffleShape.FULL_GROUP_BY,
                ActualShuffleShape.SINGLE,
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), 1);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, resultHash.getShuffleType());
        Assertions.assertEquals(1, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testDistinctGlobalSkipsOrdinaryGlobalSubsetRegulation() {
        PhysicalProperties result = adjustParentShuffleReuse(
                100, 100, 100, 3, RequiredShuffleShape.FULL_GROUP_BY,
                ActualShuffleShape.SINGLE, AggPhase.DISTINCT_GLOBAL);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, resultHash.getShuffleType());
        Assertions.assertEquals(1, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testOverlappingEquivalentActualKeysFallBackToRequiredDimensions() {
        ShuffleAdjustment adjustment = adjustParentShuffleReuseWithKeys(
                1000, 1000, 1000, 3,
                RequiredShuffleShape.PARENT_TWO_KEYS, ActualShuffleShape.OVERLAPPING_EQUIVALENCE,
                AggPhase.GLOBAL);

        PhysicalProperties result = adjustment.properties;
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(adjustment.requiredKeys, resultHash.getOrderedShuffledColumns());
    }

    @Test
    public void testOverlappingEquivalentActualKeysDoNotMultiplyNdv() {
        PhysicalProperties result = adjustParentShuffleReuse(1000, 3, true, true);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testOverlappingEquivalentActualKeysUseConservativeMemberStatistics() {
        PhysicalProperties result = adjustParentShuffleReuse(2_000_000, 100, 3, true, true);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    @Test
    public void testEquivalentReplacementOfReducedRequirementUsesActualMemberStatistics() {
        PhysicalProperties result = adjustParentShuffleReuse(
                2_000_000, 2_000_000, 10, 3, false, ActualShuffleShape.EQUIVALENT_REPLACEMENT);

        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash resultHash = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, resultHash.getShuffleType());
        Assertions.assertEquals(3, resultHash.getOrderedShuffledColumns().size());
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double actualKeyNdv, int beNumber, boolean parentRequiresAllGroupByKeys) {
        return adjustParentShuffleReuse(actualKeyNdv, actualKeyNdv, actualKeyNdv,
                beNumber, parentRequiresAllGroupByKeys, ActualShuffleShape.SINGLE);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double actualKeyNdv, int beNumber, boolean parentRequiresAllGroupByKeys,
            boolean overlappingEquivalentActualKeys) {
        return adjustParentShuffleReuse(actualKeyNdv, actualKeyNdv, actualKeyNdv,
                beNumber, parentRequiresAllGroupByKeys, overlappingEquivalentActualKeys
                        ? ActualShuffleShape.OVERLAPPING_EQUIVALENCE : ActualShuffleShape.SINGLE);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double key1Ndv, double key2Ndv, int beNumber, boolean parentRequiresAllGroupByKeys,
            boolean overlappingEquivalentActualKeys) {
        return adjustParentShuffleReuse(key1Ndv, key2Ndv, key2Ndv,
                beNumber, parentRequiresAllGroupByKeys, overlappingEquivalentActualKeys
                        ? ActualShuffleShape.OVERLAPPING_EQUIVALENCE : ActualShuffleShape.SINGLE);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double key1Ndv, double key2Ndv, double equivalentKeyNdv, int beNumber,
            boolean parentRequiresAllGroupByKeys, ActualShuffleShape actualShuffleShape) {
        double rowCount = 1_000_000_000D;
        ColumnStatistic key1Statistic = new ColumnStatisticBuilder(rowCount)
                .setNdv(key1Ndv)
                .setHotValues(Maps.newHashMap())
                .build();
        RequiredShuffleShape requiredShuffleShape = parentRequiresAllGroupByKeys
                ? RequiredShuffleShape.FULL_GROUP_BY : RequiredShuffleShape.PARENT_TWO_KEYS;
        return adjustParentShuffleReuse(key1Statistic, key2Ndv, equivalentKeyNdv,
                beNumber, requiredShuffleShape, actualShuffleShape);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            ColumnStatistic key1Statistic, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape) {
        return adjustParentShuffleReuse(key1Statistic, key2Ndv, equivalentKeyNdv, beNumber,
                requiredShuffleShape, actualShuffleShape,
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT), 0);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double key1Ndv, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape,
            AggPhase aggPhase) {
        return adjustParentShuffleReuseWithKeys(key1Ndv, key2Ndv, equivalentKeyNdv, beNumber,
                requiredShuffleShape, actualShuffleShape, aggPhase).properties;
    }

    private PhysicalProperties adjustParentShuffleReuse(
            double key1Ndv, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape,
            AggregateParam aggregateParam, int sessionAggPhase) {
        double rowCount = 1_000_000_000D;
        ColumnStatistic key1Statistic = new ColumnStatisticBuilder(rowCount)
                .setNdv(key1Ndv)
                .setHotValues(Maps.newHashMap())
                .build();
        return adjustParentShuffleReuse(key1Statistic, key2Ndv, equivalentKeyNdv, beNumber,
                requiredShuffleShape, actualShuffleShape, aggregateParam, sessionAggPhase);
    }

    private PhysicalProperties adjustParentShuffleReuse(
            ColumnStatistic key1Statistic, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape,
            AggregateParam aggregateParam, int sessionAggPhase) {
        return adjustParentShuffleReuseWithKeys(
                key1Statistic, key2Ndv, equivalentKeyNdv, beNumber,
                requiredShuffleShape, actualShuffleShape, aggregateParam, sessionAggPhase).properties;
    }

    private ShuffleAdjustment adjustParentShuffleReuseWithKeys(
            double key1Ndv, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape,
            AggPhase aggPhase) {
        double rowCount = 1_000_000_000D;
        ColumnStatistic key1Statistic = new ColumnStatisticBuilder(rowCount)
                .setNdv(key1Ndv)
                .setHotValues(Maps.newHashMap())
                .build();
        return adjustParentShuffleReuseWithKeys(
                key1Statistic, key2Ndv, equivalentKeyNdv, beNumber,
                requiredShuffleShape, actualShuffleShape,
                new AggregateParam(aggPhase, AggMode.BUFFER_TO_RESULT), 0);
    }

    private ShuffleAdjustment adjustParentShuffleReuseWithKeys(
            ColumnStatistic key1Statistic, double key2Ndv, double equivalentKeyNdv, int beNumber,
            RequiredShuffleShape requiredShuffleShape, ActualShuffleShape actualShuffleShape,
            AggregateParam aggregateParam, int sessionAggPhase) {
        SlotReference key1 = new SlotReference("key1", IntegerType.INSTANCE);
        SlotReference key2 = new SlotReference("key2", IntegerType.INSTANCE);
        SlotReference key3 = new SlotReference("key3", IntegerType.INSTANCE);
        SlotReference equivalentKey1 = new SlotReference("equivalent_key1", IntegerType.INSTANCE);
        SlotReference equivalentKey2 = new SlotReference("equivalent_key2", IntegerType.INSTANCE);
        double rowCount = key1Statistic.count;
        Statistics inputStatistics = new StatisticsBuilder()
                .setRowCount(rowCount)
                .putColumnStatistics(key1, key1Statistic)
                .putColumnStatistics(key2, new ColumnStatisticBuilder(rowCount)
                        .setNdv(key2Ndv)
                        .setHotValues(Maps.newHashMap())
                        .build())
                .putColumnStatistics(equivalentKey1, new ColumnStatisticBuilder(rowCount)
                        .setNdv(equivalentKeyNdv)
                        .setHotValues(Maps.newHashMap())
                        .build())
                .putColumnStatistics(equivalentKey2, new ColumnStatisticBuilder(rowCount)
                        .setNdv(equivalentKeyNdv)
                        .setHotValues(Maps.newHashMap())
                        .build())
                .build();

        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().setBeNumberForTest(beNumber);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().aggPhase = sessionAggPhase;
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);
        JobContext jobContext = Mockito.mock(JobContext.class);
        Mockito.when(jobContext.getCascadesContext()).thenReturn(cascadesContext);
        List<ExprId> parentRequiredKeys = Lists.newArrayList(key1.getExprId());
        if (requiredShuffleShape != RequiredShuffleShape.PARENT_SINGLE_KEY) {
            parentRequiredKeys.add(key2.getExprId());
        }
        if (requiredShuffleShape == RequiredShuffleShape.FULL_GROUP_BY) {
            parentRequiredKeys.add(key3.getExprId());
        }
        Mockito.when(jobContext.getRequiredProperties()).thenReturn(
                PhysicalProperties.createHash(parentRequiredKeys, ShuffleType.REQUIRE));

        GroupPlan aggregateChild = Mockito.mock(GroupPlan.class);
        Mockito.when(aggregateChild.getAllChildrenTypes()).thenReturn(new BitSet());
        Mockito.when(aggregateChild.getOutput()).thenReturn(
                Lists.newArrayList(key1, key2, key3, equivalentKey1, equivalentKey2));
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key1, key2, key3), Lists.newArrayList(key1, key2, key3),
                aggregateParam,
                true, Mockito.mock(LogicalProperties.class), false, aggregateChild);
        GroupExpression parent = Mockito.mock(GroupExpression.class);
        Mockito.when(parent.getPlan()).thenReturn(aggregate);
        Mockito.when(parent.childStatistics(0)).thenReturn(inputStatistics);

        PhysicalProperties requiredChildProperty = PhysicalProperties.createHash(
                parentRequiredKeys, ShuffleType.REQUIRE);
        DistributionSpecHash actualChildHash;
        if (actualShuffleShape == ActualShuffleShape.OVERLAPPING_EQUIVALENCE) {
            Map<ExprId, Integer> exprIdToEquivalenceSet = Maps.newHashMap();
            exprIdToEquivalenceSet.put(key1.getExprId(), 0);
            exprIdToEquivalenceSet.put(key2.getExprId(), 1);
            exprIdToEquivalenceSet.put(equivalentKey1.getExprId(), 1);
            actualChildHash = new DistributionSpecHash(
                    Lists.newArrayList(key1.getExprId(), key2.getExprId()), ShuffleType.NATURAL,
                    -1L, Sets.newHashSet(),
                    Lists.newArrayList(
                            Sets.newHashSet(key1.getExprId(), equivalentKey1.getExprId()),
                            Sets.newHashSet(key2.getExprId(), equivalentKey1.getExprId())),
                    exprIdToEquivalenceSet);
        } else if (actualShuffleShape == ActualShuffleShape.EQUIVALENT_REPLACEMENT) {
            Map<ExprId, Integer> exprIdToEquivalenceSet = Maps.newHashMap();
            exprIdToEquivalenceSet.put(key1.getExprId(), 0);
            exprIdToEquivalenceSet.put(equivalentKey1.getExprId(), 0);
            exprIdToEquivalenceSet.put(key2.getExprId(), 1);
            exprIdToEquivalenceSet.put(equivalentKey2.getExprId(), 1);
            actualChildHash = new DistributionSpecHash(
                    Lists.newArrayList(equivalentKey1.getExprId(), equivalentKey2.getExprId()), ShuffleType.NATURAL,
                    -1L, Sets.newHashSet(),
                    Lists.newArrayList(
                            Sets.newHashSet(key1.getExprId(), equivalentKey1.getExprId()),
                            Sets.newHashSet(key2.getExprId(), equivalentKey2.getExprId())),
                    exprIdToEquivalenceSet);
        } else {
            actualChildHash = new DistributionSpecHash(
                    Lists.newArrayList(key1.getExprId()), ShuffleType.NATURAL);
        }
        PhysicalProperties actualChildProperty = PhysicalProperties.createHash(actualChildHash);

        Group childGroup = Mockito.mock(Group.class);
        Mockito.when(childGroup.getLogicalProperties()).thenReturn(Mockito.mock(LogicalProperties.class));
        GroupPlan childGroupPlan = new GroupPlan(childGroup);
        Mockito.when(childGroup.getGroupPlan()).thenReturn(childGroupPlan);
        Mockito.when(childGroup.getEnforcerSpecs()).thenReturn(Maps.newHashMap());
        GroupExpression child = Mockito.mock(GroupExpression.class);
        Mockito.when(child.getPlan()).thenReturn(Mockito.mock(Plan.class));
        Mockito.when(child.getOwnerGroup()).thenReturn(childGroup);
        Mockito.when(child.getOutputProperties(actualChildProperty)).thenReturn(actualChildProperty);
        Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable = Maps.newHashMap();
        lowestCostTable.put(actualChildProperty, Pair.of(Cost.zero(), Lists.newArrayList()));
        Mockito.when(child.getLowestCostTable()).thenReturn(lowestCostTable);

        try (MockedStatic<CostCalculator> mockedCostCalculator = Mockito.mockStatic(CostCalculator.class)) {
            mockedCostCalculator.when(() -> CostCalculator.calculateCost(
                    Mockito.any(), Mockito.any(), Mockito.anyList())).thenReturn(Cost.zero());
            mockedCostCalculator.when(() -> CostCalculator.addChildCost(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt()))
                    .thenReturn(Cost.zero());
            ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent,
                    Lists.newArrayList(child), Lists.newArrayList(actualChildProperty),
                    Lists.newArrayList(requiredChildProperty), jobContext);
            PhysicalProperties result = regulator.adjustChildrenProperties().get(0).get(0);
            return new ShuffleAdjustment(result, Lists.newArrayList(parentRequiredKeys),
                    Lists.newArrayList(key1.getExprId(), key2.getExprId(), key3.getExprId()));
        }
    }

    private List<List<PhysicalProperties>> adjustOnePhaseAggWithCte(
            SlotReference key, Statistics inputStatistics, Statistics aggregateStatistics) {
        return adjustOnePhaseAggWithCte(key, inputStatistics, aggregateStatistics, 3);
    }

    private List<List<PhysicalProperties>> adjustOnePhaseAggWithCte(
            SlotReference key, Statistics inputStatistics, Statistics aggregateStatistics, int beNumber) {
        return adjustOnePhaseAggWithCte(
                Lists.newArrayList(key), inputStatistics, aggregateStatistics, beNumber);
    }

    private List<List<PhysicalProperties>> adjustOnePhaseAggWithCte(
            List<SlotReference> keys, Statistics inputStatistics, Statistics aggregateStatistics, int beNumber) {
        return adjustOnePhaseAggWithCte(
                keys, inputStatistics, aggregateStatistics, beNumber, 0.1, 10);
    }

    private List<List<PhysicalProperties>> adjustOnePhaseAggWithCte(
            List<SlotReference> keys, Statistics inputStatistics, Statistics aggregateStatistics,
            int beNumber, double hotValueThreshold, int skewValueThreshold) {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setBeNumberForTest(beNumber);
        ctx.getSessionVariable().parallelPipelineTaskNum = 1;
        ctx.getSessionVariable().setHotValueThreshold(hotValueThreshold);
        ctx.getSessionVariable().setSkewValueThreshold(skewValueThreshold);
        Mockito.when(mockedCascadesContext.getConnectContext()).thenReturn(ctx);

        Group ownerGroup = Mockito.mock(Group.class);
        Mockito.when(ownerGroup.getStatistics()).thenReturn(aggregateStatistics);
        GroupExpression aggregateGroupExpression = Mockito.mock(GroupExpression.class);
        Mockito.when(aggregateGroupExpression.getOwnerGroup()).thenReturn(ownerGroup);
        Mockito.when(aggregateGroupExpression.childStatistics(0)).thenReturn(inputStatistics);

        GroupPlan aggregateChild = Mockito.mock(GroupPlan.class);
        Mockito.when(aggregateChild.getAllChildrenTypes()).thenReturn(new BitSet());
        List<Expression> groupByExpressions = Lists.newArrayList(keys);
        List<NamedExpression> outputExpressions = Lists.newArrayList(keys);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                groupByExpressions, outputExpressions, Optional.empty(),
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), false,
                Optional.of(aggregateGroupExpression), Mockito.mock(LogicalProperties.class), false, aggregateChild);
        GroupExpression parent = Mockito.mock(GroupExpression.class);
        Mockito.when(parent.getPlan()).thenReturn(aggregate);

        GroupExpression child = Mockito.mock(GroupExpression.class);
        Mockito.when(child.getPlan()).thenReturn(Mockito.mock(PhysicalDistribute.class));
        Group distributeChildGroup = Mockito.mock(Group.class);
        Mockito.when(child.children()).thenReturn(Lists.newArrayList(distributeChildGroup));
        GroupExpression cteConsumer = Mockito.mock(GroupExpression.class);
        Mockito.when(cteConsumer.getPlan()).thenReturn(Mockito.mock(PhysicalCTEConsumer.class));
        Mockito.when(distributeChildGroup.getPhysicalExpressions()).thenReturn(Lists.newArrayList(cteConsumer));

        List<ExprId> requiredExprIds = new ArrayList<>(keys.size());
        for (SlotReference key : keys) {
            requiredExprIds.add(key.getExprId());
        }
        PhysicalProperties requiredProperty = PhysicalProperties.createHash(requiredExprIds, ShuffleType.REQUIRE);
        ChildrenPropertiesRegulator regulator = new ChildrenPropertiesRegulator(parent,
                Lists.newArrayList(child), Lists.newArrayList(PhysicalProperties.ANY),
                Lists.newArrayList(requiredProperty), mockedJobContext);
        ConnectContext previousContext = ConnectContext.get();
        ctx.setThreadLocalInfo();
        try {
            return regulator.adjustChildrenProperties();
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
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
