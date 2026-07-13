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

package org.apache.doris.nereids.stats;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for MemoStatsAndCostRecomputer trust join counting.
 */
public class MemoStatsAndCostRecomputerTest {

    private MemoStatsAndCostRecomputer recomputer;
    private IdGenerator<GroupId> idGenerator;

    @BeforeEach
    public void setUp() {
        recomputer = new MemoStatsAndCostRecomputer(null, null);
        idGenerator = GroupId.createGenerator();
    }

    // Use LogicalOneRowRelation as a leaf plan — it's a real concrete plan class
    // that doesn't need catalog setup and is not a Join (so isTrustJoin returns false).

    /**
     * Create a leaf Group (no join) with row count statistics.
     */
    private Group createLeafGroup(int rowCount) {
        LogicalOneRowRelation leafPlan = new LogicalOneRowRelation(
                new RelationId(idGenerator.getNextId().asInt()), ImmutableList.of());
        GroupExpression leafExpr = new GroupExpression(leafPlan);
        Group group = new Group(idGenerator.getNextId(), leafExpr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        group.setStatistics(new StatisticsBuilder().setRowCount(rowCount).build());
        return group;
    }

    /**
     * Create a Group containing a LogicalJoin with the given children, join type,
     * equal predicates, and statistics for the children.
     *
     * The created Group contains a single logical expression (the join).
     * Child Groups must already have statistics set.
     */
    private Group createJoinGroup(Group leftChild, Group rightChild,
            JoinType joinType, List<SlotReference> leftKeys, List<SlotReference> rightKeys,
            ColumnStatistic leftKeyStats, ColumnStatistic rightKeyStats) {
        // Build equal predicates
        List<EqualTo> equalPredicates = new ArrayList<>();
        for (int i = 0; i < leftKeys.size(); i++) {
            equalPredicates.add(new EqualTo(leftKeys.get(i), rightKeys.get(i)));
        }

        // Set column statistics on child groups
        Statistics leftStats = leftChild.getStatistics();
        if (leftStats != null && leftKeyStats != null) {
            for (SlotReference key : leftKeys) {
                leftStats.addColumnStats(key, leftKeyStats);
            }
        }
        Statistics rightStats = rightChild.getStatistics();
        if (rightStats != null && rightKeyStats != null) {
            for (SlotReference key : rightKeys) {
                rightStats.addColumnStats(key, rightKeyStats);
            }
        }

        LogicalJoin join = new LogicalJoin(joinType, equalPredicates,
                leftChild.getGroupPlan(), rightChild.getGroupPlan(), null);
        GroupExpression joinExpr = new GroupExpression(join, Lists.newArrayList(leftChild, rightChild));
        Group joinGroup = new Group(idGenerator.getNextId(), joinExpr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        return joinGroup;
    }

    /**
     * Create a Group containing the given logical expressions (multiple alternatives).
     */
    private Group createGroupWithAlternatives(List<GroupExpression> expressions) {
        Assertions.assertFalse(expressions.isEmpty());
        Group group = new Group(idGenerator.getNextId(), expressions.get(0),
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        for (int i = 1; i < expressions.size(); i++) {
            group.addLogicalExpression(expressions.get(i));
        }
        return group;
    }

    // ========== Test cases ==========

    @Test
    public void testIsTrustJoin_knownStats_returnsTrue() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        // NDV/rowCount = 95/100 = 0.95 > 0.9 threshold → trustable
        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();

        Group joinGroup = createJoinGroup(leftGroup, rightGroup,
                JoinType.INNER_JOIN,
                ImmutableList.of(a), ImmutableList.of(b),
                trustableStats, trustableStats);

        GroupExpression joinExpr = joinGroup.getFirstLogicalExpression();
        Assertions.assertTrue(recomputer.isTrustJoin(joinExpr));
    }

    @Test
    public void testIsTrustJoin_crossJoin_returnsFalse() {
        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        // Cross join has no equal predicates
        LogicalJoin crossJoin = new LogicalJoin(JoinType.CROSS_JOIN,
                ImmutableList.of(), leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression crossJoinExpr = new GroupExpression(crossJoin,
                Lists.newArrayList(leftGroup, rightGroup));
        Group crossJoinGroup = new Group(idGenerator.getNextId(), crossJoinExpr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));

        GroupExpression expr = crossJoinGroup.getFirstLogicalExpression();
        Assertions.assertFalse(recomputer.isTrustJoin(expr));
    }

    @Test
    public void testIsTrustJoin_lowNdvStats_returnsFalse() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        // NDV/rowCount = 10/100 = 0.1 < 0.9 threshold → not trustable
        ColumnStatistic lowNdvStats = new ColumnStatisticBuilder(100)
                .setNdv(10).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();

        Group joinGroup = createJoinGroup(leftGroup, rightGroup,
                JoinType.INNER_JOIN,
                ImmutableList.of(a), ImmutableList.of(b),
                lowNdvStats, lowNdvStats);

        GroupExpression joinExpr = joinGroup.getFirstLogicalExpression();
        Assertions.assertFalse(recomputer.isTrustJoin(joinExpr));
    }

    @Test
    public void testIsTrustJoin_unknownColStats_returnsFalse() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        // One-row left side with NO column stats → ExpressionEstimation returns
        // ColumnStatistic.UNKNOWN (ndv=1, isUnKnown=true).
        // Without the isUnKnown guard in hasTrustableEqualCondition(), the NDV
        // ratio would be 1 / nonZeroDivisor(1) = 1.0 > 0.9, incorrectly marking
        // the join as trusted.
        Group leftGroup = createLeafGroup(1);
        Group rightGroup = createLeafGroup(100);

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression joinExpr = new GroupExpression(join,
                Lists.newArrayList(leftGroup, rightGroup));

        Assertions.assertFalse(recomputer.isTrustJoin(joinExpr));
    }

    @Test
    public void testCountTrustJoins_singleTrustJoin_returnsOne() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();

        Group joinGroup = createJoinGroup(leftGroup, rightGroup,
                JoinType.INNER_JOIN,
                ImmutableList.of(a), ImmutableList.of(b),
                trustableStats, trustableStats);

        GroupExpression joinExpr = joinGroup.getFirstLogicalExpression();
        // joinGroup has 1 trust join (the join itself), leaf children are not joins → 0
        Assertions.assertEquals(1, recomputer.countTrustJoins(joinExpr));
    }

    @Test
    public void testGetGroupTrustJoinCount_returnsMaxAcrossAlternatives() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        // Alternative 1: trust join (NDV/rowCount > 0.9)
        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();
        leftGroup.getStatistics().addColumnStats(a, trustableStats);
        rightGroup.getStatistics().addColumnStats(b, trustableStats);

        LogicalJoin trustJoin = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression trustExpr = new GroupExpression(trustJoin,
                Lists.newArrayList(leftGroup, rightGroup));

        // Alternative 2: cross join (no equal predicates) → not trust join
        LogicalJoin crossJoin = new LogicalJoin(JoinType.CROSS_JOIN,
                ImmutableList.of(), leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression crossExpr = new GroupExpression(crossJoin,
                Lists.newArrayList(leftGroup, rightGroup));

        Group group = createGroupWithAlternatives(ImmutableList.of(crossExpr, trustExpr));
        // Both alternatives are in the group. The max trust join count should be 1.
        Assertions.assertEquals(1, recomputer.getGroupTrustJoinCount(group));
    }

    @Test
    public void testGetGroupTrustJoinCount_orderIndependent() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();
        leftGroup.getStatistics().addColumnStats(a, trustableStats);
        rightGroup.getStatistics().addColumnStats(b, trustableStats);

        LogicalJoin trustJoin = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression trustExpr = new GroupExpression(trustJoin,
                Lists.newArrayList(leftGroup, rightGroup));

        LogicalJoin crossJoin = new LogicalJoin(JoinType.CROSS_JOIN,
                ImmutableList.of(), leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression crossExpr = new GroupExpression(crossJoin,
                Lists.newArrayList(leftGroup, rightGroup));

        // Order 1: cross first, trust second
        Group group1 = createGroupWithAlternatives(ImmutableList.of(crossExpr, trustExpr));
        int score1 = recomputer.getGroupTrustJoinCount(group1);

        // Order 2: trust first, cross second (fresh recomputer to clear cache)
        MemoStatsAndCostRecomputer recomputer2 = new MemoStatsAndCostRecomputer(null, null);
        Group group2 = createGroupWithAlternatives(ImmutableList.of(trustExpr, crossExpr));
        int score2 = recomputer2.getGroupTrustJoinCount(group2);

        Assertions.assertEquals(score1, score2);
        Assertions.assertEquals(1, score1);
    }

    @Test
    public void testCountTrustJoins_bottomUp_childGroupBeforeParent() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leafLeft = createLeafGroup(100);
        Group leafRight = createLeafGroup(80);

        // Child join group: trust join (score = 1 just for the join itself)
        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();

        // Add column stats to leaf groups for the child join keys
        leafLeft.getStatistics().addColumnStats(a, trustableStats);
        leafRight.getStatistics().addColumnStats(b, trustableStats);

        LogicalJoin childJoin = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leafLeft.getGroupPlan(), leafRight.getGroupPlan(), null);
        GroupExpression childExpr = new GroupExpression(childJoin,
                Lists.newArrayList(leafLeft, leafRight));
        Group childGroup = new Group(idGenerator.getNextId(), childExpr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        childGroup.setStatistics(new StatisticsBuilder().setRowCount(50).build());
        childGroup.getStatistics().addColumnStats(a, trustableStats);
        childGroup.getStatistics().addColumnStats(b, trustableStats);

        // Parent join group: trust join using the child group as left input.
        // Right input is a new leaf.
        Group newLeaf = createLeafGroup(200);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE);
        childGroup.getStatistics().addColumnStats(c, trustableStats);
        newLeaf.getStatistics().addColumnStats(d, trustableStats);

        LogicalJoin parentJoin = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(c, d)),
                childGroup.getGroupPlan(), newLeaf.getGroupPlan(), null);
        GroupExpression parentExpr = new GroupExpression(parentJoin,
                Lists.newArrayList(childGroup, newLeaf));
        Group parentGroup = new Group(idGenerator.getNextId(), parentExpr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));

        // parentJoin itself is trustable (+1), childGroup has getGroupTrustJoinCount = 1
        // Expected total: 1 (parent) + 1 (child) = 2
        Assertions.assertEquals(2, recomputer.countTrustJoins(parentGroup.getFirstLogicalExpression()));

        // Also verify getGroupTrustJoinCount on childGroup independently
        int childScore = recomputer.getGroupTrustJoinCount(childGroup);
        Assertions.assertEquals(1, childScore,
                "Child group should have 1 trust join (the join inside it)");
    }

    @Test
    public void testGetGroupTrustJoinCount_noAlternatives_returnsZeroForLeaf() {
        Group leafGroup = createLeafGroup(100);
        // Leaf group has no join expressions → trust join count = 0
        Assertions.assertEquals(0, recomputer.getGroupTrustJoinCount(leafGroup));
    }

    @Test
    public void testCountTrustJoins_multiLevel_bottomUpAccumulatesCorrectly() {
        // Build a 3-level join tree:
        //   ((A join B) join C) join D
        // where all joins are trust joins.
        // Expected count: 3 (one for each join level)

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE);

        Group leafA = createLeafGroup(100);
        Group leafB = createLeafGroup(100);
        Group leafC = createLeafGroup(100);
        Group leafD = createLeafGroup(100);

        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();

        // Level 1: A join B — add column stats to leaf groups
        leafA.getStatistics().addColumnStats(a, trustableStats);
        leafB.getStatistics().addColumnStats(b, trustableStats);
        LogicalJoin joinAB = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leafA.getGroupPlan(), leafB.getGroupPlan(), null);
        GroupExpression exprAB = new GroupExpression(joinAB, Lists.newArrayList(leafA, leafB));
        Group groupAB = new Group(idGenerator.getNextId(), exprAB,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        groupAB.setStatistics(new StatisticsBuilder().setRowCount(80).build());

        // Level 2: (A join B) join C — groupAB needs c stats, leafC needs c stats
        groupAB.getStatistics().addColumnStats(c, trustableStats);
        leafC.getStatistics().addColumnStats(c, trustableStats);
        LogicalJoin joinABC = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(c, c)), // use same slot for simplicity; just need an equal predicate
                groupAB.getGroupPlan(), leafC.getGroupPlan(), null);
        GroupExpression exprABC = new GroupExpression(joinABC, Lists.newArrayList(groupAB, leafC));
        Group groupABC = new Group(idGenerator.getNextId(), exprABC,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        groupABC.setStatistics(new StatisticsBuilder().setRowCount(60).build());
        groupABC.getStatistics().addColumnStats(c, trustableStats);

        // Level 3: ((A join B) join C) join D — groupABC needs d stats, leafD needs d stats
        groupABC.getStatistics().addColumnStats(d, trustableStats);
        leafD.getStatistics().addColumnStats(d, trustableStats);
        LogicalJoin joinABCD = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(d, d)),
                groupABC.getGroupPlan(), leafD.getGroupPlan(), null);
        GroupExpression exprABCD = new GroupExpression(joinABCD, Lists.newArrayList(groupABC, leafD));
        Group groupABCD = new Group(idGenerator.getNextId(), exprABCD,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));
        groupABCD.setStatistics(new StatisticsBuilder().setRowCount(40).build());
        groupABCD.getStatistics().addColumnStats(d, trustableStats);

        // All 3 levels are trust joins.
        int totalTrustJoins = recomputer.countTrustJoins(groupABCD.getFirstLogicalExpression());
        Assertions.assertEquals(3, totalTrustJoins,
                "Three-level trust join tree should accumulate to 3");
    }

    @Test
    public void testCacheConsistency_sameGroupMultipleCalls() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();
        leftGroup.getStatistics().addColumnStats(a, trustableStats);
        rightGroup.getStatistics().addColumnStats(b, trustableStats);

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression expr = new GroupExpression(join, Lists.newArrayList(leftGroup, rightGroup));
        Group group = new Group(idGenerator.getNextId(), expr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));

        // Multiple calls should return the same cached value
        int first = recomputer.getGroupTrustJoinCount(group);
        int second = recomputer.getGroupTrustJoinCount(group);
        int third = recomputer.getGroupTrustJoinCount(group);
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first, third);
        Assertions.assertEquals(1, first);
    }

    @Test
    public void testGroupTrustJoinCount_invalidatedAfterCacheClear() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        Group leftGroup = createLeafGroup(100);
        Group rightGroup = createLeafGroup(80);

        // Start with low-NDV stats → not a trust join
        ColumnStatistic lowNdvStats = new ColumnStatisticBuilder(100)
                .setNdv(10).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();
        leftGroup.getStatistics().addColumnStats(a, lowNdvStats);
        rightGroup.getStatistics().addColumnStats(b, lowNdvStats);

        LogicalJoin join = new LogicalJoin(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(a, b)),
                leftGroup.getGroupPlan(), rightGroup.getGroupPlan(), null);
        GroupExpression expr = new GroupExpression(join, Lists.newArrayList(leftGroup, rightGroup));
        Group group = new Group(idGenerator.getNextId(), expr,
                new LogicalProperties(ArrayList::new, () -> DataTrait.EMPTY_TRAIT));

        // First pass: low NDV → not trustable → trust count = 0
        int firstCount = recomputer.getGroupTrustJoinCount(group);
        Assertions.assertEquals(0, firstCount);

        // Simulate the second pass: clear cache, replace with high-NDV stats
        ColumnStatistic trustableStats = new ColumnStatisticBuilder(100)
                .setNdv(95).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(100).build();
        leftGroup.getStatistics().addColumnStats(a, trustableStats);
        rightGroup.getStatistics().addColumnStats(b, trustableStats);

        // Clear the cache (as recompute() does before each logical re-estimation pass)
        recomputer.groupTrustJoinCountCache.clear();

        // Second pass: high NDV → now trustable → trust count = 1
        int secondCount = recomputer.getGroupTrustJoinCount(group);
        Assertions.assertEquals(1, secondCount,
                "After cache clear with updated stats, trust join count should reflect new statistics");
    }
}
