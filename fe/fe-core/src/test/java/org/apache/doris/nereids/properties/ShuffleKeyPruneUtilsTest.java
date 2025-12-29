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
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for {@link ShuffleKeyPruneUtils}.
 */
class ShuffleKeyPruneUtilsTest extends TestWithFeService {

    private static final double ROW_COUNT = 204000;
    private static final int INSTANCE_NUM = 8;

    private SlotReference slotA;
    private SlotReference slotB;
    private SlotReference slotC;
    private SlotReference slotD;
    private SlotReference slotE;
    private SlotReference slotF;
    private SlotReference slotG;
    private SlotReference rightSlotA;
    private SlotReference rightSlotB;
    private SlotReference rightSlotC;
    private SlotReference rightSlotD;
    private SlotReference rightSlotE;
    private SlotReference rightSlotF;
    private SlotReference rightSlotG;
    private GroupExpression emptyExpression;

    @Override
    protected void runBeforeAll() {
        slotA = new SlotReference(new ExprId(0), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        slotB = new SlotReference(new ExprId(1), "b", IntegerType.INSTANCE, true, ImmutableList.of());
        slotC = new SlotReference(new ExprId(2), "c", IntegerType.INSTANCE, true, ImmutableList.of());
        slotD = new SlotReference(new ExprId(3), "d", IntegerType.INSTANCE, true, ImmutableList.of());
        slotE = new SlotReference(new ExprId(4), "e", IntegerType.INSTANCE, true, ImmutableList.of());
        slotF = new SlotReference(new ExprId(5), "f", IntegerType.INSTANCE, true, ImmutableList.of());
        slotG = new SlotReference(new ExprId(6), "g", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotA = new SlotReference(new ExprId(7), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotB = new SlotReference(new ExprId(8), "b", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotC = new SlotReference(new ExprId(9), "c", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotD = new SlotReference(new ExprId(10), "d", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotE = new SlotReference(new ExprId(11), "e", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotF = new SlotReference(new ExprId(12), "f", IntegerType.INSTANCE, true, ImmutableList.of());
        rightSlotG = new SlotReference(new ExprId(13), "g", IntegerType.INSTANCE, true, ImmutableList.of());
        emptyExpression = new GroupExpression(new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), null));
        new ConnectContextMockUp();
    }

    /** MockUp for static getTotalInstanceNum - use named class for Java 8 (anonymous class cannot have static members). */
    private static class ConnectContextMockUp extends MockUp<ConnectContext> {
        @Mock
        public int getTotalInstanceNum() {
            return INSTANCE_NUM;
        }
    }

    /** Creates a child Group with emptyRelation and statistics (ndv=5000) for given slots. */
    private Group createChildGroupWithStats(Expression... slots) {
        ImmutableMap.Builder<Expression, Double> builder = ImmutableMap.builder();
        for (Expression slot : slots) {
            builder.put(slot, 5000.0);
        }
        Group group = new Group(GroupId.createGenerator().getNextId(), emptyExpression, null);
        group.setStatistics(statsWithNdv(builder.build(), ROW_COUNT));
        return group;
    }

    /** Creates a child Group with emptyRelation and custom ndv statistics for given expressions. */
    private Group createChildGroupWithCustomStats(Map<Expression, Double> exprToNdv) {
        Group group = new Group(GroupId.createGenerator().getNextId(), emptyExpression, null);
        group.setStatistics(statsWithNdv(exprToNdv, ROW_COUNT));
        return group;
    }

    /** Creates a child Group with emptyRelation, without statistics. */
    private Group createChildGroup() {
        return new Group(GroupId.createGenerator().getNextId(), emptyExpression, null);
    }

    /** Creates PhysicalHashAggregate over childGroup, registers in memo. Returns (aggWithGroupExpr, aggGroupExpr). */
    private Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> createAggAndRegister(Group childGroup,
            List<? extends Expression> groupByExprs, boolean hasSourceRepeat) {
        GroupPlan groupPlan = new GroupPlan(childGroup);
        PhysicalHashAggregate<GroupPlan> agg = new PhysicalHashAggregate<>(
                (List) groupByExprs, (List) groupByExprs,
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, hasSourceRepeat, groupPlan);
        GroupExpression aggGroupExpr = new GroupExpression(agg, Lists.newArrayList(childGroup));
        new Group(GroupId.createGenerator().getNextId(), aggGroupExpr, null);
        return Pair.of((PhysicalHashAggregate<GroupPlan>) aggGroupExpr.getPlan(), aggGroupExpr);
    }

    /** Creates left/right agg groups with 7 slots each for join tests. Returns (leftGroup, rightGroup). */
    private Pair<Group, Group> createJoinAggGroups() {
        return createJoinAggGroups(false, false);
    }

    /** Creates left/right agg groups with configurable hasSourceRepeat for join tests. */
    private Pair<Group, Group> createJoinAggGroups(boolean hasSourceRepeatLeft, boolean hasSourceRepeatRight) {
        Group leftChildGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF, slotG);
        PhysicalHashAggregate<GroupPlan> leftAgg = new PhysicalHashAggregate<>(
                Lists.newArrayList(slotA, slotB, slotC, slotD, slotE, slotF, slotG),
                Lists.newArrayList(slotA, slotB, slotC, slotD, slotE, slotF, slotG),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, hasSourceRepeatLeft, new GroupPlan(leftChildGroup));
        GroupExpression leftAggGroupExpr = new GroupExpression(leftAgg, Lists.newArrayList(leftChildGroup));
        Group leftGroup = new Group(GroupId.createGenerator().getNextId(), leftAggGroupExpr, null);

        Group rightChildGroup = createChildGroupWithStats(rightSlotA, rightSlotB, rightSlotC, rightSlotD,
                rightSlotE, rightSlotF, rightSlotG);
        PhysicalHashAggregate<GroupPlan> rightAgg = new PhysicalHashAggregate<>(
                Lists.newArrayList(rightSlotA, rightSlotB, rightSlotC, rightSlotD, rightSlotE, rightSlotF, rightSlotG),
                Lists.newArrayList(rightSlotA, rightSlotB, rightSlotC, rightSlotD, rightSlotE, rightSlotF, rightSlotG),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, hasSourceRepeatRight, new GroupPlan(rightChildGroup));
        GroupExpression rightAggGroupExpr = new GroupExpression(rightAgg, Lists.newArrayList(rightChildGroup));
        Group rightGroup = new Group(GroupId.createGenerator().getNextId(), rightAggGroupExpr, null);

        return Pair.of(leftGroup, rightGroup);
    }

    /** Creates hash join with given number of conjuncts and mocks getHashConjunctsExprIds. */
    private PhysicalHashJoin<GroupPlan, GroupPlan> createHashJoinWithConjuncts(Group leftGroup, Group rightGroup,
            int numConjuncts) {
        List<Slot> leftSlots = ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF, slotG);
        List<Slot> rightSlots = ImmutableList.of(rightSlotA, rightSlotB, rightSlotC, rightSlotD,
                rightSlotE, rightSlotF, rightSlotG);
        List<Expression> hashJoinConjuncts = Lists.newArrayList();
        List<ExprId> leftIds = Lists.newArrayList();
        List<ExprId> rightIds = Lists.newArrayList();
        for (int i = 0; i < numConjuncts; i++) {
            hashJoinConjuncts.add(new EqualTo(leftSlots.get(i), rightSlots.get(i)));
            leftIds.add(leftSlots.get(i).getExprId());
            rightIds.add(rightSlots.get(i).getExprId());
        }
        PhysicalHashJoin<GroupPlan, GroupPlan> hashJoin = new PhysicalHashJoin<>(
                JoinType.INNER_JOIN,
                hashJoinConjuncts,
                ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                null, new GroupPlan(leftGroup), new GroupPlan(rightGroup));

        final List<ExprId> finalLeftIds = leftIds;
        final List<ExprId> finalRightIds = rightIds;
        new MockUp<PhysicalHashJoin<GroupPlan, GroupPlan>>() {
            @Mock
            public Pair<List<ExprId>, List<ExprId>> getHashConjunctsExprIds() {
                return Pair.of(finalLeftIds, finalRightIds);
            }
        };
        return hashJoin;
    }

    @Test
    void testSelectOptimalShuffleKeyForAggWithParentHashRequest_disabled() {
        connectContext.getSessionVariable().enableAggShuffleKeyPrune = false;

        Group childGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        Set<ExprId> intersectIdSet = Sets.newHashSet(slotA.getExprId(), slotB.getExprId(), slotC.getExprId(),
                slotD.getExprId(), slotE.getExprId(), slotF.getExprId());
        PlanContext planContext = new PlanContext(connectContext, aggSetup.second,
                Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY));

        List<ExprId> result = ShuffleKeyPruneUtils.selectOptimalShuffleKeyForAggWithParentHashRequest(
                aggSetup.first, Utils.fastToImmutableList(intersectIdSet), planContext);

        Assertions.assertEquals(6, result.size());
        connectContext.getSessionVariable().enableAggShuffleKeyPrune = true;
    }

    @Test
    void testSelectOptimalShuffleKeyForAggWithParentHashRequest_noStatistics() {
        Set<ExprId> intersectIdSet = Sets.newHashSet(slotA.getExprId(), slotB.getExprId(), slotC.getExprId(),
                slotD.getExprId(), slotE.getExprId(), slotF.getExprId());

        Group childGroup = createChildGroup();  // no statistics
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        PlanContext planContext = new PlanContext(connectContext, aggSetup.second,
                Lists.newArrayList(PhysicalProperties.ANY));

        List<ExprId> result = ShuffleKeyPruneUtils.selectOptimalShuffleKeyForAggWithParentHashRequest(
                aggSetup.first, Utils.fastToImmutableList(intersectIdSet), planContext);

        Assertions.assertEquals(6, result.size());
    }

    @Test
    void testSelectOptimalShuffleKeyForAggWithParentHashRequest_selectBestKey() {
        Set<ExprId> intersectIdSet = Sets.newHashSet(slotA.getExprId(), slotB.getExprId(), slotC.getExprId(),
                slotD.getExprId(), slotE.getExprId(), slotF.getExprId());

        Group childGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        PlanContext planContext = new PlanContext(connectContext, aggSetup.second,
                Lists.newArrayList(PhysicalProperties.ANY));
        List<ExprId> result = ShuffleKeyPruneUtils.selectOptimalShuffleKeyForAggWithParentHashRequest(
                aggSetup.first, Utils.fastToImmutableList(intersectIdSet), planContext);

        Assertions.assertEquals(1, result.size());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_disabled() {
        connectContext.getSessionVariable().enableAggShuffleKeyPrune = false;

        Group childGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(aggSetup.first,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), connectContext);

        Assertions.assertFalse(result.isPresent());
        connectContext.getSessionVariable().enableAggShuffleKeyPrune = true;
    }

    @Test
    void testSelectBestShuffleKeyForAgg_hasSourceRepeat() {
        Group childGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), true);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(aggSetup.first,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_noChildStats() {
        Group childGroup = createChildGroup();  // no statistics
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(aggSetup.first,
                aggSetup.first.getGroupByExpressions(), connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_selectBestKey() {
        Group childGroup = createChildGroupWithStats(slotA, slotB, slotC, slotD, slotE, slotF);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(aggSetup.first,
                aggSetup.first.getGroupByExpressions(), connectContext);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertFalse(result.get().isEmpty());
        Assertions.assertInstanceOf(SlotReference.class, result.get().get(0));
    }

    @Test
    void testTryFindOptimalShuffleKeyForBothAggChildren_selectBestKey() {
        Pair<Group, Group> groups = createJoinAggGroups();
        PhysicalHashJoin<GroupPlan, GroupPlan> hashJoin = createHashJoinWithConjuncts(groups.first, groups.second, 7);

        GroupExpression joinGroupExpr = new GroupExpression(hashJoin, Lists.newArrayList(groups.first, groups.second));
        PlanContext planContext = new PlanContext(connectContext, joinGroupExpr,
                Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY));
        new Group(GroupId.createGenerator().getNextId(), joinGroupExpr, null);

        Optional<Pair<List<ExprId>, List<ExprId>>> result = ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForBothAggChildren(
                hashJoin, planContext);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertFalse(result.get().first.isEmpty());
        Assertions.assertFalse(result.get().second.isEmpty());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_pruneStringKeysWithCombinedNdv() {
        SlotReference numericSlot = new SlotReference(new ExprId(14), "num_col", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference dateSlot = new SlotReference(new ExprId(15), "date_col", DateType.INSTANCE, true,
                ImmutableList.of());
        SlotReference stringSlot = new SlotReference(new ExprId(16), "str_col", new VarcharType(64), true,
                ImmutableList.of());

        Map<Expression, Double> exprToNdv = new HashMap<>();
        exprToNdv.put(numericSlot, 3000.0);
        exprToNdv.put(dateSlot, 3000.0);
        exprToNdv.put(stringSlot, 3000.0);
        Group childGroup = createChildGroupWithCustomStats(exprToNdv);
        Pair<PhysicalHashAggregate<GroupPlan>, GroupExpression> aggSetup = createAggAndRegister(childGroup,
                ImmutableList.of(numericSlot, dateSlot, stringSlot), false);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(aggSetup.first,
                ImmutableList.of(numericSlot, dateSlot, stringSlot), connectContext);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(2, result.get().size());
        Assertions.assertEquals(numericSlot, result.get().get(0));
        Assertions.assertEquals(dateSlot, result.get().get(1));
    }

    @Test
    void testTryFindOptimalShuffleKeyForBothAggChildren_leftHasSourceRepeat() {
        Pair<Group, Group> groups = createJoinAggGroups(true, false);
        PhysicalHashJoin<GroupPlan, GroupPlan> hashJoin = createHashJoinWithConjuncts(groups.first, groups.second, 7);

        GroupExpression joinGroupExpr = new GroupExpression(hashJoin, Lists.newArrayList(groups.first, groups.second));
        new Group(GroupId.createGenerator().getNextId(), joinGroupExpr, null);
        PlanContext planContext = new PlanContext(connectContext, joinGroupExpr,
                Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY));

        Optional<Pair<List<ExprId>, List<ExprId>>> result = ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForBothAggChildren(
                hashJoin, planContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalShuffleKeyForBothAggChildren_rightHasSourceRepeat() {
        Pair<Group, Group> groups = createJoinAggGroups(false, true);
        PhysicalHashJoin<GroupPlan, GroupPlan> hashJoin = createHashJoinWithConjuncts(groups.first, groups.second, 7);

        GroupExpression joinGroupExpr = new GroupExpression(hashJoin, Lists.newArrayList(groups.first, groups.second));
        PlanContext planContext = new PlanContext(connectContext, joinGroupExpr,
                Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.ANY));
        new Group(GroupId.createGenerator().getNextId(), joinGroupExpr, null);

        Optional<Pair<List<ExprId>, List<ExprId>>> result = ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForBothAggChildren(
                hashJoin, planContext);

        Assertions.assertFalse(result.isPresent());
    }
}
