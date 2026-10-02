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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo.HashType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class ChildOutputPropertyDeriverTest {
    GroupExpression ge = new GroupExpression(
            new LogicalOneRowRelation(
                    new RelationId(1),
                    ImmutableList.of(new Alias(Literal.of(1)))
            ),
            ImmutableList.of()
    );

    GroupPlan groupPlan = new GroupPlan(
            new Group(GroupId.createGenerator().getNextId(),
                    ge.getPlan().getLogicalProperties()
            )
    );

    LogicalProperties logicalProperties = Mockito.mock(LogicalProperties.class);

    ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;

        mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        mockedEnv.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);

        mockedConnectContext = Mockito.mockStatic(ConnectContext.class, Mockito.CALLS_REAL_METHODS);
        mockedConnectContext.when(ConnectContext::get).thenReturn(new ConnectContext());
    }

    @AfterEach
    public void tearDown() {
        if (mockedConnectContext != null) {
            mockedConnectContext.close();
        }
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    @Test
    void testInnerJoin() {
        testInnerJoinHelper(JoinType.INNER_JOIN);
        testInnerJoinHelper(JoinType.ASOF_LEFT_INNER_JOIN);
        testInnerJoinHelper(JoinType.ASOF_RIGHT_INNER_JOIN);
    }

    private void testInnerJoinHelper(JoinType joinType) {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(joinType,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(2, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testCrossJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(2, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testLeftOuterJoin() {
        testLeftOuterJoinHelper(JoinType.LEFT_OUTER_JOIN);
        testLeftOuterJoinHelper(JoinType.ASOF_LEFT_OUTER_JOIN);
    }

    private void testLeftOuterJoinHelper(JoinType joinType) {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(joinType,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(0, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testLeftSemiJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(0, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testLeftAntiJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.LEFT_ANTI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(0, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testNullAwareLeftAntiJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.NULL_AWARE_LEFT_ANTI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(0, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testRightSemiJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_SEMI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        Assertions.assertEquals(-1, actual.getTableId());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testRightAntiJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_ANTI_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        long leftTableId = 0L;
        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        leftTableId,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        long rightTableId = 1L;
        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.NATURAL,
                rightTableId,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();

        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        Assertions.assertEquals(
                SessionVariable.canUseNereidsDistributePlanner() ? rightTableId : -1L, actual.getTableId()
        );
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testRightOuterJoin() {
        testRightOuterJoinHelper(JoinType.RIGHT_OUTER_JOIN);
        testRightOuterJoinHelper(JoinType.ASOF_RIGHT_OUTER_JOIN);
    }

    private void testRightOuterJoinHelper(JoinType joinType) {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(joinType,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        Assertions.assertEquals(-1, actual.getTableId());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testFullOuterJoinWithNatural() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.FULL_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.NATURAL,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.NATURAL,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecStorageAny.class, result.getDistributionSpec());
    }

    @Test
    void testFullOuterJoinWithOther() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.FULL_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        PhysicalProperties left = new PhysicalProperties(
                new DistributionSpecHash(
                        Lists.newArrayList(new ExprId(0)),
                        ShuffleType.EXECUTION_BUCKETED,
                        0,
                        Sets.newHashSet(0L)
                ),
                new OrderSpec(
                        Lists.newArrayList(new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE),
                                true, true)))
        );

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(1)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecAny.class, result.getDistributionSpec());
    }

    @Test
    void testBroadcastJoin() {
        SlotReference leftSlot = new SlotReference(new ExprId(0), "left", IntegerType.INSTANCE, false, Collections.emptyList());
        SlotReference rightSlot = new SlotReference(new ExprId(2), "right", IntegerType.INSTANCE, false, Collections.emptyList());
        List<Slot> leftOutput = new ArrayList<>();
        List<Slot> rightOutput = new ArrayList<>();
        leftOutput.add(leftSlot);
        rightOutput.add(rightSlot);
        LogicalProperties leftProperties = new LogicalProperties(() -> leftOutput, () -> DataTrait.EMPTY_TRAIT);
        LogicalProperties rightProperties = new LogicalProperties(() -> rightOutput, () -> DataTrait.EMPTY_TRAIT);

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan leftGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), leftProperties));
        GroupPlan rightGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), rightProperties));
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(new EqualTo(
                        leftSlot, rightSlot
                )),
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE),
                Optional.empty(), logicalProperties, leftGroupPlan, rightGroupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        Map<ExprId, Integer> leftMap = Maps.newHashMap();
        leftMap.put(new ExprId(0), 0);
        leftMap.put(new ExprId(1), 0);
        PhysicalProperties left = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                ImmutableList.of(Sets.newHashSet(new ExprId(0), new ExprId(1))),
                leftMap
        ));

        PhysicalProperties right = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(3, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testShuffleJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(new EqualTo(
                        new SlotReference(new ExprId(0), "left", IntegerType.INSTANCE, false, Collections.emptyList()),
                        new SlotReference(new ExprId(2), "right", IntegerType.INSTANCE, false,
                                Collections.emptyList()))),
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        Map<ExprId, Integer> leftMap = Maps.newHashMap();
        leftMap.put(new ExprId(0), 0);
        leftMap.put(new ExprId(1), 0);
        PhysicalProperties left = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0)),
                ShuffleType.EXECUTION_BUCKETED,
                0,
                Sets.newHashSet(0L),
                ImmutableList.of(Sets.newHashSet(new ExprId(0), new ExprId(1))),
                leftMap
        ));

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2)),
                ShuffleType.EXECUTION_BUCKETED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(3, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testNestedLoopJoin() {
        PhysicalNestedLoopJoin<GroupPlan, GroupPlan> join = new PhysicalNestedLoopJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, Optional.empty(), logicalProperties, groupPlan,
                groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        Map<ExprId, Integer> leftMap = Maps.newHashMap();
        leftMap.put(new ExprId(0), 0);
        leftMap.put(new ExprId(1), 0);
        DistributionSpecHash leftHash = new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0)),
                ShuffleType.NATURAL,
                0,
                Sets.newHashSet(0L),
                ImmutableList.of(Sets.newHashSet(new ExprId(0), new ExprId(1))),
                leftMap
        );
        PhysicalProperties left = new PhysicalProperties(leftHash);
        PhysicalProperties right = PhysicalProperties.REPLICATED;
        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(leftHash, actual);
    }

    @Test
    void testLocalPhaseAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER),
                true,
                logicalProperties,
                false,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertEquals(child.getDistributionSpec(), result.getDistributionSpec());
    }

    @Test
    void testGlobalPhaseAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference partition = new SlotReference("col2", BigIntType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        DistributionSpecHash childHash = new DistributionSpecHash(Lists.newArrayList(partition.getExprId()),
                ShuffleType.EXECUTION_BUCKETED);
        PhysicalProperties child = new PhysicalProperties(childHash,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertInstanceOf(DistributionSpecHash.class, result.getDistributionSpec());
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, actual.getShuffleType());
        Assertions.assertEquals(Lists.newArrayList(partition).stream()
                        .map(SlotReference::getExprId).collect(Collectors.toList()),
                actual.getOrderedShuffledColumns());
    }

    @Test
    void testAggregateWithoutGroupBy() {
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(),
                Lists.newArrayList(),
                new AggregateParam(AggPhase.LOCAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                false,
                groupPlan
        );

        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecGather.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(PhysicalProperties.GATHER, result);
    }

    @Test
    void testLocalQuickSort() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalQuickSort<GroupPlan> sort = new PhysicalQuickSort<>(orderKeys, SortPhase.LOCAL_SORT, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecReplicated.INSTANCE, result.getDistributionSpec());
    }

    @Test
    void testQuickSort() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalQuickSort<GroupPlan> sort = new PhysicalQuickSort<>(orderKeys, SortPhase.MERGE_SORT, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    void testTopN() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        // localSort require any
        PhysicalTopN<GroupPlan> sort = new PhysicalTopN<>(orderKeys, 10, 10, SortPhase.LOCAL_SORT, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecReplicated.INSTANCE, result.getDistributionSpec());
        // merge/gather sort requires gather
        sort = new PhysicalTopN<>(orderKeys, 10, 10, SortPhase.MERGE_SORT, logicalProperties, groupPlan);
        groupExpression = new GroupExpression(sort);
        new Group(null, groupExpression, null);
        child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    void testLimit() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalLimit<GroupPlan> limit = new PhysicalLimit<>(10, 10, LimitPhase.ORIGIN, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(limit);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecGather.INSTANCE,
                new OrderSpec(orderKeys));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    void testAssertNumRows() {
        PhysicalAssertNumRows<GroupPlan> assertNumRows = new PhysicalAssertNumRows<>(
                new AssertNumRowsElement(1, "", AssertNumRowsElement.Assertion.EQ),
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(assertNumRows);
        new Group(null, groupExpression, null);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecGather.INSTANCE, new OrderSpec());
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(PhysicalProperties.GATHER, result);
    }

    @Test
    void testRepeatReturnAny() {
        SlotReference c1 = new SlotReference(
                new ExprId(1), "c1", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c2 = new SlotReference(
                new ExprId(2), "c2", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c3 = new SlotReference(
                new ExprId(3), "c3", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c4 = new SlotReference(
                new ExprId(4), "c4", TinyIntType.INSTANCE, true, ImmutableList.of());
        PhysicalRepeat<GroupPlan> repeat = new PhysicalRepeat<>(
                ImmutableList.of(ImmutableList.of(c1, c2), ImmutableList.of(c1), ImmutableList.of(c1, c3)),
                ImmutableList.of(c1, c2, c3),
                c4,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(repeat);
        new Group(null, groupExpression, null);
        PhysicalProperties child = PhysicalProperties.createHash(
                ImmutableList.of(new ExprId(1), new ExprId(2)), ShuffleType.EXECUTION_BUCKETED);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(PhysicalProperties.ANY, result);
    }

    @Test
    void testRepeatReturnChild() {
        SlotReference c1 = new SlotReference(
                new ExprId(1), "c1", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c2 = new SlotReference(
                new ExprId(2), "c2", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c3 = new SlotReference(
                new ExprId(3), "c3", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c4 = new SlotReference(
                new ExprId(4), "c4", TinyIntType.INSTANCE, true, ImmutableList.of());
        PhysicalRepeat<GroupPlan> repeat = new PhysicalRepeat<>(
                ImmutableList.of(ImmutableList.of(c1, c2), ImmutableList.of(c1), ImmutableList.of(c1, c3)),
                ImmutableList.of(c1, c2, c3),
                c4,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(repeat);
        new Group(null, groupExpression, null);
        PhysicalProperties child = PhysicalProperties.createHash(
                ImmutableList.of(new ExprId(1)), ShuffleType.EXECUTION_BUCKETED);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(child, result);
    }

    @Test
    void testRepeatReturnChild2() {
        SlotReference c1 = new SlotReference(
                new ExprId(1), "c1", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c2 = new SlotReference(
                new ExprId(2), "c2", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c3 = new SlotReference(
                new ExprId(3), "c3", TinyIntType.INSTANCE, true, ImmutableList.of());
        SlotReference c4 = new SlotReference(
                new ExprId(4), "c4", TinyIntType.INSTANCE, true, ImmutableList.of());
        PhysicalRepeat<GroupPlan> repeat = new PhysicalRepeat<>(
                ImmutableList.of(ImmutableList.of(c1, c2, c3), ImmutableList.of(c1, c2), ImmutableList.of(c1, c2)),
                ImmutableList.of(c1, c2, c3),
                c4,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(repeat);
        new Group(null, groupExpression, null);
        PhysicalProperties child = PhysicalProperties.createHash(
                ImmutableList.of(new ExprId(1)), ShuffleType.EXECUTION_BUCKETED);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(null, groupExpression);
        Assertions.assertEquals(child, result);
    }

    @Test
    void testComputeProjectOutputProperties() {
        SlotReference c1 = new SlotReference(
                new ExprId(1), "c1", TinyIntType.INSTANCE, true, ImmutableList.of());
        PhysicalProperties hashC1 = PhysicalProperties.createHash(
                ImmutableList.of(new ExprId(1)), ShuffleType.EXECUTION_BUCKETED);
        List<NamedExpression> projects1 = new ArrayList<>();
        projects1.add(c1);
        PhysicalProperties phyProp = ChildOutputPropertyDeriver.computeProjectOutputProperties(projects1, hashC1);
        Assertions.assertEquals(hashC1, phyProp);

        List<NamedExpression> projects2 = new ArrayList<>();
        projects2.add(new Alias(new Abs(c1)));
        PhysicalProperties phyProp2 = ChildOutputPropertyDeriver.computeProjectOutputProperties(projects2, hashC1);
        Assertions.assertEquals(DistributionSpecAny.INSTANCE, phyProp2.getDistributionSpec());

        List<NamedExpression> projects3 = new ArrayList<>();
        projects3.add(new Alias(new Abs(c1)));
        projects3.add(c1);
        PhysicalProperties phyProp3 = ChildOutputPropertyDeriver.computeProjectOutputProperties(projects3, hashC1);
        Assertions.assertEquals(hashC1, phyProp3);
    }

    @Test
    void testComputeUniformAfterRecomputeLogicalProperties() {
        // left child has a uniform slot, right child empty
        SlotReference leftSlot = new SlotReference(new ExprId(100), "left", IntegerType.INSTANCE, false,
                Collections.emptyList());
        SlotReference rightSlot = new SlotReference(new ExprId(101), "right", IntegerType.INSTANCE, false,
                Collections.emptyList());
        List<Slot> leftOutput = Lists.newArrayList(leftSlot);
        List<Slot> rightOutput = Lists.newArrayList(rightSlot);

        DataTrait.Builder leftBuilder = new DataTrait.Builder();
        leftBuilder.addUniformSlot(leftSlot);
        DataTrait leftTrait = leftBuilder.build();

        LogicalProperties leftLogical = new LogicalProperties(() -> leftOutput, () -> leftTrait);
        LogicalProperties rightLogical = new LogicalProperties(() -> rightOutput, () -> DataTrait.EMPTY_TRAIT);

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan leftGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), leftLogical));
        GroupPlan rightGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), rightLogical));

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.ASOF_LEFT_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                leftGroupPlan, rightGroupPlan);

        // simulate physical-tree logical prop recompute by resetting logical properties on the join
        AbstractPhysicalPlan processed = (AbstractPhysicalPlan) join.resetLogicalProperties();
        processed = (AbstractPhysicalPlan) processed.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) join);

        Assertions.assertInstanceOf(PhysicalHashJoin.class, processed);

        DataTrait.Builder builder = new DataTrait.Builder();
        processed.computeUniform(builder);
        DataTrait result = builder.build();

        // left slot should still be recognized as uniform after recompute
        Assertions.assertTrue(result.isUniformAndNotNull(leftSlot));
    }

    @Test
    void testComputeUniformAfterRecomputeLogicalProperties_AsOfLeftInner() {
        SlotReference leftSlot = new SlotReference(new ExprId(200), "l", IntegerType.INSTANCE, false,
                Collections.emptyList());
        SlotReference rightSlot = new SlotReference(new ExprId(201), "r", IntegerType.INSTANCE, false,
                Collections.emptyList());
        List<Slot> leftOutput = Lists.newArrayList(leftSlot);
        List<Slot> rightOutput = Lists.newArrayList(rightSlot);

        DataTrait.Builder leftBuilder = new DataTrait.Builder();
        leftBuilder.addUniformSlot(leftSlot);
        DataTrait leftTrait = leftBuilder.build();

        DataTrait.Builder rightBuilder = new DataTrait.Builder();
        rightBuilder.addUniformSlot(rightSlot);
        DataTrait rightTrait = rightBuilder.build();

        LogicalProperties leftLogical = new LogicalProperties(() -> leftOutput, () -> leftTrait);
        LogicalProperties rightLogical = new LogicalProperties(() -> rightOutput, () -> rightTrait);

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan leftGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), leftLogical));
        GroupPlan rightGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), rightLogical));

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.ASOF_LEFT_INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                leftGroupPlan, rightGroupPlan);

        // simulate physical-tree logical prop recompute by resetting logical properties on the join
        AbstractPhysicalPlan processed = (AbstractPhysicalPlan) join.resetLogicalProperties();
        processed = (AbstractPhysicalPlan) processed.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) join);

        Assertions.assertInstanceOf(PhysicalHashJoin.class, processed);

        DataTrait.Builder builder = new DataTrait.Builder();
        processed.computeUniform(builder);
        DataTrait result = builder.build();

        Assertions.assertTrue(result.isUniformAndNotNull(leftSlot));
        Assertions.assertTrue(result.isUniformAndNotNull(rightSlot));
    }

    @Test
    void testComputeUniformAfterRecomputeLogicalProperties_AsOfRightInner() {
        SlotReference leftSlot = new SlotReference(new ExprId(300), "l2", IntegerType.INSTANCE, false,
                Collections.emptyList());
        SlotReference rightSlot = new SlotReference(new ExprId(301), "r2", IntegerType.INSTANCE, false,
                Collections.emptyList());
        List<Slot> leftOutput = Lists.newArrayList(leftSlot);
        List<Slot> rightOutput = Lists.newArrayList(rightSlot);

        DataTrait.Builder leftBuilder = new DataTrait.Builder();
        leftBuilder.addUniformSlot(leftSlot);
        DataTrait leftTrait = leftBuilder.build();

        DataTrait.Builder rightBuilder = new DataTrait.Builder();
        rightBuilder.addUniformSlot(rightSlot);
        DataTrait rightTrait = rightBuilder.build();

        LogicalProperties leftLogical = new LogicalProperties(() -> leftOutput, () -> leftTrait);
        LogicalProperties rightLogical = new LogicalProperties(() -> rightOutput, () -> rightTrait);

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan leftGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), leftLogical));
        GroupPlan rightGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), rightLogical));

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.ASOF_RIGHT_INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                leftGroupPlan, rightGroupPlan);

        // simulate physical-tree logical prop recompute by resetting logical properties on the join
        AbstractPhysicalPlan processed = (AbstractPhysicalPlan) join.resetLogicalProperties();
        processed = (AbstractPhysicalPlan) processed.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) join);

        Assertions.assertInstanceOf(PhysicalHashJoin.class, processed);

        DataTrait.Builder builder = new DataTrait.Builder();
        processed.computeUniform(builder);
        DataTrait result = builder.build();

        Assertions.assertTrue(result.isUniformAndNotNull(leftSlot));
        Assertions.assertTrue(result.isUniformAndNotNull(rightSlot));
    }

    @Test
    void testComputeUniformAfterRecomputeLogicalProperties_AsOfRightOuter() {
        SlotReference leftSlot = new SlotReference(new ExprId(400), "l3", IntegerType.INSTANCE, false,
                Collections.emptyList());
        SlotReference rightSlot = new SlotReference(new ExprId(401), "r3", IntegerType.INSTANCE, false,
                Collections.emptyList());
        List<Slot> leftOutput = Lists.newArrayList(leftSlot);
        List<Slot> rightOutput = Lists.newArrayList(rightSlot);

        DataTrait.Builder rightBuilder = new DataTrait.Builder();
        rightBuilder.addUniformSlot(rightSlot);
        DataTrait rightTrait = rightBuilder.build();

        LogicalProperties leftLogical = new LogicalProperties(() -> leftOutput, () -> DataTrait.EMPTY_TRAIT);
        LogicalProperties rightLogical = new LogicalProperties(() -> rightOutput, () -> rightTrait);

        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan leftGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), leftLogical));
        GroupPlan rightGroupPlan = new GroupPlan(new Group(idGenerator.getNextId(), rightLogical));

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.ASOF_RIGHT_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties,
                leftGroupPlan, rightGroupPlan);

        // simulate physical-tree logical prop recompute by resetting logical properties on the join
        AbstractPhysicalPlan processed = (AbstractPhysicalPlan) join.resetLogicalProperties();
        processed = (AbstractPhysicalPlan) processed.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) join);

        Assertions.assertInstanceOf(PhysicalHashJoin.class, processed);

        DataTrait.Builder builder = new DataTrait.Builder();
        processed.computeUniform(builder);
        DataTrait result = builder.build();

        Assertions.assertTrue(result.isUniformAndNotNull(rightSlot));
    }

    private SlotReference slot(String name, long uniqueId) {
        return new SlotReference(new ExprId((int) uniqueId), name, IntegerType.INSTANCE, false,
                Collections.emptyList());
    }

    private LogicalProperties setOpLogicalProperties(SlotReference out1, SlotReference out2) {
        List<Slot> outputs = Lists.newArrayList(out1, out2);
        return new LogicalProperties(() -> outputs, () -> DataTrait.EMPTY_TRAIT);
    }

    private PhysicalSetOperation unionOf(List<SlotReference> leftOutput, List<SlotReference> rightOutput,
            SlotReference out1, SlotReference out2) {
        LogicalProperties leftLogical = new LogicalProperties(() -> Lists.newArrayList(leftOutput),
                () -> DataTrait.EMPTY_TRAIT);
        LogicalProperties rightLogical = new LogicalProperties(() -> Lists.newArrayList(rightOutput),
                () -> DataTrait.EMPTY_TRAIT);
        IdGenerator<GroupId> idGenerator = GroupId.createGenerator();
        GroupPlan left = new GroupPlan(new Group(idGenerator.getNextId(), leftLogical));
        GroupPlan right = new GroupPlan(new Group(idGenerator.getNextId(), rightLogical));
        return new PhysicalUnion(Qualifier.ALL, Lists.newArrayList(out1, out2),
                ImmutableList.of(leftOutput, rightOutput), ImmutableList.of(),
                Optional.empty(), setOpLogicalProperties(out1, out2), Lists.newArrayList(left, right));
    }

    /**
     * The generic EXECUTION_BUCKETED path must derive the set operation's output hash spec from
     * the children's specs: same shuffle type, keys mapped to the set operation outputs, and the
     * hash type EXECUTION_BUCKETED always carries (CRC32, per DistributionSpecHash's
     * normalization). Each child's equivalence map must cover every regular child output, as a
     * PhysicalDistribute-derived spec would.
     */
    @Test
    void testSetOperationExecutionOutputDerivesKeys() {
        SlotReference left1 = slot("l1", 1);
        SlotReference left2 = slot("l2", 2);
        SlotReference right1 = slot("r1", 3);
        SlotReference right2 = slot("r2", 4);
        SlotReference out1 = slot("o1", 5);
        SlotReference out2 = slot("o2", 6);
        PhysicalSetOperation setOperation = unionOf(Lists.newArrayList(left1, left2),
                Lists.newArrayList(right1, right2), out1, out2);

        // Each child shuffles on its first output column; its equivalence map must contain every
        // regular child output (the deriver maps each output position through
        // exprIdToEquivalenceSet and bails out with ANY when one is missing).
        PhysicalProperties leftChild = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(left1.getExprId(), left2.getExprId()),
                ShuffleType.EXECUTION_BUCKETED, -1L, -1L, Collections.emptySet(), HashType.CRC32));
        PhysicalProperties rightChild = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(right1.getExprId(), right2.getExprId()),
                ShuffleType.EXECUTION_BUCKETED, -1L, -1L, Collections.emptySet(), HashType.CRC32));
        PhysicalProperties result = new ChildOutputPropertyDeriver(Lists.newArrayList(leftChild, rightChild))
                .getOutputProperties(null, new GroupExpression(setOperation));

        DistributionSpecHash output = Assertions.assertInstanceOf(DistributionSpecHash.class,
                result.getDistributionSpec());
        Assertions.assertEquals(ShuffleType.EXECUTION_BUCKETED, output.getShuffleType());
        Assertions.assertEquals(HashType.CRC32, output.getHashType());
        Assertions.assertEquals(Lists.newArrayList(out1.getExprId(), out2.getExprId()),
                output.getOrderedShuffledColumns());
    }

    /**
     * The storage-layout branch must keep advertising the basic child's layout for a NON-EMPTY key
     * set (table id and partition ids ride along, keyed by set-operation outputs).
     */
    @Test
    void testSetOperationStorageLayoutOutputKeepsLayout() {
        SlotReference left1 = slot("l1", 1);
        SlotReference left2 = slot("l2", 2);
        SlotReference right1 = slot("r1", 3);
        SlotReference right2 = slot("r2", 4);
        SlotReference out1 = slot("o1", 5);
        SlotReference out2 = slot("o2", 6);
        PhysicalSetOperation setOperation = unionOf(Lists.newArrayList(left1, left2),
                Lists.newArrayList(right1, right2), out1, out2);

        PhysicalProperties leftChild = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(left1.getExprId()), ShuffleType.STORAGE_BUCKETED, 100L, 7L,
                Collections.emptySet(), HashType.IDENTITY));
        PhysicalProperties rightChild = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(right1.getExprId()), ShuffleType.STORAGE_BUCKETED, 100L, 7L,
                Collections.emptySet(), HashType.IDENTITY));
        PhysicalProperties result = new ChildOutputPropertyDeriver(Lists.newArrayList(leftChild, rightChild))
                .getOutputProperties(null, new GroupExpression(setOperation));

        DistributionSpecHash output = Assertions.assertInstanceOf(DistributionSpecHash.class,
                result.getDistributionSpec());
        Assertions.assertEquals(ShuffleType.STORAGE_BUCKETED, output.getShuffleType());
        Assertions.assertEquals(HashType.IDENTITY, output.getHashType());
        Assertions.assertEquals(100L, output.getTableId());
        Assertions.assertEquals(Lists.newArrayList(out1.getExprId()), output.getOrderedShuffledColumns());
    }

    /**
     * A set operation whose basic child shuffles on ZERO columns must not advertise a zero-key
     * hash spec: containsSatisfy() is vacuously true on the empty equivalence map, so such a
     * spec satisfies any hash REQUIRE demand and suppresses the parent's exchange. The empty
     * key set must fall through to the generic loop, whose offset mapping cannot resolve any
     * child output and degrades to a non-hash property (ANY/STORAGE_ANY) instead.
     */
    @Test
    void testSetOperationZeroShuffleKeysNormalizesToGather() {
        SlotReference left1 = slot("l1", 1);
        SlotReference left2 = slot("l2", 2);
        SlotReference right1 = slot("r1", 3);
        SlotReference right2 = slot("r2", 4);
        SlotReference out1 = slot("o1", 5);
        SlotReference out2 = slot("o2", 6);
        PhysicalSetOperation setOperation = unionOf(Lists.newArrayList(left1, left2),
                Lists.newArrayList(right1, right2), out1, out2);

        // Zero shuffled columns on the basic child: the storage-layout branch used to accept
        // 0 == 0 and return a zero-key DistributionSpecHash (the hazard); it must now fall
        // through to the generic loop, which returns a non-hash property.
        PhysicalProperties zeroKeyChild = new PhysicalProperties(new DistributionSpecHash(
                Collections.emptyList(), ShuffleType.STORAGE_BUCKETED, 100L, 7L,
                Collections.emptySet(), HashType.IDENTITY));
        PhysicalProperties otherChild = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(right1.getExprId()), ShuffleType.STORAGE_BUCKETED, 100L, 7L,
                Collections.emptySet(), HashType.IDENTITY));
        PhysicalProperties result = new ChildOutputPropertyDeriver(
                Lists.newArrayList(zeroKeyChild, otherChild))
                .getOutputProperties(null, new GroupExpression(setOperation));

        Assertions.assertFalse(result.getDistributionSpec() instanceof DistributionSpecHash,
                "zero shuffled columns must not produce a zero-key hash spec, got: "
                        + result.getDistributionSpec().getClass().getSimpleName());
    }
}
