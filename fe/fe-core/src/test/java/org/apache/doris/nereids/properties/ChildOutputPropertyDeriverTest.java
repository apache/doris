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
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class ChildOutputPropertyDeriverTest {

    @Mocked
    GroupPlan groupPlan;

    @Mocked
    LogicalProperties logicalProperties;

    @Injectable
    ColocateTableIndex colocateTableIndex;

    @BeforeEach
    public void setUp() {
        new MockUp<Env>() {
            @Mock
            ColocateTableIndex getCurrentColocateIndex() {
                return colocateTableIndex;
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            ConnectContext get() {
                return new ConnectContext();
            }
        };
    }

    @Test
    void testInnerJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(2, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testLeftOuterJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.LEFT_OUTER_JOIN,
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        Assertions.assertEquals(-1, actual.getTableId());
        // check merged
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().size());
        Assertions.assertEquals(1, actual.getExprIdToEquivalenceSet().keySet().iterator().next().asInt());
    }

    @Test
    void testRightOuterJoin() {
        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_OUTER_JOIN,
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
    void testBroadcastJoin(@Injectable LogicalProperties p1, @Injectable GroupPlan p2) {
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(3, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    void testShuffleJoin() {
        new MockUp<JoinUtils>() {
            @Mock
            Pair<List<ExprId>, List<ExprId>> getOnClauseUsedSlots(
                    AbstractPhysicalJoin<? extends Plan, ? extends Plan> join) {
                return Pair.of(Lists.newArrayList(new ExprId(0)), Lists.newArrayList(new ExprId(2)));
            }
        };

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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
                RequireProperties.of(PhysicalProperties.GATHER),
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
                RequireProperties.of(PhysicalProperties.createHash(ImmutableList.of(partition), ShuffleType.REQUIRE)),
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
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
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
                RequireProperties.of(PhysicalProperties.GATHER),
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
}
