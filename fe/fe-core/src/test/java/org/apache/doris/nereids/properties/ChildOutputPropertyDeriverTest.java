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
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class ChildOutputPropertyDeriverTest {

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
    public void testColocateJoin() {

        new Expectations() {
            {
                colocateTableIndex.isSameGroup(0, 1);
                result = true;

                colocateTableIndex.getGroup(0);
                result = new GroupId(0, 0);

                colocateTableIndex.isGroupUnstable(new GroupId(0, 0));
                result = false;
            }
        };

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

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

        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(2, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    public void testBroadcastJoin() {
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
                ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

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

        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.NATURAL, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(2, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    public void testShuffleJoin() {
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
                ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

        Map<ExprId, Integer> leftMap = Maps.newHashMap();
        leftMap.put(new ExprId(0), 0);
        leftMap.put(new ExprId(1), 0);
        PhysicalProperties left = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(0)),
                ShuffleType.ENFORCED,
                0,
                Sets.newHashSet(0L),
                ImmutableList.of(Sets.newHashSet(new ExprId(0), new ExprId(1))),
                leftMap
        ));

        PhysicalProperties right = new PhysicalProperties(new DistributionSpecHash(
                Lists.newArrayList(new ExprId(2)),
                ShuffleType.ENFORCED,
                1,
                Sets.newHashSet(1L)
        ));

        List<PhysicalProperties> childrenOutputProperties = Lists.newArrayList(left, right);
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(childrenOutputProperties);

        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.BUCKETED, actual.getShuffleType());
        // check merged
        Assertions.assertEquals(3, actual.getExprIdToEquivalenceSet().size());
    }

    @Test
    public void testNestedLoopJoin() {
        PhysicalNestedLoopJoin<GroupPlan, GroupPlan> join = new PhysicalNestedLoopJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

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

        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(leftHash, actual);
    }

    @Test
    public void testLocalPhaseAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalAggregate<GroupPlan> aggregate = new PhysicalAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                AggPhase.LOCAL,
                true,
                true,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertEquals(child.getDistributionSpec(), result.getDistributionSpec());
    }

    @Test
    public void testGlobalPhaseAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference partition = new SlotReference("col2", BigIntType.INSTANCE);
        PhysicalAggregate<GroupPlan> aggregate = new PhysicalAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                Lists.newArrayList(partition),
                AggPhase.GLOBAL,
                true,
                true,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        DistributionSpecHash childHash = new DistributionSpecHash(Lists.newArrayList(partition.getExprId()),
                ShuffleType.BUCKETED);
        PhysicalProperties child = new PhysicalProperties(childHash,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertTrue(result.getOrderSpec().getOrderKeys().isEmpty());
        Assertions.assertTrue(result.getDistributionSpec() instanceof DistributionSpecHash);
        DistributionSpecHash actual = (DistributionSpecHash) result.getDistributionSpec();
        Assertions.assertEquals(ShuffleType.BUCKETED, actual.getShuffleType());
        Assertions.assertEquals(Lists.newArrayList(partition).stream()
                        .map(SlotReference::getExprId).collect(Collectors.toList()),
                actual.getOrderedShuffledColumns());
    }

    @Test
    public void testAggregateWithoutGroupBy() {
        PhysicalAggregate<GroupPlan> aggregate = new PhysicalAggregate<>(
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                AggPhase.GLOBAL,
                true,
                true,
                logicalProperties,
                groupPlan
        );

        GroupExpression groupExpression = new GroupExpression(aggregate);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecGather.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(PhysicalProperties.GATHER, result);
    }

    @Test
    public void testLocalQuickSort() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalLocalQuickSort<GroupPlan> sort = new PhysicalLocalQuickSort(orderKeys, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecReplicated.INSTANCE, result.getDistributionSpec());
    }

    @Test
    public void testQuickSort() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalQuickSort<GroupPlan> sort = new PhysicalQuickSort<>(orderKeys, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    public void testTopN() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalTopN<GroupPlan> sort = new PhysicalTopN<>(orderKeys, 10, 10, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(sort);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(Lists.newArrayList(
                        new OrderKey(new SlotReference("ignored", IntegerType.INSTANCE), true, true))));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    public void testLimit() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, true, true));
        PhysicalLimit<GroupPlan> limit = new PhysicalLimit<>(10, 10, logicalProperties, groupPlan);
        GroupExpression groupExpression = new GroupExpression(limit);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE,
                new OrderSpec(orderKeys));

        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(orderKeys, result.getOrderSpec().getOrderKeys());
        Assertions.assertEquals(DistributionSpecGather.INSTANCE, result.getDistributionSpec());
    }

    @Test
    public void testAssertNumRows() {
        PhysicalAssertNumRows<GroupPlan> assertNumRows = new PhysicalAssertNumRows<>(
                new AssertNumRowsElement(1, "", AssertNumRowsElement.Assertion.EQ),
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(assertNumRows);
        PhysicalProperties child = new PhysicalProperties(DistributionSpecReplicated.INSTANCE, new OrderSpec());
        ChildOutputPropertyDeriver deriver = new ChildOutputPropertyDeriver(Lists.newArrayList(child));
        PhysicalProperties result = deriver.getOutputProperties(groupExpression);
        Assertions.assertEquals(PhysicalProperties.GATHER, result);
    }
}
