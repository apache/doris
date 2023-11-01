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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

class RequestPropertyDeriverTest {

    @Mocked
    GroupPlan groupPlan;

    @Mocked
    LogicalProperties logicalProperties;

    @Mocked
    ConnectContext connectContext;

    @Injectable
    Group group;

    @Injectable
    JobContext jobContext;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                jobContext.getRequiredProperties();
                result = PhysicalProperties.ANY;
            }
        };
    }

    @Test
    void testNestedLoopJoin() {
        PhysicalNestedLoopJoin<GroupPlan, GroupPlan> join = new PhysicalNestedLoopJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, Optional.empty(), logicalProperties,
                groupPlan,
                groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);
        new Group(null, groupExpression, null);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testShuffleHashJoin() {
        new MockUp<PhysicalHashJoin>() {
            @Mock
            Pair<List<ExprId>, List<ExprId>> getHashConjunctsExprIds() {
                return Pair.of(Lists.newArrayList(new ExprId(0)), Lists.newArrayList(new ExprId(1)));
            }
        };

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE, Optional.empty(),
                logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join, Lists.newArrayList(group, group));
        new Group(null, groupExpression, null);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(
                new PhysicalProperties(
                        new DistributionSpecHash(Lists.newArrayList(new ExprId(0)), ShuffleType.REQUIRE)),
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.REQUIRE))
        ));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testShuffleOrBroadcastHashJoin() {
        new MockUp<PhysicalHashJoin>() {
            @Mock
            Pair<List<ExprId>, List<ExprId>> getHashConjunctsExprIds() {
                return Pair.of(Lists.newArrayList(new ExprId(0)), Lists.newArrayList(new ExprId(1)));
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            ConnectContext get() {
                return connectContext;
            }
        };

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, JoinHint.NONE, Optional.empty(),
                logicalProperties,
                groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join, Lists.newArrayList(group, group));
        new Group(null, groupExpression, null);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(
                new PhysicalProperties(
                        new DistributionSpecHash(Lists.newArrayList(new ExprId(0)), ShuffleType.REQUIRE)),
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.REQUIRE))
        ));
        expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testLocalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT),
                true,
                logicalProperties,
                RequireProperties.of(PhysicalProperties.ANY),
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testGlobalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference partition = new SlotReference("partition", IntegerType.INSTANCE);
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
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(new DistributionSpecHash(
                Lists.newArrayList(partition.getExprId()),
                ShuffleType.REQUIRE
        ))));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testGlobalAggregateWithoutPartition() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalHashAggregate<GroupPlan> aggregate = new PhysicalHashAggregate<>(
                Lists.newArrayList(),
                Lists.newArrayList(key),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true,
                logicalProperties,
                RequireProperties.of(PhysicalProperties.GATHER),
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        new Group(null, groupExpression, null);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
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
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(null, jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }
}
