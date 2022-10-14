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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

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

@SuppressWarnings("unused")
public class RequestPropertyDeriverTest {

    @Mocked
    GroupPlan groupPlan;

    @Mocked
    LogicalProperties logicalProperties;

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
    public void testNestedLoopJoin() {
        PhysicalNestedLoopJoin<GroupPlan, GroupPlan> join = new PhysicalNestedLoopJoin<>(JoinType.CROSS_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testShuffleHashJoin() {
        new MockUp<JoinUtils>() {
            @Mock
            Pair<List<ExprId>, List<ExprId>> getOnClauseUsedSlots(
                    AbstractPhysicalJoin<? extends Plan, ? extends Plan> join) {
                return Pair.of(Lists.newArrayList(new ExprId(0)), Lists.newArrayList(new ExprId(1)));
            }
        };

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(0)), ShuffleType.JOIN)),
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.JOIN))
        ));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testShuffleOrBroadcastHashJoin() {
        new MockUp<JoinUtils>() {
            @Mock
            Pair<List<ExprId>, List<ExprId>> getOnClauseUsedSlots(
                    AbstractPhysicalJoin<? extends Plan, ? extends Plan> join) {
                return Pair.of(Lists.newArrayList(new ExprId(0)), Lists.newArrayList(new ExprId(1)));
            }
        };

        PhysicalHashJoin<GroupPlan, GroupPlan> join = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, logicalProperties, groupPlan, groupPlan);
        GroupExpression groupExpression = new GroupExpression(join);

        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);

        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY, PhysicalProperties.REPLICATED));
        expected.add(Lists.newArrayList(
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(0)), ShuffleType.JOIN)),
                new PhysicalProperties(new DistributionSpecHash(Lists.newArrayList(new ExprId(1)), ShuffleType.JOIN))
        ));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testLocalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalAggregate<GroupPlan> aggregate = new PhysicalAggregate<>(
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                Lists.newArrayList(key),
                AggPhase.LOCAL,
                true,
                false,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.ANY));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testGlobalAggregate() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        SlotReference partition = new SlotReference("partition", IntegerType.INSTANCE);
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
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.createHash(new DistributionSpecHash(
                Lists.newArrayList(partition.getExprId()),
                ShuffleType.AGGREGATE
        ))));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testGlobalAggregateWithoutPartition() {
        SlotReference key = new SlotReference("col1", IntegerType.INSTANCE);
        PhysicalAggregate<GroupPlan> aggregate = new PhysicalAggregate<>(
                Lists.newArrayList(),
                Lists.newArrayList(key),
                Lists.newArrayList(),
                AggPhase.GLOBAL,
                true,
                true,
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(aggregate);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testAssertNumRows() {
        PhysicalAssertNumRows<GroupPlan> assertNumRows = new PhysicalAssertNumRows<>(
                new AssertNumRowsElement(1, "", AssertNumRowsElement.Assertion.EQ),
                logicalProperties,
                groupPlan
        );
        GroupExpression groupExpression = new GroupExpression(assertNumRows);
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(jobContext);
        List<List<PhysicalProperties>> actual
                = requestPropertyDeriver.getRequestChildrenPropertyList(groupExpression);
        List<List<PhysicalProperties>> expected = Lists.newArrayList();
        expected.add(Lists.newArrayList(PhysicalProperties.GATHER));
        Assertions.assertEquals(expected, actual);
    }
}
