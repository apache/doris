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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

class PlanEqualsTest {
    /* *************************** Logical *************************** */
    @Test
    void testLogicalAggregate(@Mocked Plan child) {
        LogicalAggregate<Plan> actual = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);

        LogicalAggregate<Plan> expected = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertEquals(expected, actual);

        LogicalAggregate<Plan> unexpected = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertNotEquals(unexpected, actual);

        unexpected = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                false, Optional.empty(), child);
        Assertions.assertNotEquals(unexpected, actual);

        unexpected = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                true, Optional.empty(), child);
        Assertions.assertNotEquals(unexpected, actual);

        unexpected = new LogicalAggregate<>(Lists.newArrayList(), ImmutableList.of(
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                false, Optional.empty(), child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testLogicalFilter(@Mocked Plan child) {
        LogicalFilter<Plan> actual = new LogicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(1))), child);

        LogicalFilter<Plan> expected = new LogicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(1))), child);
        Assertions.assertEquals(expected, actual);

        LogicalFilter<Plan> unexpected = new LogicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(2))), child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testLogicalJoin(@Mocked Plan left, @Mocked Plan right) {
        LogicalJoin<Plan, Plan> actual = new LogicalJoin<>(JoinType.INNER_JOIN, Lists.newArrayList(new EqualTo(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()),
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                left, right, null);

        LogicalJoin<Plan, Plan> expected = new LogicalJoin<>(JoinType.INNER_JOIN, Lists.newArrayList(new EqualTo(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()),
                new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                left, right, null);
        Assertions.assertEquals(expected, actual);

        LogicalJoin<Plan, Plan> unexpected = new LogicalJoin<>(JoinType.INNER_JOIN, Lists.newArrayList(new EqualTo(
                new SlotReference(new ExprId(2), "a", BigIntType.INSTANCE, false, Lists.newArrayList()),
                new SlotReference(new ExprId(3), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                left, right, null);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testLogicalProject(@Mocked Plan child) {
        LogicalProject<Plan> actual = new LogicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);

        LogicalProject<Plan> expected = new LogicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertEquals(expected, actual);

        LogicalProject<Plan> unexpected1 = new LogicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(1), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertNotEquals(unexpected1, actual);

        LogicalProject<Plan> unexpected2 = new LogicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertNotEquals(unexpected2, actual);
    }

    @Test
    void testLogicalSort(@Mocked Plan child) {
        LogicalSort<Plan> actual = new LogicalSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                child);

        LogicalSort<Plan> expected = new LogicalSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                child);
        Assertions.assertEquals(expected, actual);

        LogicalSort<Plan> unexpected = new LogicalSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(2), "a", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testLogicalResultSink(@Mocked Plan child) {
        LogicalResultSink<Plan> actual = new LogicalResultSink<>(
                ImmutableList.of(new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);

        LogicalResultSink<Plan> expected = new LogicalResultSink<>(
                ImmutableList.of(new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertEquals(expected, actual);

        LogicalResultSink<Plan> unexpected = new LogicalResultSink<>(
                ImmutableList.of(new SlotReference(new ExprId(1), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    /* *************************** Physical *************************** */
    @Test
    void testPhysicalAggregate(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        List<NamedExpression> outputExpressionList = ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()));
        PhysicalHashAggregate<Plan> actual = new PhysicalHashAggregate<>(Lists.newArrayList(), outputExpressionList,
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT), true, logicalProperties,
                RequireProperties.of(PhysicalProperties.GATHER), child);

        List<NamedExpression> outputExpressionList1 = ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()));
        PhysicalHashAggregate<Plan> expected = new PhysicalHashAggregate<>(Lists.newArrayList(),
                outputExpressionList1,
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT), true, logicalProperties,
                RequireProperties.of(PhysicalProperties.GATHER), child);
        Assertions.assertEquals(expected, actual);

        List<NamedExpression> outputExpressionList2 = ImmutableList.of(
                new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()));
        PhysicalHashAggregate<Plan> unexpected = new PhysicalHashAggregate<>(Lists.newArrayList(),
                outputExpressionList2,
                new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT), false, logicalProperties,
                RequireProperties.of(PhysicalProperties.GATHER), child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testPhysicalFilter(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        PhysicalFilter<Plan> actual = new PhysicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(2))),
                logicalProperties, child);

        PhysicalFilter<Plan> expected = new PhysicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(2))),
                logicalProperties, child);
        Assertions.assertEquals(expected, actual);

        PhysicalFilter<Plan> unexpected = new PhysicalFilter<>(ImmutableSet.of(new EqualTo(Literal.of(1), Literal.of(1))),
                logicalProperties, child);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testPhysicalJoin(@Mocked Plan left, @Mocked Plan right, @Mocked LogicalProperties logicalProperties) {
        PhysicalHashJoin<Plan, Plan> actual = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(new EqualTo(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()),
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, left, right);

        PhysicalHashJoin<Plan, Plan> expected = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(new EqualTo(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList()),
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, left, right);
        Assertions.assertEquals(expected, actual);

        PhysicalHashJoin<Plan, Plan> unexpected = new PhysicalHashJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(new EqualTo(
                        new SlotReference(new ExprId(2), "a", BigIntType.INSTANCE, false, Lists.newArrayList()),
                        new SlotReference(new ExprId(3), "b", BigIntType.INSTANCE, true, Lists.newArrayList()))),
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE), Optional.empty(), logicalProperties, left, right);
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testPhysicalOlapScan(
            @Mocked LogicalProperties logicalProperties,
            @Mocked OlapTable olapTable,
            @Mocked DistributionSpecHash distributionSpecHash) {

        List<Long> selectedTabletId = Lists.newArrayList();
        for (Partition partition : olapTable.getAllPartitions()) {
            selectedTabletId.addAll(partition.getBaseIndex().getTabletIdsInOrder());
        }

        RelationId id = StatementScopeIdGenerator.newRelationId();

        PhysicalOlapScan actual = new PhysicalOlapScan(id, olapTable, Lists.newArrayList("a"),
                1L, selectedTabletId, olapTable.getPartitionIds(), distributionSpecHash,
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(), logicalProperties,
                Optional.empty());

        PhysicalOlapScan expected = new PhysicalOlapScan(id, olapTable, Lists.newArrayList("a"),
                1L, selectedTabletId, olapTable.getPartitionIds(), distributionSpecHash,
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(), logicalProperties,
                Optional.empty());
        Assertions.assertEquals(expected, actual);

        PhysicalOlapScan unexpected = new PhysicalOlapScan(id, olapTable, Lists.newArrayList("b"),
                12345L, selectedTabletId, olapTable.getPartitionIds(), distributionSpecHash,
                PreAggStatus.on(), ImmutableList.of(), Optional.empty(), logicalProperties,
                Optional.empty());
        Assertions.assertNotEquals(unexpected, actual);
    }

    @Test
    void testPhysicalProject(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {
        PhysicalProject<Plan> actual = new PhysicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                logicalProperties,
                child);

        PhysicalProject<Plan> expected = new PhysicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(0), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                logicalProperties,
                child);
        Assertions.assertEquals(expected, actual);

        PhysicalProject<Plan> unexpected1 = new PhysicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(1), "a", BigIntType.INSTANCE, true, Lists.newArrayList())),
                logicalProperties,
                child);
        Assertions.assertNotEquals(unexpected1, actual);

        PhysicalProject<Plan> unexpected2 = new PhysicalProject<>(
                ImmutableList.of(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList())),
                logicalProperties,
                child);
        Assertions.assertNotEquals(unexpected2, actual);
    }

    @Test
    void testPhysicalSort(@Mocked Plan child, @Mocked LogicalProperties logicalProperties) {

        PhysicalQuickSort<Plan> actual = new PhysicalQuickSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                SortPhase.LOCAL_SORT, logicalProperties,
                child);

        PhysicalQuickSort<Plan> expected = new PhysicalQuickSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(1), "b", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                SortPhase.LOCAL_SORT, logicalProperties,
                child);
        Assertions.assertEquals(expected, actual);

        PhysicalQuickSort<Plan> unexpected = new PhysicalQuickSort<>(
                ImmutableList.of(new OrderKey(
                        new SlotReference(new ExprId(2), "a", BigIntType.INSTANCE, true, Lists.newArrayList()), true,
                        true)),
                SortPhase.LOCAL_SORT, logicalProperties,
                child);
        Assertions.assertNotEquals(unexpected, actual);
    }
}
