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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class GroupJoinFusionUtilsTest {

    @Test
    void testCrossSideAggregationEligibleWithoutImplementationSelection() {
        SlotReference leftKey = new SlotReference("left_key", IntegerType.INSTANCE);
        SlotReference rightKey = new SlotReference("right_key", IntegerType.INSTANCE);
        SlotReference leftValue = new SlotReference("left_value", IntegerType.INSTANCE);
        SlotReference rightValue = new SlotReference("right_value", IntegerType.INSTANCE);
        PhysicalPlan left = Mockito.mock(PhysicalPlan.class);
        PhysicalPlan right = Mockito.mock(PhysicalPlan.class);
        Mockito.when(left.getOutputSet()).thenReturn(Set.of(leftKey, leftValue));
        Mockito.when(right.getOutputSet()).thenReturn(Set.of(rightKey, rightValue));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.doReturn(left).when(join).left();
        Mockito.doReturn(right).when(join).right();
        Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        EqualTo conjunct = new EqualTo(leftKey, rightKey);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(conjunct));
        AggregateExpression sum = new AggregateExpression(
                new Sum(new Add(leftValue, rightValue)), AggregateParam.GLOBAL_RESULT);
        PhysicalHashAggregate<?> aggregate = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.GLOBAL_RESULT);
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(leftKey));
        Mockito.when(aggregate.getOutputExpressions()).thenReturn(List.of(leftKey, new Alias(sum, "sum")));
        Mockito.when(aggregate.getInputSlots()).thenReturn(Set.of(leftKey, leftValue, rightValue));

        Assertions.assertEquals(List.of(conjunct),
                GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
        Mockito.when(join.getFilterConjuncts()).thenReturn(List.of(conjunct));
        Assertions.assertNull(GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
        Mockito.when(join.getFilterConjuncts()).thenReturn(List.of());
        Mockito.when(join.getJoinType()).thenReturn(JoinType.CROSS_JOIN);
        Assertions.assertNull(GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
        Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.LOCAL_BUFFER);
        Assertions.assertNull(GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
    }

    @Test
    void testFloatingAndNestedKeysFallbackBeforePartitioning() {
        for (DataType type : List.of(FloatType.INSTANCE, DoubleType.INSTANCE,
                ArrayType.of(DoubleType.INSTANCE))) {
            SlotReference leftKey = new SlotReference("left_key", type);
            SlotReference rightKey = new SlotReference("right_key", type);
            SlotReference value = new SlotReference("value", IntegerType.INSTANCE);
            PhysicalPlan left = Mockito.mock(PhysicalPlan.class);
            PhysicalPlan right = Mockito.mock(PhysicalPlan.class);
            Mockito.when(left.getOutputSet()).thenReturn(Set.of(leftKey, value));
            Mockito.when(right.getOutputSet()).thenReturn(Set.of(rightKey));
            PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
            Mockito.doReturn(left).when(join).left();
            Mockito.doReturn(right).when(join).right();
            Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
            Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(leftKey, rightKey)));
            PhysicalHashAggregate<?> aggregate = Mockito.mock(PhysicalHashAggregate.class);
            Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.GLOBAL_RESULT);
            Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(leftKey));
            Mockito.when(aggregate.getOutputExpressions()).thenReturn(List.of(leftKey,
                    new Alias(new AggregateExpression(new Sum(value), AggregateParam.GLOBAL_RESULT), "sum")));
            Mockito.when(aggregate.getInputSlots()).thenReturn(Set.of(leftKey, value));
            Assertions.assertNull(GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
        }
    }

    @Test
    void testJoinProjectInliningPreservesOutputIdentity() {
        SlotReference key = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference left = new SlotReference("left", IntegerType.INSTANCE);
        SlotReference right = new SlotReference("right", IntegerType.INSTANCE);
        Alias computed = new Alias(new Add(left, right), "computed");
        Alias output = new Alias(new Add(computed.toSlot(), computed.toSlot()), "result");
        Aggregate<?> aggregate = Mockito.mock(Aggregate.class);
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(key));
        Mockito.when(aggregate.getOutputExpressions()).thenReturn(List.of(output));
        AtomicReference<List<NamedExpression>> rewritten = new AtomicReference<>();
        Mockito.when(aggregate.withAggOutput(Mockito.anyList())).thenAnswer(invocation -> {
            rewritten.set(invocation.getArgument(0));
            return aggregate;
        });
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getProjects()).thenReturn(List.of(key, computed));
        Assertions.assertSame(aggregate, GroupJoinFusionUtils.inlineJoinBasedProject(aggregate, project));
        Alias actual = (Alias) rewritten.get().get(0);
        Assertions.assertEquals(output.getExprId(), actual.getExprId());
        Assertions.assertEquals(new Add(computed.child(), computed.child()), actual.child());
        Assertions.assertEquals(List.of(left, right).size(), actual.getInputSlots().size());
    }

    @Test
    void testJoinProjectRejectsComputedGroupingAndNestedVolatileExpression() {
        SlotReference key = new SlotReference("k", IntegerType.INSTANCE);
        Alias computed = new Alias(new Add(key, key), "computed");
        Aggregate<?> aggregate = Mockito.mock(Aggregate.class);
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(computed.toSlot()));
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getProjects()).thenReturn(List.of(computed));
        Assertions.assertNull(GroupJoinFusionUtils.inlineJoinBasedProject(aggregate, project));
        Alias volatileOutput = new Alias(new Add(new Random(), new Random()), "volatile");
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(key));
        Mockito.when(project.getProjects()).thenReturn(List.of(key, volatileOutput));
        Assertions.assertNull(GroupJoinFusionUtils.inlineJoinBasedProject(aggregate, project));
        Alias errorCheck = new Alias(
                new AssertTrue(BooleanLiteral.TRUE, new StringLiteral("check")), "checked");
        Mockito.when(project.getProjects()).thenReturn(List.of(key, errorCheck));
        Assertions.assertNull(GroupJoinFusionUtils.inlineJoinBasedProject(aggregate, project));
    }

    @Test
    void testSessionVarGuardExpressionRejectsFusion() {
        SlotReference leftKey = new SlotReference("left_key", IntegerType.INSTANCE);
        SlotReference rightKey = new SlotReference("right_key", IntegerType.INSTANCE);
        SlotReference leftValue = new SlotReference("left_value", IntegerType.INSTANCE);
        PhysicalPlan left = Mockito.mock(PhysicalPlan.class);
        PhysicalPlan right = Mockito.mock(PhysicalPlan.class);
        Mockito.when(left.getOutputSet()).thenReturn(Set.of(leftKey, leftValue));
        Mockito.when(right.getOutputSet()).thenReturn(Set.of(rightKey));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.doReturn(left).when(join).left();
        Mockito.doReturn(right).when(join).right();
        Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        EqualTo conjunct = new EqualTo(leftKey, rightKey);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(conjunct));
        AggregateExpression sum = new AggregateExpression(
                new Sum(leftValue), AggregateParam.GLOBAL_RESULT);
        PhysicalHashAggregate<?> aggregate = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(aggregate.getAggregateParam()).thenReturn(AggregateParam.GLOBAL_RESULT);
        Mockito.when(aggregate.getGroupByExpressions()).thenReturn(List.of(leftKey));
        Mockito.when(aggregate.getInputSlots()).thenReturn(Set.of(leftKey, leftValue));

        // Unguarded aggregate over the join is eligible.
        Mockito.when(aggregate.getOutputExpressions())
                .thenReturn(List.of(leftKey, new Alias(sum, "sum")));
        Assertions.assertEquals(List.of(conjunct),
                GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));

        // A view/MV output bound under a different result-affecting session variable (e.g.
        // enable_decimal256) is wrapped in SessionVarGuardExpr. Fusing it would translate the bare
        // aggregate and compute its signature with the caller's variables, so it must stay on the
        // regular HashJoin + Aggregate path.
        SessionVarGuardExpr guarded = new SessionVarGuardExpr(
                sum, Map.of("enable_decimal256", "true"));
        Mockito.when(aggregate.getOutputExpressions())
                .thenReturn(List.of(leftKey, new Alias(guarded, "sum")));
        Assertions.assertNull(
                GroupJoinFusionUtils.alignedConjunctsForGroupJoin(aggregate, null, join));
    }
}
