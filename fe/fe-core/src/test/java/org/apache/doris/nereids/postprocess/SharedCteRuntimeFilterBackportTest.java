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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class SharedCteRuntimeFilterBackportTest {
    private final CTEId cteId = new CTEId(1);
    private final SlotReference source = new SlotReference("source", IntegerType.INSTANCE);
    private final SlotReference producer = new SlotReference("producer", IntegerType.INSTANCE);

    @Test
    public void testMinAndMaxCannotReplaceEachOther() {
        RuntimeFilter min = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false);
        RuntimeFilter max = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MAX, false);
        Assertions.assertFalse(RuntimeFilterGenerator.canPushDownRuntimeFiltersIntoCTEProducer(
                ImmutableList.of(min, max), cteId));
        Assertions.assertTrue(RuntimeFilterGenerator.canPushDownRuntimeFiltersIntoCTEProducer(
                ImmutableList.of(min, filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false)), cteId));
    }

    @Test
    public void testDifferentTypesAndNullSemanticsAreNotInterchangeable() {
        RuntimeFilter ordinary = filter(TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX, false);
        Assertions.assertFalse(RuntimeFilterGenerator.canPushDownRuntimeFiltersIntoCTEProducer(
                ImmutableList.of(ordinary,
                        filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN_MAX, false)), cteId));
        Assertions.assertFalse(RuntimeFilterGenerator.canPushDownRuntimeFiltersIntoCTEProducer(
                ImmutableList.of(ordinary,
                        filter(TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX, true)), cteId));
    }

    @Test
    public void testEveryTargetOfTheSameFilterMustMatch() {
        RuntimeFilter filter = filter(TRuntimeFilterType.IN_OR_BLOOM, TMinMaxRuntimeFilterType.MIN_MAX, false);
        SlotReference second = new SlotReference("second", IntegerType.INSTANCE);
        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getCteId()).thenReturn(cteId);
        Mockito.when(consumer.getProducerSlot(second)).thenReturn(
                new SlotReference("different_producer_column", IntegerType.INSTANCE));
        filter.addTargetSlot(second, second, consumer);
        Assertions.assertFalse(RuntimeFilterGenerator.canPushDownRuntimeFiltersIntoCTEProducer(
                ImmutableList.of(filter), cteId));
    }

    @Test
    public void testSourceCannotCrossNullGeneratingSide() {
        Plan deepest = Mockito.mock(Plan.class);
        Plan shallowest = Mockito.mock(Plan.class);
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getJoinType()).thenReturn(JoinType.RIGHT_OUTER_JOIN);
        Mockito.when(join.child(0)).thenReturn(deepest);
        Plan right = Mockito.mock(Plan.class);
        Mockito.when(join.child(1)).thenReturn(right);
        Assertions.assertFalse(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(deepest, join), shallowest));
        Assertions.assertTrue(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(right, join), shallowest));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
        Assertions.assertTrue(RuntimeFilterGenerator.keepsSourceValue(
                ImmutableList.of(deepest, join), shallowest));
    }

    @Test
    public void testIdentityKeepsDirectionsAndNullSemanticsSeparate() {
        RuntimeFilter first = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false);
        RuntimeFilterContext context = new RuntimeFilterContext(new SessionVariable(),
                RuntimeFilterId.createGenerator());
        RuntimeFilter max = copyFilter(first, TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MAX, 0);
        context.setRuntimeFilterIdentityToFilter(source, first.getType(), first.getBuilderNode(), first);
        context.setRuntimeFilterIdentityToFilter(source, max.getType(), max.getBuilderNode(), max);
        Assertions.assertSame(first, context.getRuntimeFilterBySrcAndType(source, first.getType(),
                first.getBuilderNode(), TMinMaxRuntimeFilterType.MIN, 0));
        Assertions.assertSame(max, context.getRuntimeFilterBySrcAndType(source, first.getType(),
                first.getBuilderNode(), TMinMaxRuntimeFilterType.MAX, 0));

        RuntimeFilter ordinary = copyFilter(first, TRuntimeFilterType.IN_OR_BLOOM,
                TMinMaxRuntimeFilterType.MIN_MAX, 0);
        RuntimeFilter nullSafe = copyFilter(first, TRuntimeFilterType.IN_OR_BLOOM,
                TMinMaxRuntimeFilterType.MIN_MAX, 1);
        Mockito.when(first.getBuilderNode().getHashJoinConjuncts()).thenReturn(ImmutableList.of(
                new EqualTo(first.getTargetSlots().get(0), source),
                new NullSafeEqual(first.getTargetSlots().get(0), source)));
        context.setRuntimeFilterIdentityToFilter(source, ordinary.getType(), ordinary.getBuilderNode(), ordinary);
        context.setRuntimeFilterIdentityToFilter(source, nullSafe.getType(), nullSafe.getBuilderNode(), nullSafe);
        Assertions.assertSame(ordinary, context.getRuntimeFilterBySrcAndType(source, ordinary.getType(),
                ordinary.getBuilderNode(), TMinMaxRuntimeFilterType.MIN_MAX, 0));
        Assertions.assertSame(nullSafe, context.getRuntimeFilterBySrcAndType(source, nullSafe.getType(),
                nullSafe.getBuilderNode(), TMinMaxRuntimeFilterType.MIN_MAX, 1));
        // A non-null-aware decoupled filter still shares the ordinary equality filter.
        Assertions.assertSame(ordinary, context.getRuntimeFilterBySrcAndType(source, ordinary.getType(),
                ordinary.getBuilderNode(), TMinMaxRuntimeFilterType.MIN_MAX, -1));
    }

    @Test
    public void testSelectCommonIdentitiesWithoutLosingOtherFilterTypes() {
        RuntimeFilter first = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false);
        RuntimeFilter second = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false);
        RuntimeFilter firstBloom = copyFilter(first, TRuntimeFilterType.IN_OR_BLOOM,
                TMinMaxRuntimeFilterType.MIN_MAX, 0);
        RuntimeFilter secondBloom = copyFilter(second, TRuntimeFilterType.IN_OR_BLOOM,
                TMinMaxRuntimeFilterType.MIN_MAX, 0);
        RuntimeFilter firstMax = copyFilter(first, TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MAX, 0);
        List<List<RuntimeFilter>> selected = RuntimeFilterGenerator.selectPushableRuntimeFilters(
                ImmutableList.of(first, second, firstBloom, secondBloom, firstMax),
                ImmutableSet.of((PhysicalCTEConsumer) first.getTargetScans().get(0),
                        (PhysicalCTEConsumer) second.getTargetScans().get(0)), cteId);
        Assertions.assertEquals(2, selected.size());
        Assertions.assertTrue(selected.contains(ImmutableList.of(first, second)));
        Assertions.assertTrue(selected.contains(ImmutableList.of(firstBloom, secondBloom)));
    }

    @Test
    public void testOppositeDirectionsDoNotCoverEveryConsumer() {
        RuntimeFilter min = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MIN, false);
        RuntimeFilter max = filter(TRuntimeFilterType.MIN_MAX, TMinMaxRuntimeFilterType.MAX, false);
        Assertions.assertTrue(RuntimeFilterGenerator.selectPushableRuntimeFilters(ImmutableList.of(min, max),
                ImmutableSet.of((PhysicalCTEConsumer) min.getTargetScans().get(0),
                        (PhysicalCTEConsumer) max.getTargetScans().get(0)), cteId).isEmpty());
    }

    private RuntimeFilter copyFilter(RuntimeFilter original, TRuntimeFilterType type,
            TMinMaxRuntimeFilterType direction, int exprOrder) {
        return new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(), source,
                original.getTargetSlots(), original.getTargetExpressions(), type, exprOrder,
                original.getBuilderNode(), -1L, true, direction, original.getTargetScans().get(0));
    }

    private RuntimeFilter filter(TRuntimeFilterType type, TMinMaxRuntimeFilterType direction, boolean nullSafe) {
        SlotReference target = new SlotReference("target", IntegerType.INSTANCE);
        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getCteId()).thenReturn(cteId);
        Mockito.when(consumer.getProducerSlot(target)).thenReturn(producer);
        PhysicalHashJoin<?, ?> builder = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(builder.getHashJoinConjuncts()).thenReturn(ImmutableList.of(
                nullSafe ? new NullSafeEqual(target, source) : new EqualTo(target, source)));
        return new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(), source,
                ImmutableList.of(target), ImmutableList.of(target), type, 0, builder, -1L, true, direction, consumer);
    }
}
