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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private GroupExpression emptyExpression;

    @Override
    protected void runBeforeAll() {
        slotA = new SlotReference(new ExprId(0), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        slotB = new SlotReference(new ExprId(1), "b", IntegerType.INSTANCE, true, ImmutableList.of());
        slotC = new SlotReference(new ExprId(2), "c", IntegerType.INSTANCE, true, ImmutableList.of());
        slotD = new SlotReference(new ExprId(3), "d", IntegerType.INSTANCE, true, ImmutableList.of());
        slotE = new SlotReference(new ExprId(4), "e", IntegerType.INSTANCE, true, ImmutableList.of());
        slotF = new SlotReference(new ExprId(5), "f", IntegerType.INSTANCE, true, ImmutableList.of());
        emptyExpression = new GroupExpression(new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), null));
        connectContext = Mockito.spy(connectContext);
        Mockito.doReturn(INSTANCE_NUM).when(connectContext).getTotalInstanceNum();
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

    /** Statistics with ndv=5000 per slot (aligned with {@link #createChildGroupWithStats}). */
    private static Statistics statsWithDefaultNdv(Expression... slots) {
        ImmutableMap.Builder<Expression, Double> builder = ImmutableMap.builder();
        for (Expression slot : slots) {
            builder.put(slot, 5000.0);
        }
        return statsWithNdv(builder.build(), ROW_COUNT);
    }

    /** Global aggregate over empty relation; no memo/group wiring. */
    private PhysicalHashAggregate<PhysicalEmptyRelation> createAgg(List<? extends Expression> groupByExprs,
            boolean hasSourceRepeat) {
        PhysicalEmptyRelation child = new PhysicalEmptyRelation(
                connectContext.getStatementContext().getNextRelationId(),
                (List) ImmutableList.copyOf(groupByExprs), null);
        return new PhysicalHashAggregate<>(
                (List) groupByExprs, (List) groupByExprs,
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT),
                true, null, hasSourceRepeat, child);
    }

    @Test
    void testSelectBestShuffleKeyForAgg_noColumnStatisticsForPartitionSlots() {
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);
        Statistics childStatsWithoutColumns = new Statistics(ROW_COUNT, new HashMap<>());

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                agg.getGroupByExpressions(), childStatsWithoutColumns, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_selectBestKey() {
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(
                ImmutableList.of(slotA, slotB, slotC, slotD, slotE, slotF), false);
        Statistics stats = statsWithDefaultNdv(slotA, slotB, slotC, slotD, slotE, slotF);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                agg.getGroupByExpressions(), stats, connectContext);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertFalse(result.get().isEmpty());
        Assertions.assertInstanceOf(SlotReference.class, result.get().get(0));
    }

    @Test
    void testSelectBestShuffleKeyForAgg_noChangeReturnsEmpty() {
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(slotA), false);
        Statistics stats = statsWithDefaultNdv(slotA);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(slotA), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
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
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(
                ImmutableList.of(numericSlot, dateSlot, stringSlot), false);
        Statistics stats = statsWithNdv(exprToNdv, ROW_COUNT);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(numericSlot, dateSlot, stringSlot), stats, connectContext);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(2, result.get().size());
        Assertions.assertEquals(numericSlot, result.get().get(0));
        Assertions.assertEquals(dateSlot, result.get().get(1));
    }
}
