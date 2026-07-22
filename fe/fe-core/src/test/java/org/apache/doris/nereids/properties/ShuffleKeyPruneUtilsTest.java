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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
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

    @Override
    protected void runBeforeAll() {
        slotA = new SlotReference(new ExprId(0), "a", IntegerType.INSTANCE, true, ImmutableList.of());
        slotB = new SlotReference(new ExprId(1), "b", IntegerType.INSTANCE, true, ImmutableList.of());
        slotC = new SlotReference(new ExprId(2), "c", IntegerType.INSTANCE, true, ImmutableList.of());
        slotD = new SlotReference(new ExprId(3), "d", IntegerType.INSTANCE, true, ImmutableList.of());
        slotE = new SlotReference(new ExprId(4), "e", IntegerType.INSTANCE, true, ImmutableList.of());
        slotF = new SlotReference(new ExprId(5), "f", IntegerType.INSTANCE, true, ImmutableList.of());
        connectContext = Mockito.spy(connectContext);
        Mockito.doReturn(INSTANCE_NUM).when(connectContext).getTotalInstanceNum();
    }

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

    // ===== Helper to build ColumnStatistic with null hotValues =====

    private static Statistics statsWithNullHotValues(double ndv, double rowCount, Expression... slots) {
        Map<Expression, ColumnStatistic> map = new HashMap<>();
        for (Expression slot : slots) {
            ColumnStatistic col = new ColumnStatisticBuilder(1)
                    .setNdv(ndv)
                    .setAvgSizeByte(4)
                    .setNumNulls(0)
                    .setMinValue(0)
                    .setMaxValue(100)
                    .setIsUnknown(false)
                    .setUpdatedTime("")
                    // intentionally NOT calling setHotValues → hotValues remains null
                    .build();
            map.put(slot, col);
        }
        return new Statistics(rowCount, map);
    }

    private static Statistics statsWithUnknown(double rowCount, Expression... slots) {
        Map<Expression, ColumnStatistic> map = new HashMap<>();
        for (Expression slot : slots) {
            ColumnStatistic col = new ColumnStatisticBuilder(1)
                    .setNdv(5000)
                    .setAvgSizeByte(4)
                    .setNumNulls(0)
                    .setIsUnknown(true)
                    .setUpdatedTime("")
                    .setHotValues(new HashMap<>())
                    .build();
            map.put(slot, col);
        }
        return new Statistics(rowCount, map);
    }

    // ===== Additional tests for selectBestShuffleKeyForAgg =====

    @Test
    void testSelectBestShuffleKeyForAgg_emptyPartitionExprsReturnsEmpty() {
        // partitionExprs is empty → slotRefs.isEmpty() → return empty
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(slotA), false);
        Statistics stats = statsWithDefaultNdv(slotA);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_nonSlotRefPartitionExprReturnsEmpty() {
        // partitionExprs contains only non-SlotReference expressions → slotRefs.isEmpty() → empty
        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(slotA), false);
        Statistics stats = statsWithDefaultNdv(slotA);
        Expression nonSlotExpr = Mockito.mock(Expression.class);

        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(nonSlotExpr), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_unknownColumnStatsReturnsEmpty() {
        // Column stats with isUnKnown=true → return empty
        SlotReference slot = new SlotReference(new ExprId(20), "x", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Statistics stats = statsWithUnknown(ROW_COUNT, slot);

        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(slot), false);
        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(slot), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_hotValuesNullReturnsEmpty() {
        // Column stats exist but hotValues is null → return empty
        SlotReference slot = new SlotReference(new ExprId(21), "y", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Statistics stats = statsWithNullHotValues(5000.0, ROW_COUNT, slot);

        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(slot), false);
        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(slot), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_allStringsNoneBalancedNoNumericFallbackEmpty() {
        // All partition slots are strings, none balanced (NDV=100 < 4096), no numeric/date for step 2 → empty
        SlotReference strSlot1 = new SlotReference(new ExprId(22), "s1", new VarcharType(64), true,
                ImmutableList.of());
        SlotReference strSlot2 = new SlotReference(new ExprId(23), "s2", new VarcharType(64), true,
                ImmutableList.of());
        Map<Expression, Double> ndvMap = new HashMap<>();
        ndvMap.put(strSlot1, 100.0);
        ndvMap.put(strSlot2, 100.0);
        Statistics stats = statsWithNdv(ndvMap, ROW_COUNT);

        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(
                ImmutableList.of(strSlot1, strSlot2), false);
        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(strSlot1, strSlot2), stats, connectContext);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testSelectBestShuffleKeyForAgg_numericInsufficientCombinedNdvFallbackEmpty() {
        // Single numeric slot with NDV=100 (not balanced), combinedNdv=100 < 4096 → fallback empty
        SlotReference numSlot = new SlotReference(new ExprId(24), "n", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Map<Expression, Double> ndvMap = new HashMap<>();
        ndvMap.put(numSlot, 100.0);
        Statistics stats = statsWithNdv(ndvMap, ROW_COUNT);

        PhysicalHashAggregate<PhysicalEmptyRelation> agg = createAgg(ImmutableList.of(numSlot), false);
        Optional<List<Expression>> result = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg,
                ImmutableList.of(numSlot), stats, connectContext);

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

    // ===== Tests for tryFindOptimalShuffleKeyForJoinWithDistributeColumns =====

    @Test
    void testTryFindOptimalJoin_nullLeftStatsReturnsEmpty() {
        Statistics rightStats = statsWithDefaultNdv(slotB);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(slotA), ImmutableList.<Slot>of(slotB),
                        ImmutableList.of(slotA.getExprId()), ImmutableList.of(slotB.getExprId()),
                        null, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_nullRightStatsReturnsEmpty() {
        Statistics leftStats = statsWithDefaultNdv(slotA);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(slotA), ImmutableList.<Slot>of(slotB),
                        ImmutableList.of(slotA.getExprId()), ImmutableList.of(slotB.getExprId()),
                        leftStats, null);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_mismatchedColumnSizesReturnsEmpty() {
        // leftOrderedShuffledColumns.size() != rightOrderedShuffledColumns.size()
        Statistics leftStats = statsWithDefaultNdv(slotA, slotB);
        Statistics rightStats = statsWithDefaultNdv(slotC);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(slotA, slotB), ImmutableList.<Slot>of(slotC),
                        ImmutableList.of(slotA.getExprId(), slotB.getExprId()),
                        ImmutableList.of(slotC.getExprId()),
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_mismatchedIdSizesReturnsEmpty() {
        // leftOrderedShuffledColumnId.size() != rightOrderedShuffledColumnId.size()
        Statistics leftStats = statsWithDefaultNdv(slotA);
        Statistics rightStats = statsWithDefaultNdv(slotB);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(slotA), ImmutableList.<Slot>of(slotB),
                        ImmutableList.of(slotA.getExprId(), slotB.getExprId()), // size=2
                        ImmutableList.of(slotC.getExprId()),                    // size=1
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_unknownColumnStatsReturnsEmpty() {
        // Any column with isUnKnown=true → return empty
        SlotReference leftSlot = new SlotReference(new ExprId(30), "l1", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightSlot = new SlotReference(new ExprId(31), "r1", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Statistics leftStats = statsWithUnknown(ROW_COUNT, leftSlot);
        Statistics rightStats = statsWithDefaultNdv(rightSlot);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftSlot), ImmutableList.<Slot>of(rightSlot),
                        ImmutableList.of(leftSlot.getExprId()), ImmutableList.of(rightSlot.getExprId()),
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_hotValuesNullReturnsEmpty() {
        // Any column with hotValues=null → return empty
        SlotReference leftSlot = new SlotReference(new ExprId(32), "l2", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightSlot = new SlotReference(new ExprId(33), "r2", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Statistics leftStats = statsWithNullHotValues(5000.0, ROW_COUNT, leftSlot);
        Statistics rightStats = statsWithDefaultNdv(rightSlot);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftSlot), ImmutableList.<Slot>of(rightSlot),
                        ImmutableList.of(leftSlot.getExprId()), ImmutableList.of(rightSlot.getExprId()),
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_singleBalancedKeyPairReturnsOptimal() {
        // 2 pairs: (numeric/balanced, numeric/balanced) and (string/not-balanced, string/not-balanced)
        // Step 1: numeric pair is balanced on both sides → return single pair
        SlotReference leftNum = new SlotReference(new ExprId(34), "ln", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightNum = new SlotReference(new ExprId(35), "rn", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference leftStr = new SlotReference(new ExprId(36), "ls", new VarcharType(64), true,
                ImmutableList.of());
        SlotReference rightStr = new SlotReference(new ExprId(37), "rs", new VarcharType(64), true,
                ImmutableList.of());

        Map<Expression, Double> leftNdvMap = new HashMap<>();
        leftNdvMap.put(leftNum, 5000.0);  // balanced (5000 > 4096)
        leftNdvMap.put(leftStr, 3000.0);  // not balanced
        Map<Expression, Double> rightNdvMap = new HashMap<>();
        rightNdvMap.put(rightNum, 5000.0);
        rightNdvMap.put(rightStr, 3000.0);
        Statistics leftStats = statsWithNdv(leftNdvMap, ROW_COUNT);
        Statistics rightStats = statsWithNdv(rightNdvMap, ROW_COUNT);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftNum, leftStr), ImmutableList.<Slot>of(rightNum, rightStr),
                        ImmutableList.of(leftNum.getExprId(), leftStr.getExprId()),
                        ImmutableList.of(rightNum.getExprId(), rightStr.getExprId()),
                        leftStats, rightStats);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1, result.get().first.size());
        Assertions.assertEquals(leftNum.getExprId(), result.get().first.get(0));
        Assertions.assertEquals(rightNum.getExprId(), result.get().second.get(0));
    }

    @Test
    void testTryFindOptimalJoin_singlePairAlreadyOptimalReturnsEmpty() {
        // Only 1 pair and both sides are balanced → step 1 finds it but size is unchanged → return empty
        SlotReference leftNum = new SlotReference(new ExprId(38), "ln2", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightNum = new SlotReference(new ExprId(39), "rn2", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Statistics leftStats = statsWithNdv(ImmutableMap.of((Expression) leftNum, 5000.0), ROW_COUNT);
        Statistics rightStats = statsWithNdv(ImmutableMap.of((Expression) rightNum, 5000.0), ROW_COUNT);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftNum), ImmutableList.<Slot>of(rightNum),
                        ImmutableList.of(leftNum.getExprId()), ImmutableList.of(rightNum.getExprId()),
                        leftStats, rightStats);

        // No change: original was already 1 key, optimized is also 1 key → toOptionalIfChanged returns empty
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_pruneStringKeysWithSufficientCombinedNdv() {
        // 3 pairs: (int,int), (date,date), (str,str), none individually balanced (NDV=3000 < 4096)
        // Step 2: combined NDV of int+date pairs > threshold → return 2-key subset (no string)
        SlotReference leftInt = new SlotReference(new ExprId(40), "li", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightInt = new SlotReference(new ExprId(41), "ri", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference leftDate = new SlotReference(new ExprId(42), "ld", DateType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightDate = new SlotReference(new ExprId(43), "rd", DateType.INSTANCE, true,
                ImmutableList.of());
        SlotReference leftStr = new SlotReference(new ExprId(44), "ls2", new VarcharType(64), true,
                ImmutableList.of());
        SlotReference rightStr = new SlotReference(new ExprId(45), "rs2", new VarcharType(64), true,
                ImmutableList.of());

        Map<Expression, Double> leftNdvMap = new HashMap<>();
        leftNdvMap.put(leftInt, 3000.0);
        leftNdvMap.put(leftDate, 3000.0);
        leftNdvMap.put(leftStr, 3000.0);
        Map<Expression, Double> rightNdvMap = new HashMap<>();
        rightNdvMap.put(rightInt, 3000.0);
        rightNdvMap.put(rightDate, 3000.0);
        rightNdvMap.put(rightStr, 3000.0);
        Statistics leftStats = statsWithNdv(leftNdvMap, ROW_COUNT);
        Statistics rightStats = statsWithNdv(rightNdvMap, ROW_COUNT);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftInt, leftDate, leftStr),
                        ImmutableList.<Slot>of(rightInt, rightDate, rightStr),
                        ImmutableList.of(leftInt.getExprId(), leftDate.getExprId(), leftStr.getExprId()),
                        ImmutableList.of(rightInt.getExprId(), rightDate.getExprId(), rightStr.getExprId()),
                        leftStats, rightStats);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(2, result.get().first.size());
        Assertions.assertFalse(result.get().first.contains(leftStr.getExprId()));
        Assertions.assertFalse(result.get().second.contains(rightStr.getExprId()));
    }

    @Test
    void testTryFindOptimalJoin_allStringKeysFallbackEmpty() {
        // All join keys are strings, none balanced (NDV=100 < 4096), no numeric/date → fallback empty
        SlotReference leftStr = new SlotReference(new ExprId(46), "ls3", new VarcharType(64), true,
                ImmutableList.of());
        SlotReference rightStr = new SlotReference(new ExprId(47), "rs3", new VarcharType(64), true,
                ImmutableList.of());
        Map<Expression, Double> leftNdvMap = new HashMap<>();
        leftNdvMap.put(leftStr, 100.0);
        Map<Expression, Double> rightNdvMap = new HashMap<>();
        rightNdvMap.put(rightStr, 100.0);
        Statistics leftStats = statsWithNdv(leftNdvMap, ROW_COUNT);
        Statistics rightStats = statsWithNdv(rightNdvMap, ROW_COUNT);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftStr), ImmutableList.<Slot>of(rightStr),
                        ImmutableList.of(leftStr.getExprId()), ImmutableList.of(rightStr.getExprId()),
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testTryFindOptimalJoin_numericInsufficientCombinedNdvFallbackEmpty() {
        // Single numeric pair with NDV=100 (not balanced, combinedNdv=100 < 4096) → fallback empty
        SlotReference leftNum = new SlotReference(new ExprId(48), "ln3", IntegerType.INSTANCE, true,
                ImmutableList.of());
        SlotReference rightNum = new SlotReference(new ExprId(49), "rn3", IntegerType.INSTANCE, true,
                ImmutableList.of());
        Map<Expression, Double> leftNdvMap = new HashMap<>();
        leftNdvMap.put(leftNum, 100.0);
        Map<Expression, Double> rightNdvMap = new HashMap<>();
        rightNdvMap.put(rightNum, 100.0);
        Statistics leftStats = statsWithNdv(leftNdvMap, ROW_COUNT);
        Statistics rightStats = statsWithNdv(rightNdvMap, ROW_COUNT);

        Optional<Pair<List<ExprId>, List<ExprId>>> result =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        connectContext,
                        ImmutableList.<Slot>of(leftNum), ImmutableList.<Slot>of(rightNum),
                        ImmutableList.of(leftNum.getExprId()), ImmutableList.of(rightNum.getExprId()),
                        leftStats, rightStats);

        Assertions.assertFalse(result.isPresent());
    }
}
