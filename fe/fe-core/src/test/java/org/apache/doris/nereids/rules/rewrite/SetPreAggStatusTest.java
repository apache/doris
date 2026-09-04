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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;

/**
 * Unit tests for the pre-agg dispatch logic in {@link SetPreAggStatus}.
 *
 * <p>The decision logic lives in the private static class SetOlapScanPreAgg
 * (checkAggregateFunctions / checkAggWithKeyAndValueSlots / createPreAggStatus),
 * so we reach it via reflection, mirroring TabletSlidingWindowAccessStatsTest.
 * The dispatch matrix covered here (pure-key multi-arg, mixed multi-arg,
 * pure-value IF path, volatile guards, ownership fence) is otherwise only
 * exercised by explain regression tests.
 */
class SetPreAggStatusTest {

    private static final Method CHECK_AGG_FUNCS;
    private static final Method CHECK_AGG_WITH_KEY_VALUE;
    private static final Method CREATE_PRE_AGG_STATUS;
    private static final Object SET_OLAP_SCAN_PRE_AGG_INSTANCE;

    static {
        try {
            Class<?> clazz = Class.forName(
                    "org.apache.doris.nereids.rules.rewrite.SetPreAggStatus$SetOlapScanPreAgg");
            CHECK_AGG_FUNCS = clazz.getDeclaredMethod(
                    "checkAggregateFunctions", Set.class, Set.class, Set.class);
            CHECK_AGG_FUNCS.setAccessible(true);
            CHECK_AGG_WITH_KEY_VALUE = clazz.getDeclaredMethod(
                    "checkAggWithKeyAndValueSlots", AggregateFunction.class, Set.class);
            CHECK_AGG_WITH_KEY_VALUE.setAccessible(true);
            CREATE_PRE_AGG_STATUS = clazz.getDeclaredMethod(
                    "createPreAggStatus", LogicalOlapScan.class, SetPreAggStatus.PreAggInfoContext.class);
            CREATE_PRE_AGG_STATUS.setAccessible(true);
            Field instance = clazz.getDeclaredField("INSTANCE");
            instance.setAccessible(true);
            SET_OLAP_SCAN_PRE_AGG_INSTANCE = instance.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static int exprIdCounter = 0;

    private static SlotReference keySlot(String name) {
        return slot(name, new Column(name, Type.INT, true, AggregateType.NONE, null, ""));
    }

    private static SlotReference valueSlot(String name, AggregateType aggregateType) {
        return slot(name, new Column(name, Type.INT, false, aggregateType, null, ""));
    }

    private static SlotReference slot(String name, Column column) {
        return new SlotReference(new ExprId(exprIdCounter++), name, IntegerType.INSTANCE, true,
                ImmutableList.of("t"), null, column, null, null);
    }

    private static PreAggStatus checkAggregateFunctions(
            Set<AggregateFunction> aggregateFuncs, Set<Slot> groupingExprsInputSlots, Set<Slot> outputSlots) {
        try {
            // checkAggregateFunctions is an instance method of SetOlapScanPreAgg, so pass its INSTANCE
            return (PreAggStatus) CHECK_AGG_FUNCS.invoke(SET_OLAP_SCAN_PRE_AGG_INSTANCE,
                    aggregateFuncs, groupingExprsInputSlots, outputSlots);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static PreAggStatus checkAggWithKeyAndValueSlots(AggregateFunction aggFunc, Set<Slot> outputSlots) {
        try {
            return (PreAggStatus) CHECK_AGG_WITH_KEY_VALUE.invoke(SET_OLAP_SCAN_PRE_AGG_INSTANCE, aggFunc, outputSlots);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static PreAggStatus createPreAggStatus(LogicalOlapScan scan, SetPreAggStatus.PreAggInfoContext context) {
        try {
            return (PreAggStatus) CREATE_PRE_AGG_STATUS.invoke(SET_OLAP_SCAN_PRE_AGG_INSTANCE, scan, context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Expression greaterThanZero(Slot slot) {
        return new GreaterThan(slot, new IntegerLiteral(0));
    }

    private static Expression ifGreaterThanZero(Slot key, Expression thenExpr, Expression elseExpr) {
        return new If(greaterThanZero(key), thenExpr, elseExpr);
    }

    @Test
    void testNoAggregateReturnsOff() {
        SlotReference k = keySlot("k");
        Assertions.assertTrue(checkAggregateFunctions(Collections.emptySet(), Collections.emptySet(),
                Sets.newHashSet(k)).isOff());
    }

    @Test
    void testGroupingOnlyReturnsOn() {
        SlotReference k = keySlot("k");
        // aggregateFuncs empty but groupingExprsInputSlots non-empty -> loop is a no-op, returns ON
        Assertions.assertTrue(checkAggregateFunctions(Collections.emptySet(), Sets.newHashSet(k),
                Sets.newHashSet(k)).isOn());
    }

    @Test
    void testPureKeySlotsDispatch() {
        SlotReference k1 = keySlot("k1");
        SlotReference k2 = keySlot("k2");
        Set<Slot> output = Sets.newHashSet(k1, k2);

        // max/min over key slots are allowed (KeySlotAggChecker)
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Max(k1)), Collections.emptySet(), output)
                .isOn());
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Min(k1)), Collections.emptySet(), output)
                .isOn());

        // distinct aggregates over key slots are allowed (KeySlotAggChecker)
        Assertions.assertTrue(
                checkAggregateFunctions(Sets.newHashSet(new Count(true, k1)), Collections.emptySet(), output).isOn());
        // pure-key multi-argument count(distinct k1, k2)
        Assertions.assertTrue(
                checkAggregateFunctions(Sets.newHashSet(new Count(true, k1, k2)), Collections.emptySet(), output)
                        .isOn());

        // non-distinct, non-max/min aggregates over key slots are rejected
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Sum(k1)), Collections.emptySet(), output)
                .isOff());
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Count(k1)), Collections.emptySet(), output)
                .isOff());
    }

    @Test
    void testMixedMultiArgRejected() {
        SlotReference k = keySlot("k");
        SlotReference v = valueSlot("v", AggregateType.SUM);
        Set<Slot> output = Sets.newHashSet(k, v);

        // multi-argument aggregate with mixed local key/value slots cannot pre-agg
        PreAggStatus status = checkAggregateFunctions(
                Sets.newHashSet(new Count(true, k, v)), Collections.emptySet(), output);
        Assertions.assertTrue(status.isOff());
        Assertions.assertTrue(status.getOffReason().contains("can't turn preAgg on for aggregate function"));
    }

    @Test
    void testPureValueSlotDispatch() {
        SlotReference vSum = valueSlot("v", AggregateType.SUM);
        SlotReference vMax = valueSlot("vmax", AggregateType.MAX);
        Set<Slot> output = Sets.newHashSet(vSum);

        // sum(v) with SUM-type value column (OneValueSlotAggChecker)
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Sum(vSum)), Collections.emptySet(), output)
                .isOn());
        // max(v) with MAX-type value column
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Max(vMax)), Collections.emptySet(),
                Sets.newHashSet(vMax)).isOn());
        // aggregation-type mismatch
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Sum(vMax)), Collections.emptySet(),
                Sets.newHashSet(vMax)).isOff());
        // count over a bare value column is not pre-aggregable (OneValueSlotAggChecker has no visitCount)
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Count(vSum)), Collections.emptySet(),
                output).isOff());
        // non-slot, non-IF/CaseWhen child is rejected
        Assertions.assertTrue(checkAggregateFunctions(
                Sets.newHashSet(new Sum(new Add(vSum, new IntegerLiteral(1)))),
                Collections.emptySet(), output).isOff());
    }

    @Test
    void testIfCaseWhenValuePath() {
        SlotReference k = keySlot("k");
        SlotReference v = valueSlot("v", AggregateType.SUM);
        SlotReference vMax = valueSlot("vmax", AggregateType.MAX);
        SlotReference foreignV = valueSlot("fv", AggregateType.SUM);
        SlotReference foreignVMax = valueSlot("fvmax", AggregateType.MAX);
        Set<Slot> output = Sets.newHashSet(k, v);

        // sum(if(k > 0, v, 0)): row-stable key condition + local SUM return -> ON
        Expression localIf = ifGreaterThanZero(k, v, new IntegerLiteral(0));
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Sum(localIf)), Collections.emptySet(),
                output).isOn());

        // condition references a value column -> OFF (step 2)
        Expression condOnValue = new If(new GreaterThan(v, new IntegerLiteral(0)), v, new IntegerLiteral(0));
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Sum(condOnValue)), Collections.emptySet(),
                output).isOff());

        // foreign value in a SUM return: local slots are pure keys, so this goes through
        // KeySlotAggChecker (sum is not distinct) -> OFF, never reaching the ownership fence
        Expression foreignSumIf = ifGreaterThanZero(k, foreignV, new IntegerLiteral(0));
        PreAggStatus foreignSum = checkAggregateFunctions(
                Sets.newHashSet(new Sum(foreignSumIf)), Collections.emptySet(), output);
        Assertions.assertTrue(foreignSum.isOff());
        Assertions.assertTrue(foreignSum.getOffReason().contains("is not distinct"));

        // MAX exemption: foreign MAX value return is allowed when local slots are mixed
        Expression mixedMaxIf = new If(greaterThanZero(k), foreignVMax, vMax);
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Max(mixedMaxIf)), Collections.emptySet(),
                Sets.newHashSet(k, vMax)).isOn());

        // same mixed shape with SUM is rejected by the ownership fence
        Expression mixedSumIf = new If(greaterThanZero(k), foreignV, v);
        PreAggStatus mixedSum = checkAggregateFunctions(
                Sets.newHashSet(new Sum(mixedSumIf)), Collections.emptySet(), output);
        Assertions.assertTrue(mixedSum.isOff());
        Assertions.assertTrue(mixedSum.getOffReason().contains("references column not owned by this scan"));
    }

    @Test
    void testCountDistinctDispatch() {
        SlotReference k = keySlot("k");
        SlotReference v = valueSlot("v", AggregateType.SUM);
        Set<Slot> output = Sets.newHashSet(k, v);

        // count(distinct if(k > 0, k, 0)): local slots are all keys -> KeySlotAggChecker -> ON
        Expression cdKeyIf = ifGreaterThanZero(k, k, new IntegerLiteral(0));
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Count(true, cdKeyIf)),
                Collections.emptySet(), output).isOn());

        // count(distinct if(k > 0, v, 0)): value column in return -> OFF (visitCount accepts only key/0/NULL)
        Expression cdValueIf = ifGreaterThanZero(k, v, new IntegerLiteral(0));
        Assertions.assertTrue(checkAggregateFunctions(Sets.newHashSet(new Count(true, cdValueIf)),
                Collections.emptySet(), output).isOff());
    }

    @Test
    void testCheckAggWithKeyAndValueSlots() {
        SlotReference k = keySlot("k");
        SlotReference k2 = keySlot("k2");
        SlotReference v = valueSlot("v", AggregateType.SUM);
        SlotReference foreignV = valueSlot("fv", AggregateType.SUM);
        SlotReference foreignVMax = valueSlot("fvmax", AggregateType.MAX);
        Set<Slot> output = Sets.newHashSet(k, v);

        // ownership fence: foreign value in a SUM return
        Assertions.assertTrue(checkAggWithKeyAndValueSlots(
                new Sum(ifGreaterThanZero(k, foreignV, new IntegerLiteral(0))), output).isOff());

        // MAX exemption: foreign MAX value return is safe
        Assertions.assertTrue(checkAggWithKeyAndValueSlots(
                new Max(ifGreaterThanZero(k, foreignVMax, new IntegerLiteral(0))), output).isOn());

        // condition referencing a value column is rejected (step 2)
        Assertions.assertTrue(checkAggWithKeyAndValueSlots(
                new Sum(new If(new GreaterThan(v, new IntegerLiteral(0)), v, new IntegerLiteral(0))), output).isOff());

        // count(distinct) returns must be key/0/NULL: value return -> OFF, key return -> ON
        Assertions.assertTrue(checkAggWithKeyAndValueSlots(
                new Count(true, ifGreaterThanZero(k, v, new IntegerLiteral(0))), output).isOff());
        Assertions.assertTrue(checkAggWithKeyAndValueSlots(
                new Count(true, ifGreaterThanZero(k, k2, new IntegerLiteral(0))),
                Sets.newHashSet(k, k2, v)).isOn());
    }

    @Test
    void testVolatileAggregateTurnsScanOff() throws Exception {
        SlotReference k = keySlot("k");
        PreAggStatus status = createPreAggStatus(mockScan(Sets.newHashSet(k)),
                contextWithAggregateFunctions(Sets.newHashSet(new Sum(new Random()))));
        Assertions.assertTrue(status.isOff());
        Assertions.assertTrue(status.getOffReason().contains("aggregate function")
                && status.getOffReason().contains("contains volatile expression"));
    }

    @Test
    void testVolatileFilterTurnsScanOff() throws Exception {
        SlotReference k = keySlot("k");
        SetPreAggStatus.PreAggInfoContext context = new SetPreAggStatus.PreAggInfoContext();
        Field filterField = SetPreAggStatus.PreAggInfoContext.class.getDeclaredField("filterConjuncts");
        filterField.setAccessible(true);
        filterField.set(context,
                Lists.newArrayList(new GreaterThan(new Random(), new DoubleLiteral(0.5))));

        PreAggStatus status = createPreAggStatus(mockScan(Sets.newHashSet(k)), context);
        Assertions.assertTrue(status.isOff());
        Assertions.assertTrue(status.getOffReason().contains("filter conjunct")
                && status.getOffReason().contains("contains volatile expression"));
    }

    @Test
    void testValidAggregateTurnsScanOn() throws Exception {
        SlotReference k = keySlot("k");
        SlotReference v = valueSlot("v", AggregateType.SUM);
        PreAggStatus status = createPreAggStatus(mockScan(Sets.newHashSet(k, v)),
                contextWithAggregateFunctions(Sets.newHashSet(new Sum(v))));
        Assertions.assertTrue(status.isOn());
    }

    private static LogicalOlapScan mockScan(Set<Slot> outputSlots) {
        LogicalOlapScan scan = Mockito.mock(LogicalOlapScan.class);
        Mockito.when(scan.getOutputSet()).thenReturn(outputSlots);
        return scan;
    }

    private static SetPreAggStatus.PreAggInfoContext contextWithAggregateFunctions(
            Set<AggregateFunction> aggregateFunctions) throws Exception {
        SetPreAggStatus.PreAggInfoContext context = new SetPreAggStatus.PreAggInfoContext();
        Field aggField = SetPreAggStatus.PreAggInfoContext.class.getDeclaredField("aggregateFunctions");
        aggField.setAccessible(true);
        aggField.set(context, aggregateFunctions);
        return context;
    }
}
