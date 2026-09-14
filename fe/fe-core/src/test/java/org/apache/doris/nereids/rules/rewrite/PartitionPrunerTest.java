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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.OneListPartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.OnePartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.OneRangePartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionPruneResult;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PartitionPrunerTest extends TestWithFeService {
    private Method canBePrunedOutMethod;
    private final Column partitionColumn = new Column("a", PrimitiveType.INT);
    private CascadesContext cascadesContext;
    private final SlotReference slotA = new SlotReference("a", IntegerType.INSTANCE);
    private final SlotReference slotB = new SlotReference("b", IntegerType.INSTANCE);
    private final SlotReference slotC = new SlotReference("c", IntegerType.INSTANCE);
    private final SlotReference slotD = new SlotReference("d", IntegerType.INSTANCE);

    @Override
    protected void runBeforeAll() throws Exception {
        Class<?> clazz = PartitionPruner.class;
        canBePrunedOutMethod = clazz.getDeclaredMethod("canBePrunedOut", Expression.class, OnePartitionEvaluator.class);
        canBePrunedOutMethod.setAccessible(true);
        cascadesContext = createCascadesContext("select * from t1");
    }

    // test canBePrunedOut res
    // list partition p1, partition value is 1
    // predicate: a = 1
    @Test
    public void testEqualPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(1));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a = 1
    @Test
    public void testEqualPredicate2()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1),
                ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2),
                ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(1));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: a = 2
    @Test
    public void testEqualPredicate3()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(2));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: a = NULL
    @Test
    public void testNullPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new EqualTo(slotA, NullLiteral.INSTANCE);
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: a IN (1, 2)
    @Test
    public void testInPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new InPredicate(slotA, ImmutableList.of(Literal.of(1), Literal.of(2)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a IN (1, 2)
    @Test
    public void testInPredicateExactMatch()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new InPredicate(slotA, ImmutableList.of(Literal.of(1), Literal.of(2)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second);
    }


    // list partition p1, partition value (1, "a"), (2, "b")
    // predicate: a = 1 AND b = "a"
    @Test
    public void testMultiColumnPartition()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        Column partitionColumn2 = new Column("b", PrimitiveType.VARCHAR);
        SlotReference slot2 = new SlotReference("b", VarcharType.createVarcharType(10));

        PartitionValue partitionValue1a = new PartitionValue("1");
        PartitionValue partitionValue1b = new PartitionValue("a");
        PartitionValue partitionValue2a = new PartitionValue("2");
        PartitionValue partitionValue2b = new PartitionValue("b");

        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue1a, partitionValue1b),
                ImmutableList.of(partitionColumn, partitionColumn2));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue2a, partitionValue2b),
                ImmutableList.of(partitionColumn, partitionColumn2));

        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);

        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA, slot2), partitionItem, cascadesContext);

        Expression predicate = new And(
                new EqualTo(slotA, Literal.of(1)),
                new EqualTo(slot2, Literal.of("a"))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a = 1 OR a = 3
    @Test
    public void testOrPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new Or(
                new EqualTo(slotA, Literal.of(1)),
                new EqualTo(slotA, Literal.of(3))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: NOT (a = 1)
    @Test
    public void testNotPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new Not(new EqualTo(slotA, Literal.of(1)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: a > 2
    @Test
    public void testGreaterThanPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new GreaterThan(slotA, Literal.of(2));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: (a = 1 OR a = 2) AND a > 0
    @Test
    public void testComplexNestedPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new And(
                new Or(
                        new EqualTo(slotA, Literal.of(1)),
                        new EqualTo(slotA, Literal.of(2))
                ),
                new GreaterThan(slotA, Literal.of(0))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    @Test
    public void testPruneWithResultIgnoresNonPruningPartitionPredicate() throws AnalysisException {
        Map<String, PartitionItem> idToPartitions = ImmutableMap.of(
                "p1", createListPartitionItem("1"),
                "p2", createListPartitionItem("2"));

        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                ImmutableList.of(slotA), new Not(new IsNull(slotA)), idToPartitions, cascadesContext,
                PartitionTableType.OLAP, Optional.empty());

        Assertions.assertEquals(2, result.partitions.size());
        Assertions.assertFalse(result.hasPartitionPredicate);
    }

    @Test
    public void testPruneWithResultMarksEffectivePartitionPredicate() throws AnalysisException {
        Map<String, PartitionItem> idToPartitions = ImmutableMap.of(
                "p1", createListPartitionItem("1"),
                "p2", createListPartitionItem("2"));

        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                ImmutableList.of(slotA), new EqualTo(slotA, Literal.of(1)), idToPartitions, cascadesContext,
                PartitionTableType.OLAP, Optional.empty());

        Assertions.assertEquals(1, result.partitions.size());
        Assertions.assertTrue(result.hasPartitionPredicate);
    }

    @Test
    public void testThreeColumnLexicographicRangeBoundaries()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        List<Column> columns = ImmutableList.of(
                new Column("a", PrimitiveType.INT),
                new Column("b", PrimitiveType.INT),
                new Column("c", PrimitiveType.INT));
        List<Slot> slots = ImmutableList.of(slotA, slotB, slotC);
        RangePartitionItem partitionItem = createRangePartitionItem(
                columns, new int[] {1, 10, 100}, new int[] {100, 20, 200});

        for (int expandThreshold : new int[] {1, 200}) {
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 1, 11, 50);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 100, 19, 250);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 100, 20, 199);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 1, 10, 100);

            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 1, 10, 99);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 100, 20, 200);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 100, 20, 250);
        }
    }

    @Test
    public void testFourColumnLexicographicRangeBoundaries()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        List<Column> columns = ImmutableList.of(
                new Column("a", PrimitiveType.INT),
                new Column("b", PrimitiveType.INT),
                new Column("c", PrimitiveType.INT),
                new Column("d", PrimitiveType.INT));
        List<Slot> slots = ImmutableList.of(slotA, slotB, slotC, slotD);
        RangePartitionItem partitionItem = createRangePartitionItem(
                columns, new int[] {1, 10, 100, 1000}, new int[] {4, 20, 200, 2000});

        for (int expandThreshold : new int[] {1, 10}) {
            // Once an earlier column diverges, all suffix columns are unbounded.
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 2, -1000, -1000, -1000);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 3, 9999, 9999, 9999);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 1, 11, -1, -1);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 4, 19, 9999, 9999);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 1, 10, 101, -1);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 4, 20, 199, 9999);

            // An equal prefix leaves the final column to decide the inclusive lower and exclusive upper bounds.
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 1, 10, 100, 1000);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, false, 4, 20, 200, 1999);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 1, 10, 100, 999);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 4, 20, 200, 2000);
            assertRangeTuplePruned(partitionItem, slots, expandThreshold, true, 4, 20, 200, 2001);
        }
    }

    private void assertRangeTuplePruned(RangePartitionItem partitionItem, List<Slot> slots,
            int expandThreshold, boolean expectedPruned, int... values)
            throws InvocationTargetException, IllegalAccessException {
        ImmutableList.Builder<Expression> equalities = ImmutableList.builderWithExpectedSize(values.length);
        for (int i = 0; i < values.length; i++) {
            equalities.add(new EqualTo(slots.get(i), Literal.of(values[i])));
        }
        Expression predicate = ExpressionUtils.and(equalities.build());
        OneRangePartitionEvaluator<String> evaluator = new OneRangePartitionEvaluator<>(
                "p1", slots, partitionItem, cascadesContext, expandThreshold);
        Pair<Boolean, Boolean> result =
                (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, evaluator);
        Assertions.assertEquals(expectedPruned, result.first,
                "tuple=" + Arrays.toString(values) + ", expandThreshold=" + expandThreshold);
    }

    private RangePartitionItem createRangePartitionItem(
            List<Column> columns, int[] lowerValues, int[] upperValues) throws AnalysisException {
        ImmutableList.Builder<PartitionValue> lower = ImmutableList.builderWithExpectedSize(lowerValues.length);
        ImmutableList.Builder<PartitionValue> upper = ImmutableList.builderWithExpectedSize(upperValues.length);
        for (int value : lowerValues) {
            lower.add(new PartitionValue(Integer.toString(value)));
        }
        for (int value : upperValues) {
            upper.add(new PartitionValue(Integer.toString(value)));
        }
        PartitionKey lowerKey = PartitionKey.createPartitionKey(lower.build(), columns);
        PartitionKey upperKey = PartitionKey.createPartitionKey(upper.build(), columns);
        return new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
    }

    private ListPartitionItem createListPartitionItem(String... values) throws AnalysisException {
        ImmutableList.Builder<PartitionKey> partitionKeys = ImmutableList.builder();
        for (String value : values) {
            PartitionValue partitionValue = new PartitionValue(value);
            partitionKeys.add(PartitionKey.createPartitionKey(
                    ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn)));
        }
        return new ListPartitionItem(partitionKeys.build());
    }
}
