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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySort;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySortedMerge;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Upper;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrayMapNullFoldingTest {
    private final ArrayType arrayType = ArrayType.of(StringType.INSTANCE);
    private final Expression nullArray = new NullLiteral(arrayType);

    @Test
    public void testOrdinaryAndHigherOrderNullPropagation() {
        Assertions.assertTrue(fold(new Upper(new NullLiteral(StringType.INSTANCE))).isNullLiteral());
        Assertions.assertTrue(fold(new MapKeys(new NullLiteral(
                MapType.of(StringType.INSTANCE, StringType.INSTANCE)))).isNullLiteral());
        ArrayItemReference item = new ArrayItemReference("x", nullArray);
        Lambda predicate = new Lambda(ImmutableList.of("x"),
                new IsNull(item.toSlot()), ImmutableList.of(item));
        Assertions.assertTrue(fold(new ArrayFilter(predicate)).isNullLiteral());
    }

    @Test
    public void testNullArrayWithReferencedAndConstantBodies() {
        ArrayItemReference item = new ArrayItemReference("x", nullArray);
        for (Expression body : ImmutableList.of(item.toSlot(), new StringLiteral("constant"))) {
            Expression result = fold(new ArrayMap(new Lambda(
                    ImmutableList.of("x"), body, ImmutableList.of(item))));
            Assertions.assertEquals(new NullLiteral(arrayType), result);
        }
    }

    @Test
    public void testAnyNullArrayIncludingUnusedArgument() {
        Expression array = new SlotReference("arr", arrayType);
        for (boolean nullFirst : ImmutableList.of(false, true)) {
            ArrayItemReference x = new ArrayItemReference("x", nullFirst ? nullArray : array);
            ArrayItemReference y = new ArrayItemReference("y", nullFirst ? array : nullArray);
            Expression result = fold(new ArrayMap(new Lambda(
                    ImmutableList.of("x", "y"), x.toSlot(), ImmutableList.of(x, y))));
            Assertions.assertEquals(new NullLiteral(arrayType), result);
        }
    }

    @Test
    public void testConditionalArrayInputAndDefaultValue() {
        Expression fallback = new Array(new StringLiteral("n"));
        for (Expression array : ImmutableList.of(
                new If(new IsNull(nullArray), fallback, nullArray),
                new Coalesce(nullArray, fallback))) {
            ArrayItemReference item = new ArrayItemReference("x", array);
            Expression result = fold(new ArrayMap(new Lambda(
                    ImmutableList.of("x"), item.toSlot(), ImmutableList.of(item))));
            Assertions.assertInstanceOf(ArrayMap.class, result);
            Assertions.assertFalse(result.isNullLiteral());
        }
        Expression conditionalNull = new If(new IsNull(nullArray), nullArray, fallback);
        ArrayItemReference item = new ArrayItemReference("x", conditionalNull);
        Assertions.assertTrue(fold(new ArrayMap(new Lambda(
                ImmutableList.of("x"), item.toSlot(), ImmutableList.of(item)))).isNullLiteral());
    }

    @Test
    public void testNestedArrayMap() {
        ArrayItemReference innerItem = new ArrayItemReference("y", nullArray);
        Expression inner = new ArrayMap(new Lambda(
                ImmutableList.of("y"), innerItem.toSlot(), ImmutableList.of(innerItem)));
        ArrayItemReference outerItem = new ArrayItemReference("x", inner);
        Expression outer = new ArrayMap(new Lambda(
                ImmutableList.of("x"), outerItem.toSlot(), ImmutableList.of(outerItem)));
        Assertions.assertEquals(new NullLiteral(arrayType), fold(outer));
    }

    @Test
    public void testComparatorSortNullInputs() {
        ArrayItemReference x = new ArrayItemReference("x", nullArray);
        ArrayItemReference y = new ArrayItemReference("y", nullArray);
        Lambda comparator = new Lambda(ImmutableList.of("x", "y"),
                new IntegerLiteral(0), ImmutableList.of(x, y));
        Assertions.assertEquals(new NullLiteral(arrayType), fold(new ArraySort(comparator)));
        Assertions.assertEquals(new NullLiteral(arrayType), fold(new ArraySort(nullArray)));
    }

    @Test
    public void testSortedMergeNullInputsAndInvalidFlags() {
        Expression array = new SlotReference("arr", arrayType);
        for (boolean nullFirst : ImmutableList.of(false, true)) {
            ArrayItemReference x = new ArrayItemReference("x", nullFirst ? nullArray : array);
            ArrayItemReference y = new ArrayItemReference("y", nullFirst ? array : nullArray);
            Lambda merge = new Lambda(ImmutableList.of("x", "y"), x.toSlot(), ImmutableList.of(x, y));
            for (BooleanLiteral flag : ImmutableList.of(BooleanLiteral.TRUE, BooleanLiteral.FALSE)) {
                Assertions.assertEquals(new NullLiteral(arrayType), fold(new ArraySortedMerge(merge, flag)));
            }
            Assertions.assertThrows(AnalysisException.class, () -> fold(
                    new ArraySortedMerge(merge, NullLiteral.BOOLEAN_INSTANCE)));
        }
    }

    private Expression fold(Expression expression) {
        return FoldConstantRuleOnFE.evaluateWithoutContext(expression);
    }
}
