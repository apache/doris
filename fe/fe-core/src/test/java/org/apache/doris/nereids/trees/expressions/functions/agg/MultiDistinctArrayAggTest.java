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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Direct expression unit tests for {@link MultiDistinctArrayAgg} (no FE service needed),
 * plus coverage of {@link ArrayAgg#convertToMultiDistinct()}.
 */
public class MultiDistinctArrayAggTest {

    private final SlotReference intSlot = new SlotReference("col_int", IntegerType.INSTANCE);
    private final SlotReference anotherSlot = new SlotReference("col_int2", IntegerType.INSTANCE);

    @Test
    public void testOneArgConstructor() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        Assertions.assertEquals("multi_distinct_array_agg", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertEquals(intSlot, func.getArgument(0));
        Assertions.assertFalse(func.isDistinct());
        Assertions.assertTrue(func.getDataType() instanceof ArrayType);
    }

    @Test
    public void testGetSignatures() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        Assertions.assertEquals(1, func.getSignatures().size());
        Assertions.assertSame(MultiDistinctArrayAgg.SIGNATURES, func.getSignatures());
    }

    @Test
    public void testWithDistinctAndChildren() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        MultiDistinctArrayAgg replaced =
                func.withDistinctAndChildren(false, ImmutableList.of(anotherSlot));
        Assertions.assertEquals(1, replaced.arity());
        Assertions.assertEquals(anotherSlot, replaced.getArgument(0));
        Assertions.assertFalse(replaced.isDistinct());
    }

    @Test
    public void testWithChildren() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        Expression replaced = func.withChildren(ImmutableList.of(anotherSlot));
        Assertions.assertTrue(replaced instanceof MultiDistinctArrayAgg);
        Assertions.assertEquals(anotherSlot, ((MultiDistinctArrayAgg) replaced).getArgument(0));
    }

    @Test
    public void testWithDistinctAndChildrenIllegalArity() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> func.withDistinctAndChildren(false, ImmutableList.of(intSlot, anotherSlot)));
    }

    @Test
    public void testResultForEmptyInput() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        Expression empty = func.resultForEmptyInput();
        Assertions.assertTrue(empty instanceof ArrayLiteral);
        Assertions.assertTrue(((ArrayLiteral) empty).getValue().isEmpty());
    }

    @Test
    public void testAcceptVisitorDispatch() {
        MultiDistinctArrayAgg func = new MultiDistinctArrayAgg(intSlot);
        DefaultExpressionVisitor<String, Void> visitor = new DefaultExpressionVisitor<String, Void>() {
            @Override
            public String visitMultiDistinctArrayAgg(MultiDistinctArrayAgg f, Void ctx) {
                return "visited_multi_distinct_array_agg";
            }
        };
        Assertions.assertEquals("visited_multi_distinct_array_agg", func.accept(visitor, null));
    }

    @Test
    public void testArrayAggConvertToMultiDistinct() {
        AggregateFunction converted = new ArrayAgg(true, intSlot).convertToMultiDistinct();
        Assertions.assertTrue(converted instanceof MultiDistinctArrayAgg);
        Assertions.assertEquals(1, converted.arity());
        Assertions.assertEquals(intSlot, converted.getArgument(0));
    }

    @Test
    public void testArrayAggConvertToMultiDistinctNonDistinctThrows() {
        ArrayAgg nonDistinct = new ArrayAgg(false, intSlot);
        Assertions.assertThrows(IllegalArgumentException.class, nonDistinct::convertToMultiDistinct);
    }
}
