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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Direct expression unit tests for {@link MultiDistinctCollectList} (no FE service needed),
 * plus coverage of {@link CollectList#convertToMultiDistinct()}.
 */
public class MultiDistinctCollectListTest {

    private final SlotReference intSlot = new SlotReference("col_int", IntegerType.INSTANCE);
    private final SlotReference anotherSlot = new SlotReference("col_int2", IntegerType.INSTANCE);

    @Test
    public void testOneArgConstructor() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        Assertions.assertEquals("multi_distinct_collect_list", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertEquals(intSlot, func.getArgument(0));
        Assertions.assertFalse(func.isDistinct());
        Assertions.assertTrue(func.getDataType() instanceof ArrayType);
    }

    @Test
    public void testTwoArgConstructor() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot, new IntegerLiteral(10));
        Assertions.assertEquals("multi_distinct_collect_list", func.getName());
        Assertions.assertEquals(2, func.arity());
        Assertions.assertEquals(intSlot, func.getArgument(0));
        Assertions.assertTrue(func.getDataType() instanceof ArrayType);
    }

    @Test
    public void testGetSignatures() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        Assertions.assertEquals(2, func.getSignatures().size());
        Assertions.assertSame(MultiDistinctCollectList.SIGNATURES, func.getSignatures());
    }

    @Test
    public void testWithDistinctAndChildrenOneArg() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        MultiDistinctCollectList replaced =
                func.withDistinctAndChildren(false, ImmutableList.of(anotherSlot));
        Assertions.assertEquals(1, replaced.arity());
        Assertions.assertEquals(anotherSlot, replaced.getArgument(0));
        // distinct is always forced to false by withDistinctAndChildren
        Assertions.assertFalse(replaced.isDistinct());
    }

    @Test
    public void testWithDistinctAndChildrenTwoArgs() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot, new IntegerLiteral(5));
        MultiDistinctCollectList replaced = func.withDistinctAndChildren(
                false, ImmutableList.of(anotherSlot, new IntegerLiteral(7)));
        Assertions.assertEquals(2, replaced.arity());
        Assertions.assertEquals(anotherSlot, replaced.getArgument(0));
    }

    @Test
    public void testWithChildren() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        Expression replaced = func.withChildren(ImmutableList.of(anotherSlot));
        Assertions.assertTrue(replaced instanceof MultiDistinctCollectList);
        Assertions.assertEquals(anotherSlot, ((MultiDistinctCollectList) replaced).getArgument(0));
    }

    @Test
    public void testWithDistinctAndChildrenIllegalArity() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> func.withDistinctAndChildren(false, ImmutableList.of()));
    }

    @Test
    public void testResultForEmptyInput() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        Expression empty = func.resultForEmptyInput();
        Assertions.assertTrue(empty instanceof ArrayLiteral);
        Assertions.assertTrue(((ArrayLiteral) empty).getValue().isEmpty());
    }

    @Test
    public void testAcceptVisitorDispatch() {
        MultiDistinctCollectList func = new MultiDistinctCollectList(intSlot);
        DefaultExpressionVisitor<String, Void> visitor = new DefaultExpressionVisitor<String, Void>() {
            @Override
            public String visitMultiDistinctCollectList(MultiDistinctCollectList f, Void ctx) {
                return "visited_multi_distinct_collect_list";
            }
        };
        Assertions.assertEquals("visited_multi_distinct_collect_list", func.accept(visitor, null));
    }

    @Test
    public void testCollectListConvertToMultiDistinctOneArg() {
        AggregateFunction converted = new CollectList(true, intSlot).convertToMultiDistinct();
        Assertions.assertTrue(converted instanceof MultiDistinctCollectList);
        Assertions.assertEquals(1, converted.arity());
        Assertions.assertEquals(intSlot, converted.getArgument(0));
    }

    @Test
    public void testCollectListConvertToMultiDistinctTwoArgs() {
        AggregateFunction converted =
                new CollectList(true, intSlot, new IntegerLiteral(10)).convertToMultiDistinct();
        Assertions.assertTrue(converted instanceof MultiDistinctCollectList);
        Assertions.assertEquals(2, converted.arity());
        Assertions.assertEquals(intSlot, converted.getArgument(0));
    }

    @Test
    public void testCollectListConvertToMultiDistinctNonDistinctThrows() {
        CollectList nonDistinct = new CollectList(false, intSlot);
        Assertions.assertThrows(IllegalArgumentException.class, nonDistinct::convertToMultiDistinct);
    }
}
