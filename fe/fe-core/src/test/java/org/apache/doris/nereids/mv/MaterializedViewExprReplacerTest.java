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

package org.apache.doris.nereids.mv;

import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewExprReplacer;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class MaterializedViewExprReplacerTest {

    @Test
    void testSimpleSlotReplace() {
        SlotReference oldSlot = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference newSlot = new SlotReference("b", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = ImmutableMap.of(oldSlot, newSlot);
        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visit(oldSlot, null);
        Assertions.assertEquals(newSlot, result);
        Assertions.assertTrue(replacer.isValid());
    }

    @Test
    void testSlotNotFoundInReplaceMap() {
        SlotReference slot = new SlotReference("a", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visit(slot, null);
        Assertions.assertEquals(slot, result);
        Assertions.assertFalse(replacer.isValid());
    }

    @Test
    void testComplexExpressionReplace() {
        SlotReference slot1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference newSlot1 = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference newSlot2 = new SlotReference("d", IntegerType.INSTANCE);

        Add addExpr = new Add(slot1, slot2);

        Map<Expression, Expression> replaceMap = ImmutableMap.of(
                slot1, newSlot1,
                slot2, newSlot2
        );

        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visit(addExpr, null);
        Assertions.assertTrue(result instanceof Add);
        Add resultAdd = (Add) result;
        Assertions.assertEquals(newSlot1, resultAdd.child(0));
        Assertions.assertEquals(newSlot2, resultAdd.child(1));
        Assertions.assertTrue(replacer.isValid());
    }

    @Test
    void testWholeExpressionReplace() {
        SlotReference slot1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("b", IntegerType.INSTANCE);
        Add addExpr = new Add(slot1, slot2);
        IntegerLiteral replacement = new IntegerLiteral(100);

        Map<Expression, Expression> replaceMap = ImmutableMap.of(addExpr, replacement);

        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visit(addExpr, null);
        Assertions.assertEquals(replacement, result);
        Assertions.assertTrue(replacer.isValid());
    }

    @Test
    void testVirtualSlotWithoutOriginExpression() {
        VirtualSlotReference virtualSlot = new VirtualSlotReference(
                "virtual", IntegerType.INSTANCE, Optional.empty(), null);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visitSlot(virtualSlot, null);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(replacer.isValid());
    }

    @Test
    void testInvalidStatePreservation() {
        SlotReference slot1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("b", IntegerType.INSTANCE);

        Map<Expression, Expression> replaceMap = new HashMap<>();
        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        replacer.visit(slot1, null);
        Assertions.assertFalse(replacer.isValid());

        Expression result = replacer.visit(slot2, null);
        Assertions.assertEquals(slot2, result);
        Assertions.assertFalse(replacer.isValid());
    }

    @Test
    void testNestedExpression() {
        SlotReference slot1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference newSlot1 = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference newSlot2 = new SlotReference("d", IntegerType.INSTANCE);

        Add innerAdd = new Add(slot1, slot2);
        Add outerAdd = new Add(innerAdd, new IntegerLiteral(10));

        Map<Expression, Expression> replaceMap = ImmutableMap.of(
                slot1, newSlot1,
                slot2, newSlot2
        );

        MaterializedViewExprReplacer replacer = new MaterializedViewExprReplacer(
                replaceMap, null, new BitSet());

        Expression result = replacer.visit(outerAdd, null);
        Assertions.assertTrue(result instanceof Add);
        Add resultOuter = (Add) result;
        Assertions.assertTrue(resultOuter.child(0) instanceof Add);
        Add resultInner = (Add) resultOuter.child(0);
        Assertions.assertEquals(newSlot1, resultInner.child(0));
        Assertions.assertEquals(newSlot2, resultInner.child(1));
        Assertions.assertTrue(replacer.isValid());
    }
}
