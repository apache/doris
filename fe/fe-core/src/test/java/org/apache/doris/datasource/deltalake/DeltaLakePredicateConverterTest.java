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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;

import io.delta.kernel.expressions.Predicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for DeltaLakePredicateConverter.
 */
public class DeltaLakePredicateConverterTest {

    private SlotRef createSlotRef(String colName) {
        return new SlotRef(null, colName);
    }

    @Test
    public void testEmptyConjuncts() {
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(null);
        Assert.assertFalse(result.isPresent());

        result = DeltaLakePredicateConverter.convertToKernelPredicate(Collections.emptyList());
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testBoolLiteralTrue() {
        BoolLiteral trueLiteral = new BoolLiteral(true);
        List<Expr> conjuncts = Collections.singletonList(trueLiteral);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("ALWAYS_TRUE", result.get().getName());
    }

    @Test
    public void testBoolLiteralFalse() {
        BoolLiteral falseLiteral = new BoolLiteral(false);
        List<Expr> conjuncts = Collections.singletonList(falseLiteral);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("ALWAYS_FALSE", result.get().getName());
    }

    @Test
    public void testBinaryPredicateEqual() {
        SlotRef slotRef = createSlotRef("id");
        IntLiteral literal = new IntLiteral(42);
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, literal);

        List<Expr> conjuncts = Collections.singletonList(eq);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("=", result.get().getName());
    }

    @Test
    public void testBinaryPredicateNotEqual() {
        SlotRef slotRef = createSlotRef("name");
        StringLiteral literal = new StringLiteral("test");
        BinaryPredicate ne = new BinaryPredicate(BinaryPredicate.Operator.NE, slotRef, literal);

        List<Expr> conjuncts = Collections.singletonList(ne);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        Assert.assertTrue(result.isPresent());
        // NE is converted as NOT(=)
        Assert.assertEquals("NOT", result.get().getName());
    }

    @Test
    public void testBinaryPredicateComparisons() {
        SlotRef slotRef = createSlotRef("value");
        IntLiteral literal = new IntLiteral(100);

        // LT
        BinaryPredicate lt = new BinaryPredicate(BinaryPredicate.Operator.LT, slotRef, literal);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(lt));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("<", result.get().getName());

        // LE
        BinaryPredicate le = new BinaryPredicate(BinaryPredicate.Operator.LE, slotRef, literal);
        result = DeltaLakePredicateConverter.convertToKernelPredicate(Collections.singletonList(le));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("<=", result.get().getName());

        // GT
        BinaryPredicate gt = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, literal);
        result = DeltaLakePredicateConverter.convertToKernelPredicate(Collections.singletonList(gt));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(">", result.get().getName());

        // GE
        BinaryPredicate ge = new BinaryPredicate(BinaryPredicate.Operator.GE, slotRef, literal);
        result = DeltaLakePredicateConverter.convertToKernelPredicate(Collections.singletonList(ge));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(">=", result.get().getName());
    }

    @Test
    public void testIsNullPredicate() {
        SlotRef slotRef = createSlotRef("col");

        // IS NULL
        IsNullPredicate isNull = new IsNullPredicate(slotRef, false);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(isNull));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("IS_NULL", result.get().getName());

        // IS NOT NULL
        IsNullPredicate isNotNull = new IsNullPredicate(slotRef, true);
        result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(isNotNull));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("IS_NOT_NULL", result.get().getName());
    }

    @Test
    public void testCompoundPredicateAnd() {
        SlotRef slotRef1 = createSlotRef("a");
        IntLiteral literal1 = new IntLiteral(1);
        BinaryPredicate eq1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef1, literal1);

        SlotRef slotRef2 = createSlotRef("b");
        IntLiteral literal2 = new IntLiteral(2);
        BinaryPredicate eq2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef2, literal2);

        CompoundPredicate and = new CompoundPredicate(CompoundPredicate.Operator.AND, eq1, eq2);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(and));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("AND", result.get().getName());
    }

    @Test
    public void testCompoundPredicateOr() {
        SlotRef slotRef1 = createSlotRef("x");
        IntLiteral literal1 = new IntLiteral(10);
        BinaryPredicate gt = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef1, literal1);

        SlotRef slotRef2 = createSlotRef("y");
        IntLiteral literal2 = new IntLiteral(20);
        BinaryPredicate lt = new BinaryPredicate(BinaryPredicate.Operator.LT, slotRef2, literal2);

        CompoundPredicate or = new CompoundPredicate(CompoundPredicate.Operator.OR, gt, lt);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(or));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("OR", result.get().getName());
    }

    @Test
    public void testCompoundPredicateNot() {
        SlotRef slotRef = createSlotRef("flag");
        IntLiteral literal = new IntLiteral(0);
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, literal);

        CompoundPredicate not = new CompoundPredicate(CompoundPredicate.Operator.NOT, eq, null);
        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(not));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("NOT", result.get().getName());
    }

    @Test
    public void testMultipleConjunctsAnded() {
        SlotRef slotRef1 = createSlotRef("a");
        IntLiteral literal1 = new IntLiteral(1);
        BinaryPredicate eq1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef1, literal1);

        SlotRef slotRef2 = createSlotRef("b");
        StringLiteral literal2 = new StringLiteral("hello");
        BinaryPredicate eq2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef2, literal2);

        List<Expr> conjuncts = new ArrayList<>();
        conjuncts.add(eq1);
        conjuncts.add(eq2);

        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        Assert.assertTrue(result.isPresent());
        // Multiple conjuncts should be AND-chained
        Assert.assertEquals("AND", result.get().getName());
    }

    @Test
    public void testFloatLiteral() {
        SlotRef slotRef = createSlotRef("price");
        FloatLiteral literal = new FloatLiteral(3.14, Type.DOUBLE);
        BinaryPredicate gt = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, literal);

        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(
                Collections.singletonList(gt));
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(">", result.get().getName());
    }

    @Test
    public void testPartialConversion() {
        // First conjunct is convertible, second is not (e.g., a function call)
        SlotRef slotRef = createSlotRef("id");
        IntLiteral literal = new IntLiteral(5);
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, literal);

        // Use a non-convertible expression (two SlotRefs, no literal)
        SlotRef slotRef2 = createSlotRef("col1");
        SlotRef slotRef3 = createSlotRef("col2");
        BinaryPredicate noLiteral = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef2, slotRef3);

        List<Expr> conjuncts = new ArrayList<>();
        conjuncts.add(eq);
        conjuncts.add(noLiteral);

        Optional<Predicate> result = DeltaLakePredicateConverter.convertToKernelPredicate(conjuncts);
        // Should still return the convertible predicate
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals("=", result.get().getName());
    }
}
