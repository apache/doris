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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for IcebergNereidsUtils
 */
public class IcebergNereidsUtilsTest {

    @Mock
    private Schema mockSchema;

    @Mock
    private Types.NestedField mockNestedField;

    private Schema testSchema;

    @BeforeEach
    public void setUp() {
        // Create a real schema for testing
        testSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "salary", Types.DoubleType.get()),
                Types.NestedField.required(5, "is_active", Types.BooleanType.get()));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_NullInput() {
            UserException exception = Assertions.assertThrows(UserException.class, () -> {
                    IcebergNereidsUtils.convertNereidsToIcebergExpression(null, testSchema);
            });
            Assertions.assertEquals("Nereids expression is null", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_EqualTo() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("id", 100).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_GreaterThan() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(18);
        GreaterThan greaterThan = new GreaterThan(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(greaterThan, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.greaterThan("age", 18).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_GreaterThanEqual() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(18);
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(greaterThanEqual, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.greaterThanOrEqual("age", 18).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_LessThan() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(65);
        LessThan lessThan = new LessThan(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(lessThan, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.lessThan("age", 65).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_LessThanEqual() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(65);
        LessThanEqual lessThanEqual = new LessThanEqual(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(lessThanEqual, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.lessThanOrEqual("age", 65).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_And() throws UserException {
        SlotReference slotRef1 = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference slotRef2 = new SlotReference("salary", DoubleType.INSTANCE, false);
        IntegerLiteral literal1 = new IntegerLiteral(18);
        DoubleLiteral literal2 = new DoubleLiteral(50000.0);

        GreaterThan greaterThan = new GreaterThan(slotRef1, literal1);
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef2, literal2);
        And andExpr = new And(greaterThan, greaterThanEqual);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(andExpr, testSchema);

        Assertions.assertNotNull(result);
        // The result should be an AND expression combining both conditions
        Assertions.assertTrue(result.toString().contains("age > 18"));
        Assertions.assertTrue(result.toString().contains("salary >= 50000.0"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_Or() throws UserException {
        SlotReference slotRef1 = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference slotRef2 = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal1 = new IntegerLiteral(18);
        IntegerLiteral literal2 = new IntegerLiteral(65);

        LessThan lessThan = new LessThan(slotRef1, literal1);
        GreaterThan greaterThan = new GreaterThan(slotRef2, literal2);
        Or orExpr = new Or(lessThan, greaterThan);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils.convertNereidsToIcebergExpression(orExpr,
                testSchema);

        Assertions.assertNotNull(result);
        // The result should be an OR expression combining both conditions
        Assertions.assertTrue(result.toString().contains("age < 18"));
        Assertions.assertTrue(result.toString().contains("age > 65"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_Not() throws UserException {
        SlotReference slotRef = new SlotReference("is_active", BooleanType.INSTANCE, false);
        BooleanLiteral literal = BooleanLiteral.of(true);
        EqualTo equalTo = new EqualTo(slotRef, literal);
        Not notExpr = new Not(equalTo);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(notExpr, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("NOT"));
        Assertions.assertTrue(result.toString().contains("is_active = true"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_InPredicate() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal1 = new IntegerLiteral(1);
        IntegerLiteral literal2 = new IntegerLiteral(2);
        IntegerLiteral literal3 = new IntegerLiteral(3);

        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(literal1, literal2, literal3));

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(inPredicate, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("id IN"));
        Assertions.assertTrue(result.toString().contains("1"));
        Assertions.assertTrue(result.toString().contains("2"));
        Assertions.assertTrue(result.toString().contains("3"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ComplexNested() throws UserException {
        // Test complex nested expression: (age > 18 AND salary >= 50000) OR (age < 65
        // AND salary < 100000)
        SlotReference ageRef = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference salaryRef = new SlotReference("salary", DoubleType.INSTANCE, false);

        GreaterThan ageGt = new GreaterThan(ageRef, new IntegerLiteral(18));
        GreaterThanEqual salaryGte = new GreaterThanEqual(salaryRef, new DoubleLiteral(50000.0));
        And leftAnd = new And(ageGt, salaryGte);

        LessThan ageLt = new LessThan(ageRef, new IntegerLiteral(65));
        LessThan salaryLt = new LessThan(salaryRef, new DoubleLiteral(100000.0));
        And rightAnd = new And(ageLt, salaryLt);

        Or orExpr = new Or(leftAnd, rightAnd);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils.convertNereidsToIcebergExpression(orExpr,
                testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("OR"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_WithNullLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        NullLiteral nullLiteral = new NullLiteral();
        EqualTo equalTo = new EqualTo(slotRef, nullLiteral);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("isNull"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ColumnNotFound() {
        SlotReference slotRef = new SlotReference("non_existent_column", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Column not found in Iceberg schema: non_existent_column", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_CaseInsensitiveColumnName() throws UserException {
        // Test case insensitive column name matching
        SlotReference slotRef = new SlotReference("ID", IntegerType.INSTANCE, false); // uppercase
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("id", 100).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_UnsupportedExpression() {
        // Test with an unsupported expression type
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);

        // Create a mock expression that's not supported
        org.apache.doris.nereids.trees.expressions.Expression unsupportedExpr = Mockito.mock(
                org.apache.doris.nereids.trees.expressions.Expression.class);
        Mockito.when(unsupportedExpr.children()).thenReturn(Arrays.asList(slotRef, literal));

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(unsupportedExpr, testSchema);
        });
        Assertions.assertTrue(exception.getMessage().contains("Unsupported expression type"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ExceptionHandling() {
        // Test exception handling in the conversion process
        SlotReference slotRef = Mockito.mock(SlotReference.class);
        Mockito.when(slotRef.getName()).thenThrow(new RuntimeException("Test exception"));
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Test exception", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_AndWithNullChild() {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        // Create an AND with one null child
        And andExpr = new And(equalTo, null);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(andExpr, testSchema);
        });
        Assertions.assertEquals("Failed to convert AND expression: one or both children are unsupported",
                        exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_OrWithNullChild() {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        // Create an OR with one null child
        Or orExpr = new Or(equalTo, null);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(orExpr, testSchema);
        });
        Assertions.assertEquals("Failed to convert OR expression: one or both children are unsupported",
                        exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_NotWithNullChild() {
        // Create a NOT with null child
        Not notExpr = new Not(null);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(notExpr, testSchema);
        });
        Assertions.assertEquals("Failed to convert NOT expression: child is unsupported", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_InPredicateWithInvalidChildren() {
        // Test IN predicate with invalid children
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal1 = new IntegerLiteral(1);
        SlotReference invalidSlot = new SlotReference("name", StringType.INSTANCE, false); // not a literal

        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(literal1, invalidSlot));

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema);
        });
        Assertions.assertEquals("IN predicate values must be literals", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_InPredicateWithNonSlotReference() {
        // Test IN predicate with non-slot reference left side
        IntegerLiteral literal1 = new IntegerLiteral(1);
        IntegerLiteral literal2 = new IntegerLiteral(2);

        InPredicate inPredicate = new InPredicate(literal1, Arrays.asList(literal2));

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema);
        });
        Assertions.assertEquals("Left side of IN predicate must be a column", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_InPredicateWithInsufficientChildren() {
        // Test IN predicate with insufficient children
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);

        InPredicate inPredicate = new InPredicate(slotRef, Collections.emptyList());

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema);
        });
        Assertions.assertEquals("IN predicate requires at least one value", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BinaryPredicateWithNonSlotReference() {
        // Test binary predicate with non-slot reference
        IntegerLiteral literal1 = new IntegerLiteral(100);
        IntegerLiteral literal2 = new IntegerLiteral(200);
        EqualTo equalTo = new EqualTo(literal1, literal2);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Binary predicate must be between a column and a literal", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BinaryPredicateWithNonLiteral() {
        // Test binary predicate with non-literal
        SlotReference slotRef1 = new SlotReference("id", IntegerType.INSTANCE, false);
        SlotReference slotRef2 = new SlotReference("age", IntegerType.INSTANCE, false);
        EqualTo equalTo = new EqualTo(slotRef1, slotRef2);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Binary predicate must be between a column and a literal", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_StringLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("name", StringType.INSTANCE, false);
        StringLiteral literal = new StringLiteral("John");
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("name", "John").toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BooleanLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("is_active", BooleanType.INSTANCE, false);
        BooleanLiteral literal = BooleanLiteral.of(true);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("is_active", true).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DoubleLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("salary", DoubleType.INSTANCE, false);
        DoubleLiteral literal = new DoubleLiteral(50000.5);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("salary", 50000.5).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_FloatLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("salary", FloatType.INSTANCE, false);
        FloatLiteral literal = new FloatLiteral(50000.5f);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("salary", 50000.5f).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BigIntLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("id", BigIntType.INSTANCE, false);
        BigIntLiteral literal = new BigIntLiteral(123456789L);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("id", 123456789L).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DecimalLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("salary", DecimalV2Type.SYSTEM_DEFAULT, false);
        DecimalLiteral literal = new DecimalLiteral(new BigDecimal("50000.50"));
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        // The exact string representation may vary, but should contain the decimal
        // value
        Assertions.assertTrue(result.toString().contains("50000.50"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DateLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("birth_date", DateType.INSTANCE, false);
        DateLiteral literal = new DateLiteral("2023-01-01");
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        // The exact string representation may vary, but should contain the date value
        Assertions.assertTrue(result.toString().contains("2023-01-01"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_CharLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("name", CharType.createCharType(1), false);
        CharLiteral literal = new CharLiteral("A", 1);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("name", "A").toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_TinyIntLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("age", TinyIntType.INSTANCE, false);
        TinyIntLiteral literal = new TinyIntLiteral((byte) 25);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("age", (byte) 25).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_SmallIntLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("age", SmallIntType.INSTANCE, false);
        SmallIntLiteral literal = new SmallIntLiteral((short) 25);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("age", (short) 25).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_VarcharLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("name", VarcharType.SYSTEM_DEFAULT, false);
        StringLiteral literal = new StringLiteral("John Doe");
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("name", "John Doe").toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_MixedLiteralTypesInInPredicate() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal1 = new IntegerLiteral(1);
        IntegerLiteral literal2 = new IntegerLiteral(2);
        IntegerLiteral literal3 = new IntegerLiteral(3);

        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(literal1, literal2, literal3));

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(inPredicate, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("id IN"));
        Assertions.assertTrue(result.toString().contains("1"));
        Assertions.assertTrue(result.toString().contains("2"));
        Assertions.assertTrue(result.toString().contains("3"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DeeplyNestedExpression() throws UserException {
        // Test deeply nested expression: NOT ((age > 18 AND salary >= 50000) OR (age <
        // 65 AND salary < 100000))
        SlotReference ageRef = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference salaryRef = new SlotReference("salary", DoubleType.INSTANCE, false);

        GreaterThan ageGt = new GreaterThan(ageRef, new IntegerLiteral(18));
        GreaterThanEqual salaryGte = new GreaterThanEqual(salaryRef, new DoubleLiteral(50000.0));
        And leftAnd = new And(ageGt, salaryGte);

        LessThan ageLt = new LessThan(ageRef, new IntegerLiteral(65));
        LessThan salaryLt = new LessThan(salaryRef, new DoubleLiteral(100000.0));
        And rightAnd = new And(ageLt, salaryLt);

        Or orExpr = new Or(leftAnd, rightAnd);
        Not notExpr = new Not(orExpr);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(notExpr, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("NOT"));
        Assertions.assertTrue(result.toString().contains("OR"));
        Assertions.assertTrue(result.toString().contains("AND"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_AllComparisonOperators() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(25);

        // Test all comparison operators
        EqualTo equalTo = new EqualTo(slotRef, literal);
        GreaterThan greaterThan = new GreaterThan(slotRef, literal);
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef, literal);
        LessThan lessThan = new LessThan(slotRef, literal);
        LessThanEqual lessThanEqual = new LessThanEqual(slotRef, literal);

        org.apache.iceberg.expressions.Expression equalResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);
        org.apache.iceberg.expressions.Expression greaterThanResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(greaterThan, testSchema);
        org.apache.iceberg.expressions.Expression greaterThanEqualResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(greaterThanEqual, testSchema);
        org.apache.iceberg.expressions.Expression lessThanResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(lessThan, testSchema);
        org.apache.iceberg.expressions.Expression lessThanEqualResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(lessThanEqual, testSchema);

        Assertions.assertNotNull(equalResult);
        Assertions.assertNotNull(greaterThanResult);
        Assertions.assertNotNull(greaterThanEqualResult);
        Assertions.assertNotNull(lessThanResult);
        Assertions.assertNotNull(lessThanEqualResult);

        Assertions.assertTrue(equalResult.toString().contains("age = 25"));
        Assertions.assertTrue(greaterThanResult.toString().contains("age > 25"));
        Assertions.assertTrue(greaterThanEqualResult.toString().contains("age >= 25"));
        Assertions.assertTrue(lessThanResult.toString().contains("age < 25"));
        Assertions.assertTrue(lessThanEqualResult.toString().contains("age <= 25"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ComplexInPredicate() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        List<Expression> literals = Arrays.asList(
                new IntegerLiteral(1),
                new IntegerLiteral(2),
                new IntegerLiteral(3),
                new IntegerLiteral(4),
                new IntegerLiteral(5));

        InPredicate inPredicate = new InPredicate(slotRef, literals);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(inPredicate, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("id IN"));
        Assertions.assertTrue(result.toString().contains("1"));
        Assertions.assertTrue(result.toString().contains("2"));
        Assertions.assertTrue(result.toString().contains("3"));
        Assertions.assertTrue(result.toString().contains("4"));
        Assertions.assertTrue(result.toString().contains("5"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_StringInPredicate() throws UserException {
        SlotReference slotRef = new SlotReference("name", StringType.INSTANCE, false);
        List<Expression> literals = Arrays.asList(
                new StringLiteral("Alice"),
                new StringLiteral("Bob"),
                new StringLiteral("Charlie"));

        InPredicate inPredicate = new InPredicate(slotRef, literals);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(inPredicate, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("name IN"));
        Assertions.assertTrue(result.toString().contains("Alice"));
        Assertions.assertTrue(result.toString().contains("Bob"));
        Assertions.assertTrue(result.toString().contains("Charlie"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BooleanInPredicate() throws UserException {
        SlotReference slotRef = new SlotReference("is_active", BooleanType.INSTANCE, false);
        List<Expression> literals = Arrays.asList(
                BooleanLiteral.of(true),
                BooleanLiteral.of(false));

        InPredicate inPredicate = new InPredicate(slotRef, literals);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(inPredicate, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("is_active IN"));
        Assertions.assertTrue(result.toString().contains("true"));
        Assertions.assertTrue(result.toString().contains("false"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ExceptionInLiteralExtraction() {
        // Test exception handling in literal value extraction
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        Literal mockLiteral = Mockito.mock(Literal.class);
        Mockito.when(mockLiteral.getValue()).thenThrow(new RuntimeException("Test exception"));

        EqualTo equalTo = new EqualTo(slotRef, mockLiteral);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Test exception", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ExceptionInInPredicateLiteralExtraction() {
        // Test exception handling in IN predicate literal extraction
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        Literal mockLiteral = Mockito.mock(Literal.class);
        Mockito.when(mockLiteral.getValue()).thenThrow(new RuntimeException("Test exception"));

        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(mockLiteral));

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema);
        });
        Assertions.assertEquals("Test exception", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_AllLogicalOperators() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        // Test all logical operators
        And andExpr = new And(equalTo, equalTo);
        Or orExpr = new Or(equalTo, equalTo);
        Not notExpr = new Not(equalTo);

        org.apache.iceberg.expressions.Expression andResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(andExpr, testSchema);
        org.apache.iceberg.expressions.Expression orResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(orExpr, testSchema);
        org.apache.iceberg.expressions.Expression notResult = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(notExpr, testSchema);

        Assertions.assertNotNull(andResult);
        Assertions.assertNotNull(orResult);
        Assertions.assertNotNull(notResult);

        Assertions.assertTrue(andResult.toString().contains("AND"));
        Assertions.assertTrue(orResult.toString().contains("OR"));
        Assertions.assertTrue(notResult.toString().contains("NOT"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_EmptySchema() {
        // Test with empty schema
        Schema emptySchema = new Schema();
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, emptySchema);
        });
        Assertions.assertEquals("Column not found in Iceberg schema: id", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_NullSchema() {
        // Test with null schema
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
                IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, null);
        });
        Assertions.assertEquals("Column not found in Iceberg schema: id", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_AllSupportedExpressionTypes() throws UserException {
        // Test all supported expression types in one comprehensive test
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);

        // Test all supported expressions
        EqualTo equalTo = new EqualTo(slotRef, literal);
        GreaterThan greaterThan = new GreaterThan(slotRef, literal);
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef, literal);
        LessThan lessThan = new LessThan(slotRef, literal);
        LessThanEqual lessThanEqual = new LessThanEqual(slotRef, literal);
        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(literal));
        And andExpr = new And(equalTo, greaterThan);
        Or orExpr = new Or(equalTo, greaterThan);
        Not notExpr = new Not(equalTo);

        // All should convert successfully
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(greaterThan, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(greaterThanEqual, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(lessThan, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(lessThanEqual, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(andExpr, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(orExpr, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(notExpr, testSchema));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_UnsupportedLiteralValue() {
            // Test with unsupported literal value
            SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
            Literal mockLiteral = Mockito.mock(Literal.class);
            Mockito.when(mockLiteral.getValue()).thenReturn(null);
            Mockito.when(mockLiteral instanceof NullLiteral).thenReturn(false);

            EqualTo equalTo = new EqualTo(slotRef, mockLiteral);

            UserException exception = Assertions.assertThrows(UserException.class, () -> {
                    IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
            });
            Assertions.assertEquals("Unsupported or null literal value for column: id", exception.getMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_InPredicateWithNullValue() {
            // Test IN predicate with null value
            SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
            Literal mockLiteral = Mockito.mock(Literal.class);
            Mockito.when(mockLiteral.getValue()).thenReturn(null);
            Mockito.when(mockLiteral instanceof NullLiteral).thenReturn(false);

            InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(mockLiteral));

            UserException exception = Assertions.assertThrows(UserException.class, () -> {
                    IcebergNereidsUtils.convertNereidsToIcebergExpression(inPredicate, testSchema);
            });
            Assertions.assertEquals("Null or unsupported value in IN predicate for column: id", exception.getMessage());
    }
}
