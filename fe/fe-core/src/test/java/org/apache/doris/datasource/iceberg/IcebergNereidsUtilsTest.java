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
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
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
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
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
                Types.NestedField.required(5, "is_active", Types.BooleanType.get()),
                Types.NestedField.required(6, "birth_date", Types.DateType.get()),
                Types.NestedField.required(7, "event_time_tz", Types.TimestampType.withZone()),
                Types.NestedField.required(8, "event_time_ntz", Types.TimestampType.withoutZone()),
                Types.NestedField.required(9, "dec_col", Types.DecimalType.of(10, 2)),
                Types.NestedField.required(10, "time_col", Types.TimeType.get()));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_NullInput() {
        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(null, testSchema);
        });
        Assertions.assertEquals("Nereids expression is null", exception.getDetailMessage());
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
        Assertions.assertTrue(result.toString().toLowerCase().contains("not"));
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
        String s = result.toString();
        Assertions.assertTrue(s.contains("id"));
        Assertions.assertTrue(s.contains("1"));
        Assertions.assertTrue(s.contains("2"));
        Assertions.assertTrue(s.contains("3"));
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
        Assertions.assertTrue(result.toString().toLowerCase().contains("or"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_WithNullLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        NullLiteral nullLiteral = new NullLiteral();
        EqualTo equalTo = new EqualTo(slotRef, nullLiteral);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.isNull("id").toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_ColumnNotFound() {
        SlotReference slotRef = new SlotReference("non_existent_column", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(equalTo, testSchema);
        });
        Assertions.assertEquals("Column not found in Iceberg schema: non_existent_column",
                exception.getDetailMessage());
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
        Assertions.assertTrue(exception.getDetailMessage().contains("Unsupported expression type"));
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
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DateLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("birth_date", DateType.INSTANCE, false);
        DateLiteral literal = new DateLiteral("2023-01-01");
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("birth_date", "2023-01-01").toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_TimestampWithZoneMicros() throws UserException {
        org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal literal =
                new org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal(
                        org.apache.doris.nereids.types.DateTimeV2Type.forTypeFromString("2023-01-02 03:04:05.123456"),
                        2023, 1, 2, 3, 4, 5, 123456);
        EqualTo equalTo = new EqualTo(new SlotReference("event_time_tz",
                org.apache.doris.nereids.types.DateTimeV2Type.forTypeFromString("2023-01-02 03:04:05.123456"), false),
                literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        java.time.ZoneId zone = org.apache.doris.nereids.util.DateUtils.getTimeZone();
        java.time.LocalDateTime ldt = java.time.LocalDateTime.of(2023, 1, 2, 3, 4, 5, 123456000);
        long expectedMicros = ldt.atZone(zone).toInstant().toEpochMilli() * 1000L + 123456;
        Assertions.assertEquals(Expressions.equal("event_time_tz", expectedMicros).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_TimestampWithoutZoneMicros() throws UserException {
        org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral literal =
                new org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral(2023, 1, 2, 3, 4, 5);
        EqualTo equalTo = new EqualTo(new SlotReference("event_time_ntz",
                org.apache.doris.nereids.types.DateTimeType.INSTANCE, false), literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        java.time.ZoneId zone = java.time.ZoneId.of("UTC");
        java.time.LocalDateTime ldt = java.time.LocalDateTime.of(2023, 1, 2, 3, 4, 5, 0);
        long expectedMicros = ldt.atZone(zone).toInstant().toEpochMilli() * 1000L;
        Assertions.assertEquals(Expressions.equal("event_time_ntz", expectedMicros).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DecimalMapping() throws UserException {
        SlotReference slotRef = new SlotReference("dec_col", DecimalV2Type.SYSTEM_DEFAULT, false);
        DecimalLiteral literal = new DecimalLiteral(new BigDecimal("12.34"));
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("12.34"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_DecimalV3Mapping() throws UserException {
        SlotReference slotRef = new SlotReference("dec_col", DecimalV2Type.SYSTEM_DEFAULT, false);
        DecimalV3Literal literal =
                new DecimalV3Literal(new BigDecimal("99.990"));
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("99.99"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_TimeAsLong() throws UserException {
        SlotReference slotRef = new SlotReference("time_col", IntegerType.INSTANCE, false);
        // use a numeric literal to represent micros since midnight
        BigIntLiteral literal = new BigIntLiteral(12_345_678L);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("time_col", 12_345_678L).toString(), result.toString());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_StringToIntParsing() throws UserException {
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        StringLiteral literal = new StringLiteral("123");
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(Expressions.equal("id", 123).toString(), result.toString());
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
        Assertions.assertTrue(result.toString().contains("age"));
        Assertions.assertTrue(result.toString().contains("25"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_SmallIntLiteral() throws UserException {
        SlotReference slotRef = new SlotReference("age", SmallIntType.INSTANCE, false);
        SmallIntLiteral literal = new SmallIntLiteral((short) 25);
        EqualTo equalTo = new EqualTo(slotRef, literal);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(equalTo, testSchema);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.toString().contains("age"));
        Assertions.assertTrue(result.toString().contains("25"));
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
        String s = result.toString();
        Assertions.assertTrue(s.contains("id"));
        Assertions.assertTrue(s.contains("1"));
        Assertions.assertTrue(s.contains("2"));
        Assertions.assertTrue(s.contains("3"));
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
        String s = result.toString().toLowerCase();
        Assertions.assertTrue(s.contains("not"));
        Assertions.assertTrue(s.contains("or"));
        Assertions.assertTrue(s.contains("and"));
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

        String eq = equalResult.toString().toLowerCase();
        String gt = greaterThanResult.toString().toLowerCase();
        String gte = greaterThanEqualResult.toString().toLowerCase();
        String lt = lessThanResult.toString().toLowerCase();
        String lte = lessThanEqualResult.toString().toLowerCase();
        Assertions.assertTrue(eq.contains("age") && eq.contains("25"));
        Assertions.assertTrue(gt.contains("age") && gt.contains(">") && gt.contains("25"));
        Assertions.assertTrue(gte.contains("age") && gte.contains(">=") && gte.contains("25"));
        Assertions.assertTrue(lt.contains("age") && lt.contains("<") && lt.contains("25"));
        Assertions.assertTrue(lte.contains("age") && lte.contains("<=") && lte.contains("25"));
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
        String s = result.toString();
        Assertions.assertTrue(s.contains("id"));
        Assertions.assertTrue(s.contains("1"));
        Assertions.assertTrue(s.contains("2"));
        Assertions.assertTrue(s.contains("3"));
        Assertions.assertTrue(s.contains("4"));
        Assertions.assertTrue(s.contains("5"));
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
        String s = result.toString();
        Assertions.assertTrue(s.contains("name"));
        Assertions.assertTrue(s.contains("Alice"));
        Assertions.assertTrue(s.contains("Bob"));
        Assertions.assertTrue(s.contains("Charlie"));
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
        String s = result.toString().toLowerCase();
        Assertions.assertTrue(s.contains("is_active"));
        Assertions.assertTrue(s.contains("true"));
        Assertions.assertTrue(s.contains("false"));
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

        String andStr = andResult.toString().toLowerCase();
        String orStr = orResult.toString().toLowerCase();
        String notStr = notResult.toString().toLowerCase();
        Assertions.assertTrue(andStr.contains("and"));
        Assertions.assertTrue(orStr.contains("or"));
        Assertions.assertTrue(notStr.contains("not"));
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
        Assertions.assertEquals("Column not found in Iceberg schema: id", exception.getDetailMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_AllSupportedExpressionTypes() throws UserException {
        // Test all supported expression types in one comprehensive test
        SlotReference slotRef = new SlotReference("id", IntegerType.INSTANCE, false);
        IntegerLiteral literal = new IntegerLiteral(100);
        IntegerLiteral lowerBound = new IntegerLiteral(50);
        IntegerLiteral upperBound = new IntegerLiteral(150);

        // Test all supported expressions
        EqualTo equalTo = new EqualTo(slotRef, literal);
        GreaterThan greaterThan = new GreaterThan(slotRef, literal);
        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(slotRef, literal);
        LessThan lessThan = new LessThan(slotRef, literal);
        LessThanEqual lessThanEqual = new LessThanEqual(slotRef, literal);
        InPredicate inPredicate = new InPredicate(slotRef, Arrays.asList(literal));
        Between between = new Between(slotRef, lowerBound, upperBound);
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
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(andExpr, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(orExpr, testSchema));
        Assertions.assertNotNull(IcebergNereidsUtils.convertNereidsToIcebergExpression(notExpr, testSchema));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_Between() throws UserException {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(slotRef, lowerBound, upperBound);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(between, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString().toLowerCase();
        Assertions.assertTrue(resultStr.contains("age"));
        Assertions.assertTrue(resultStr.contains("18"));
        Assertions.assertTrue(resultStr.contains("65"));
        Assertions.assertTrue(resultStr.contains("and"));
        // Verify it's equivalent to: age >= 18 AND age <= 65
        Assertions.assertTrue(resultStr.contains(">=") || resultStr.contains("greaterthanequal"));
        Assertions.assertTrue(resultStr.contains("<=") || resultStr.contains("lessthanequal"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithUnboundSlot() throws UserException {
        UnboundSlot unboundSlot = new UnboundSlot("age");
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(unboundSlot, lowerBound, upperBound);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(between, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString().toLowerCase();
        Assertions.assertTrue(resultStr.contains("age"));
        Assertions.assertTrue(resultStr.contains("18"));
        Assertions.assertTrue(resultStr.contains("65"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithDouble() throws UserException {
        SlotReference slotRef = new SlotReference("salary", DoubleType.INSTANCE, false);
        DoubleLiteral lowerBound = new DoubleLiteral(10000.0);
        DoubleLiteral upperBound = new DoubleLiteral(100000.0);
        Between between = new Between(slotRef, lowerBound, upperBound);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(between, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString().toLowerCase();
        Assertions.assertTrue(resultStr.contains("salary"));
        Assertions.assertTrue(resultStr.contains("10000"));
        Assertions.assertTrue(resultStr.contains("100000"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithString() throws UserException {
        SlotReference slotRef = new SlotReference("name", StringType.INSTANCE, false);
        StringLiteral lowerBound = new StringLiteral("Alice");
        StringLiteral upperBound = new StringLiteral("Charlie");
        Between between = new Between(slotRef, lowerBound, upperBound);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(between, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString();
        Assertions.assertTrue(resultStr.contains("name"));
        Assertions.assertTrue(resultStr.contains("Alice"));
        Assertions.assertTrue(resultStr.contains("Charlie"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithDate() throws UserException {
        SlotReference slotRef = new SlotReference("birth_date", DateType.INSTANCE, false);
        DateLiteral lowerBound = new DateLiteral("2000-01-01");
        DateLiteral upperBound = new DateLiteral("2010-12-31");
        Between between = new Between(slotRef, lowerBound, upperBound);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(between, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString();
        Assertions.assertTrue(resultStr.contains("birth_date"));
        Assertions.assertTrue(resultStr.contains("2000-01-01"));
        Assertions.assertTrue(resultStr.contains("2010-12-31"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenInvalidCompareExpr() {
        // Test with non-slot compareExpr
        IntegerLiteral compareExpr = new IntegerLiteral(100);
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(compareExpr, lowerBound, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertTrue(exception.getDetailMessage().contains("must be a slot"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenInvalidLowerBound() {
        // Test with non-literal lowerBound
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference lowerBound = new SlotReference("min_age", IntegerType.INSTANCE, false);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(slotRef, lowerBound, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertTrue(exception.getDetailMessage().contains("Lower bound")
                && exception.getDetailMessage().contains("must be a literal"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenInvalidUpperBound() {
        // Test with non-literal upperBound
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        SlotReference upperBound = new SlotReference("max_age", IntegerType.INSTANCE, false);
        Between between = new Between(slotRef, lowerBound, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertTrue(exception.getDetailMessage().contains("Upper bound")
                && exception.getDetailMessage().contains("must be a literal"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenColumnNotFound() {
        SlotReference slotRef = new SlotReference("non_existent_column", IntegerType.INSTANCE, false);
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(slotRef, lowerBound, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertEquals("Column not found in Iceberg schema: non_existent_column",
                exception.getDetailMessage());
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithNullBounds() {
        SlotReference slotRef = new SlotReference("age", IntegerType.INSTANCE, false);
        NullLiteral nullLiteral = new NullLiteral();
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(slotRef, nullLiteral, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertTrue(exception.getDetailMessage().contains("cannot be null"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenInComplexExpression() throws UserException {
        // Test BETWEEN in AND expression: age BETWEEN 18 AND 65 AND salary > 50000
        SlotReference ageRef = new SlotReference("age", IntegerType.INSTANCE, false);
        SlotReference salaryRef = new SlotReference("salary", DoubleType.INSTANCE, false);

        Between between = new Between(ageRef, new IntegerLiteral(18), new IntegerLiteral(65));
        GreaterThan salaryGt = new GreaterThan(salaryRef, new DoubleLiteral(50000.0));
        And andExpr = new And(between, salaryGt);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(andExpr, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString().toLowerCase();
        Assertions.assertTrue(resultStr.contains("age"));
        Assertions.assertTrue(resultStr.contains("salary"));
        Assertions.assertTrue(resultStr.contains("and"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenWithUnboundSlotInvalidNameParts() {
        // Test UnboundSlot with multiple nameParts (should fail)
        UnboundSlot unboundSlot = new UnboundSlot("table", "age");
        IntegerLiteral lowerBound = new IntegerLiteral(18);
        IntegerLiteral upperBound = new IntegerLiteral(65);
        Between between = new Between(unboundSlot, lowerBound, upperBound);

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            IcebergNereidsUtils.convertNereidsToIcebergExpression(between, testSchema);
        });
        Assertions.assertTrue(exception.getDetailMessage().contains("single name part"));
    }

    @Test
    public void testConvertNereidsToIcebergExpression_BetweenNestedInOr() throws UserException {
        // Test: (age BETWEEN 18 AND 30) OR (age BETWEEN 50 AND 65)
        SlotReference ageRef = new SlotReference("age", IntegerType.INSTANCE, false);

        Between between1 = new Between(ageRef, new IntegerLiteral(18), new IntegerLiteral(30));
        Between between2 = new Between(ageRef, new IntegerLiteral(50), new IntegerLiteral(65));
        Or orExpr = new Or(between1, between2);

        org.apache.iceberg.expressions.Expression result = IcebergNereidsUtils
                .convertNereidsToIcebergExpression(orExpr, testSchema);

        Assertions.assertNotNull(result);
        String resultStr = result.toString().toLowerCase();
        Assertions.assertTrue(resultStr.contains("age"));
        Assertions.assertTrue(resultStr.contains("or"));
        Assertions.assertTrue(resultStr.contains("18"));
        Assertions.assertTrue(resultStr.contains("30"));
        Assertions.assertTrue(resultStr.contains("50"));
        Assertions.assertTrue(resultStr.contains("65"));
    }
}
