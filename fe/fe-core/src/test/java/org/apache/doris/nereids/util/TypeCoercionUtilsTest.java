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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class TypeCoercionUtilsTest {
    @Test
    public void testImplicitCastAccept() {
        IntegerType integerType = IntegerType.INSTANCE;
        IntegralType integralType = IntegralType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(integerType, integralType).get());
    }

    @Test
    public void testImplicitCastNullType() {
        NullType nullType = NullType.INSTANCE;
        IntegralType integralType = IntegralType.INSTANCE;
        Assertions.assertEquals(integralType.defaultConcreteType(),
                TypeCoercionUtils.implicitCast(nullType, integralType).get());
    }

    @Test
    public void testImplicitCastNumericWithExpectDecimal() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        DecimalV2Type decimalV2Type = DecimalV2Type.createDecimalV2Type(27, 9);
        Assertions.assertEquals(DecimalV2Type.forType(bigIntType),
                TypeCoercionUtils.implicitCast(bigIntType, decimalV2Type).get());
    }

    @Test
    public void testImplicitCastNumericWithExpectNumeric() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        IntegerType integerType = IntegerType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(bigIntType, integerType).get());
    }

    @Test
    public void testImplicitCastStringToDecimal() {
        StringType stringType = StringType.INSTANCE;
        DecimalV2Type decimalV2Type = DecimalV2Type.SYSTEM_DEFAULT;
        Assertions.assertEquals(decimalV2Type, TypeCoercionUtils.implicitCast(stringType, decimalV2Type).get());
    }

    @Test
    public void testImplicitCastStringToNumeric() {
        VarcharType varcharType = VarcharType.createVarcharType(10);
        IntegerType integerType = IntegerType.INSTANCE;
        Assertions.assertEquals(integerType, TypeCoercionUtils.implicitCast(varcharType, integerType).get());
    }

    @Test
    public void testImplicitCastFromPrimitiveToString() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        StringType stringType = StringType.INSTANCE;
        Assertions.assertEquals(stringType, TypeCoercionUtils.implicitCast(bigIntType, stringType).get());
    }

    @Test
    public void testHasCharacterType() {
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(NullType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(BooleanType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(TinyIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(SmallIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(IntegerType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(BigIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(LargeIntType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(FloatType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DoubleType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DecimalV2Type.SYSTEM_DEFAULT));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(CharType.createCharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(VarcharType.createVarcharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(StringType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateTimeType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateType.INSTANCE));
    }

    @Test
    public void testCastIfNotSameType() {
        Assertions.assertEquals(new DoubleLiteral(5L),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), DoubleType.INSTANCE));
        Assertions.assertEquals(new Cast(new DoubleLiteral(5L), BooleanType.INSTANCE),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), BooleanType.INSTANCE));
        Assertions.assertEquals(new StringLiteral("varchar"),
                TypeCoercionUtils.castIfNotSameType(new VarcharLiteral("varchar"), StringType.INSTANCE));
        Assertions.assertEquals(new StringLiteral("char"),
                TypeCoercionUtils.castIfNotSameType(new CharLiteral("char", 4), StringType.INSTANCE));
        Assertions.assertEquals(new CharLiteral("char", 4),
                TypeCoercionUtils.castIfNotSameType(new CharLiteral("char", 4), VarcharType.createVarcharType(100)));
        Assertions.assertEquals(new StringLiteral("string"),
                TypeCoercionUtils.castIfNotSameType(new StringLiteral("string"), VarcharType.createVarcharType(100)));

    }

    @Test
    public void testDecimalArithmetic() {
        Multiply multiply = new Multiply(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        Expression expression = TypeCoercionUtils.processBinaryArithmetic(multiply);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(9, 3)));

        Divide divide = new Divide(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(divide);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(9, 3)));

        Add add = new Add(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(add);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(10, 3)));

        Subtract sub = new Subtract(new DecimalLiteral(new BigDecimal("987654.321")),
                new DecimalV3Literal(new BigDecimal("123.45")));
        expression = TypeCoercionUtils.processBinaryArithmetic(sub);
        Assertions.assertEquals(expression.child(0),
                new Cast(multiply.child(0), DecimalV3Type.createDecimalV3Type(10, 3)));
    }

    @Test
    public void testProcessInDowngrade() {
        // DecimalV2 slot vs DecimalV3 literal
        InPredicate decimalDowngrade = new InPredicate(
                new SlotReference("c1", DecimalV2Type.createDecimalV2Type(15, 6)),
                ImmutableList.of(
                        new DecimalV3Literal(BigDecimal.valueOf(12345.1234567)),
                        new DecimalLiteral(BigDecimal.valueOf(12345.1234))));
        decimalDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(decimalDowngrade);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(16, 7), decimalDowngrade.getCompareExpr().getDataType());

        // DateV1 slot vs DateV2 literal
        InPredicate dateDowngrade = new InPredicate(
                new SlotReference("c1", DateType.INSTANCE),
                ImmutableList.of(
                        new DateLiteral(2024, 4, 12),
                        new DateV2Literal(2024, 4, 12)));
        dateDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(dateDowngrade);
        Assertions.assertEquals(DateType.INSTANCE, dateDowngrade.getCompareExpr().getDataType());

        // DatetimeV1 slot vs DateLike literal
        InPredicate datetimeDowngrade = new InPredicate(
                new SlotReference("c1", DateTimeType.INSTANCE),
                ImmutableList.of(
                        new DateLiteral(2024, 4, 12),
                        new DateV2Literal(2024, 4, 12),
                        new DateTimeLiteral(2024, 4, 12, 18, 25, 30),
                        new DateTimeV2Literal(2024, 4, 12, 18, 25, 30, 0)));
        datetimeDowngrade = (InPredicate) TypeCoercionUtils.processInPredicate(datetimeDowngrade);
        Assertions.assertEquals(DateTimeType.INSTANCE, datetimeDowngrade.getCompareExpr().getDataType());
    }

    @Test
    public void testProcessComparisonPredicateDowngrade() {
        // DecimalV2 slot vs DecimalV3 literal
        EqualTo decimalDowngrade = new EqualTo(
                new SlotReference("c1", DecimalV2Type.createDecimalV2Type(15, 6)),
                new DecimalV3Literal(BigDecimal.valueOf(12345.1234567))
        );
        decimalDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(decimalDowngrade);
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(16, 7), decimalDowngrade.left().getDataType());

        // DateV1 slot vs DateV2 literal (this case cover right slot vs left literal)
        EqualTo dateDowngrade = new EqualTo(
                new DateV2Literal(2024, 4, 12),
                new SlotReference("c1", DateType.INSTANCE)
        );
        dateDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(dateDowngrade);
        Assertions.assertEquals(DateType.INSTANCE, dateDowngrade.left().getDataType());

        // DatetimeV1 slot vs DateLike literal
        EqualTo datetimeDowngrade = new EqualTo(
                new SlotReference("c1", DateTimeType.INSTANCE),
                new DateTimeV2Literal(2024, 4, 12, 18, 25, 30, 0)
        );
        datetimeDowngrade = (EqualTo) TypeCoercionUtils.processComparisonPredicate(datetimeDowngrade);
        Assertions.assertEquals(DateTimeType.INSTANCE, datetimeDowngrade.left().getDataType());
    }

    @Test
    public void testProcessInStringCoercion() {
        // BigInt slot vs String literal
        InPredicate bigintString = new InPredicate(
                new SlotReference("c1", BigIntType.INSTANCE),
                ImmutableList.of(
                        new VarcharLiteral("200"),
                        new VarcharLiteral("922337203685477001")));
        bigintString = (InPredicate) TypeCoercionUtils.processInPredicate(bigintString);
        Assertions.assertEquals(BigIntType.INSTANCE, bigintString.getCompareExpr().getDataType());
        Assertions.assertEquals(BigIntType.INSTANCE, bigintString.getOptions().get(0).getDataType());

        // SmallInt slot vs String literal
        InPredicate smallIntString = new InPredicate(
                new SlotReference("c1", SmallIntType.INSTANCE),
                ImmutableList.of(
                        new DecimalLiteral(new BigDecimal("987654.321")),
                        new VarcharLiteral("922337203685477001")));
        smallIntString = (InPredicate) TypeCoercionUtils.processInPredicate(smallIntString);
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(23, 3), smallIntString.getCompareExpr().getDataType());
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(23, 3), smallIntString.getOptions().get(0).getDataType());
    }

    @Test
    public void testCharacterLiteralTypeCoercion() {
        // datev2
        Assertions.assertEquals(DateV2Type.INSTANCE,
                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateV2Type.INSTANCE).get().getDataType());
        // datetimev2
        Assertions.assertEquals(DateTimeV2Type.of(0),
                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateTimeV2Type.of(0)).get().getDataType());
        // date
        Assertions.assertEquals(DateV2Type.INSTANCE,
                        TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateType.INSTANCE).get()
                                        .getDataType());
        // datetime
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT,
                                TypeCoercionUtils.characterLiteralTypeCoercion("2020-02-02", DateTimeType.INSTANCE).get().getDataType());
    }

    @Test
    public void testGetNumResultType() {
        // Numeric type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(TinyIntType.INSTANCE));
        Assertions.assertEquals(SmallIntType.INSTANCE, TypeCoercionUtils.getNumResultType(SmallIntType.INSTANCE));
        Assertions.assertEquals(IntegerType.INSTANCE, TypeCoercionUtils.getNumResultType(IntegerType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(BigIntType.INSTANCE));
        Assertions.assertEquals(LargeIntType.INSTANCE, TypeCoercionUtils.getNumResultType(LargeIntType.INSTANCE));
        Assertions.assertEquals(FloatType.INSTANCE, TypeCoercionUtils.getNumResultType(FloatType.INSTANCE));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(DoubleType.INSTANCE));
        Assertions.assertEquals(DecimalV3Type.INSTANCE, TypeCoercionUtils.getNumResultType(DecimalV3Type.INSTANCE));
        // Null type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(NullType.INSTANCE));
        // Boolean type
        Assertions.assertEquals(TinyIntType.INSTANCE, TypeCoercionUtils.getNumResultType(BooleanType.INSTANCE));
        // Date like type
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateV2Type.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateTimeType.INSTANCE));
        Assertions.assertEquals(BigIntType.INSTANCE, TypeCoercionUtils.getNumResultType(DateTimeV2Type.SYSTEM_DEFAULT));
        // String like type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(StringType.INSTANCE));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(VarcharType.SYSTEM_DEFAULT));
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(CharType.SYSTEM_DEFAULT));
        // Hll type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(HllType.INSTANCE));
        // Time type
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(TimeV2Type.SYSTEM_DEFAULT));
        // Json
        Assertions.assertEquals(DoubleType.INSTANCE, TypeCoercionUtils.getNumResultType(JsonType.INSTANCE));
        // Other
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(BitmapType.INSTANCE));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(ArrayType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(MapType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(StructType.SYSTEM_DEFAULT));
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.getNumResultType(QuantileStateType.INSTANCE));
    }
}
