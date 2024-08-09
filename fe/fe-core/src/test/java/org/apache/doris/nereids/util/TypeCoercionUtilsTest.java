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
import org.apache.doris.nereids.types.DataType;
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
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

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
    public void testFindCommonPrimitiveTypeForCaseWhen() {
        testFindCommonPrimitiveTypeForCaseWhen(null, ArrayType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, NullType.INSTANCE, ArrayType.SYSTEM_DEFAULT);

        testFindCommonPrimitiveTypeForCaseWhen(NullType.INSTANCE, NullType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BooleanType.INSTANCE, NullType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TinyIntType.INSTANCE, NullType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, NullType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, NullType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, NullType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, NullType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, NullType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, NullType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, NullType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, NullType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(CharType.SYSTEM_DEFAULT, NullType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.SYSTEM_DEFAULT, NullType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, NullType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateType.INSTANCE, NullType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeType.INSTANCE, NullType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateV2Type.INSTANCE, NullType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, NullType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(TimeType.INSTANCE, NullType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TimeV2Type.INSTANCE, NullType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(HllType.INSTANCE, NullType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BitmapType.INSTANCE, NullType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(QuantileStateType.INSTANCE, NullType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BooleanType.INSTANCE, BooleanType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BooleanType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TinyIntType.INSTANCE, BooleanType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, BooleanType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, BooleanType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BooleanType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, BooleanType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, BooleanType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BooleanType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BooleanType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BooleanType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BooleanType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BooleanType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BooleanType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BooleanType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TinyIntType.INSTANCE, TinyIntType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TinyIntType.INSTANCE, TinyIntType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TinyIntType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, TinyIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, TinyIntType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, TinyIntType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, TinyIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, TinyIntType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TinyIntType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TinyIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TinyIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TinyIntType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TinyIntType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TinyIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TinyIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, SmallIntType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, SmallIntType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, SmallIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(SmallIntType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, SmallIntType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, SmallIntType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, SmallIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, SmallIntType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, SmallIntType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, SmallIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, SmallIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, SmallIntType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, SmallIntType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, SmallIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, SmallIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, IntegerType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, IntegerType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, IntegerType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, IntegerType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, IntegerType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, IntegerType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, IntegerType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, IntegerType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, IntegerType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, IntegerType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, IntegerType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, IntegerType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, BigIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, BigIntType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BigIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BigIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, BigIntType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BigIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, BigIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BigIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BigIntType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, BigIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BigIntType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BigIntType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BigIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.createDecimalV3Type(38), LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38));
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, LargeIntType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, LargeIntType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, LargeIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, LargeIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, LargeIntType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, LargeIntType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, LargeIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, LargeIntType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, LargeIntType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, LargeIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV2Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV2Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.createDecimalV3Type(38), DecimalV3Type.createDecimalV3Type(38), LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DecimalV2Type.createDecimalV2Type(27, 0));
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV3Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV3Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV3Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV3Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DecimalV3Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(FloatType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, FloatType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, FloatType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, FloatType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, FloatType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, FloatType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DoubleType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DoubleType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DoubleType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DoubleType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DoubleType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DoubleType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DoubleType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DoubleType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DoubleType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.createVarcharType(10), CharType.createCharType(5),
                CharType.createCharType(10));
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, CharType.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, CharType.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, CharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(VarcharType.createVarcharType(10), VarcharType.createVarcharType(5),
                VarcharType.createVarcharType(10));
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, VarcharType.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, VarcharType.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, VarcharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, StringType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, StringType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, StringType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, StringType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateType.INSTANCE, DateType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, DateType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, DateType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, DateType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateType.INSTANCE, DateType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeType.INSTANCE, DateType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateV2Type.INSTANCE, DateType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeType.INSTANCE, DateTimeType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, DateTimeType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DateTimeType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeType.INSTANCE, DateTimeType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeType.INSTANCE, DateTimeType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateV2Type.INSTANCE, DateV2Type.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(IntegerType.INSTANCE, DateV2Type.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BigIntType.INSTANCE, DateV2Type.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, DateV2Type.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateV2Type.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateV2Type.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateV2Type.INSTANCE, DateV2Type.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateV2Type.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateV2Type.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(LargeIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, DateTimeV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TimeType.INSTANCE, TimeType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, TimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(TimeType.INSTANCE, TimeType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TimeV2Type.INSTANCE, TimeType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(DoubleType.INSTANCE, TimeV2Type.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeV2Type.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(StringType.INSTANCE, TimeV2Type.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, TimeV2Type.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(HllType.INSTANCE, HllType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(HllType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, HllType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BitmapType.INSTANCE, BitmapType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(BitmapType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, BitmapType.INSTANCE, QuantileStateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, NullType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, BooleanType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, TinyIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, SmallIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, IntegerType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, BigIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, LargeIntType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, FloatType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DoubleType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, StringType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DateType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DateTimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DateV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, TimeType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, TimeV2Type.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, HllType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(null, QuantileStateType.INSTANCE, BitmapType.INSTANCE);
        testFindCommonPrimitiveTypeForCaseWhen(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, QuantileStateType.INSTANCE);
    }

    private void testFindCommonPrimitiveTypeForCaseWhen(DataType commonType, DataType left, DataType right) {
        Assertions.assertEquals(Optional.ofNullable(commonType),
                TypeCoercionUtils.findCommonPrimitiveTypeForCaseWhen(left, right),
                "left: " + left + ", right: " + right);
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
}
