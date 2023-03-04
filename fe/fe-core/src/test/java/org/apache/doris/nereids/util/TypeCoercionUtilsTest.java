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

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testFindPrimitiveCommonType() {
        testFindPrimitiveCommonType(null, ArrayType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, NullType.INSTANCE, ArrayType.SYSTEM_DEFAULT);

        testFindPrimitiveCommonType(NullType.INSTANCE, NullType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(BooleanType.INSTANCE, NullType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(TinyIntType.INSTANCE, NullType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, NullType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, NullType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, NullType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, NullType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, NullType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, NullType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, NullType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, NullType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(CharType.SYSTEM_DEFAULT, NullType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(VarcharType.SYSTEM_DEFAULT, NullType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, NullType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(DateType.INSTANCE, NullType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeType.INSTANCE, NullType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(DateV2Type.INSTANCE, NullType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, NullType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(TimeType.INSTANCE, NullType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(TimeV2Type.INSTANCE, NullType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(HllType.INSTANCE, NullType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(BitmapType.INSTANCE, NullType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(QuantileStateType.INSTANCE, NullType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(BooleanType.INSTANCE, BooleanType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(BooleanType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(TinyIntType.INSTANCE, BooleanType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, BooleanType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, BooleanType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BooleanType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, BooleanType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, BooleanType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BooleanType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, BooleanType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, BooleanType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, BooleanType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BooleanType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BooleanType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, BooleanType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(TinyIntType.INSTANCE, TinyIntType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(TinyIntType.INSTANCE, TinyIntType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(TinyIntType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, TinyIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, TinyIntType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, TinyIntType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, TinyIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, TinyIntType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TinyIntType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, TinyIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TinyIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TinyIntType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TinyIntType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TinyIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, TinyIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, SmallIntType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, SmallIntType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, SmallIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(SmallIntType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, SmallIntType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, SmallIntType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, SmallIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, SmallIntType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, SmallIntType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, SmallIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, SmallIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, SmallIntType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, SmallIntType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, SmallIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, SmallIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, IntegerType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, IntegerType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, IntegerType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, IntegerType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, IntegerType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, IntegerType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, IntegerType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, IntegerType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, IntegerType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, IntegerType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, IntegerType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, BigIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, BigIntType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, BigIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, BigIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, BigIntType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, BigIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BigIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BigIntType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, BigIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BigIntType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, BigIntType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, BigIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, LargeIntType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, LargeIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, LargeIntType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, LargeIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, LargeIntType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, LargeIntType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, LargeIntType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, LargeIntType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV2Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV2Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV3Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV3Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV3Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV3Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DecimalV3Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(FloatType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, FloatType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, FloatType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, FloatType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, FloatType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, FloatType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DoubleType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DoubleType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DoubleType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, DoubleType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DoubleType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DoubleType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DoubleType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DoubleType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DoubleType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(VarcharType.createVarcharType(10), CharType.createCharType(5),
                CharType.createCharType(10));
        testFindPrimitiveCommonType(VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, CharType.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, CharType.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, CharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(VarcharType.createVarcharType(10), VarcharType.createVarcharType(5),
                VarcharType.createVarcharType(10));
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, VarcharType.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, VarcharType.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, VarcharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, StringType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, StringType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, StringType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, StringType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DateType.INSTANCE, DateType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, DateType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, DateType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, DateType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(DateType.INSTANCE, DateType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeType.INSTANCE, DateType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(DateV2Type.INSTANCE, DateType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DateType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeType.INSTANCE, DateTimeType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, DateTimeType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DateTimeType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeType.INSTANCE, DateTimeType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeType.INSTANCE, DateTimeType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DateV2Type.INSTANCE, DateV2Type.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(IntegerType.INSTANCE, DateV2Type.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(BigIntType.INSTANCE, DateV2Type.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, DateV2Type.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateV2Type.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateV2Type.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(DateV2Type.INSTANCE, DateV2Type.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(DateV2Type.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DateV2Type.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(LargeIntType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, StringType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.SYSTEM_DEFAULT,
                DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, DateTimeV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(TimeType.INSTANCE, TimeType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, TimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, TimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(TimeType.INSTANCE, TimeType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(TimeV2Type.INSTANCE, TimeType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(DoubleType.INSTANCE, TimeV2Type.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeV2Type.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(StringType.INSTANCE, TimeV2Type.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(TimeV2Type.INSTANCE, TimeV2Type.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, TimeV2Type.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(HllType.INSTANCE, HllType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(HllType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, HllType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(BitmapType.INSTANCE, BitmapType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(BitmapType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(null, BitmapType.INSTANCE, QuantileStateType.INSTANCE);
        testFindPrimitiveCommonType(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, NullType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, BooleanType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, TinyIntType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, SmallIntType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, IntegerType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, BigIntType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, LargeIntType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, FloatType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DoubleType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, CharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, VarcharType.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, StringType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DateType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DateTimeType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DateV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, TimeType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, TimeV2Type.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, HllType.INSTANCE);
        testFindPrimitiveCommonType(null, QuantileStateType.INSTANCE, BitmapType.INSTANCE);
        testFindPrimitiveCommonType(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, QuantileStateType.INSTANCE);
    }

    private void testFindPrimitiveCommonType(DataType commonType, DataType left, DataType right) {
        Assertions.assertEquals(Optional.ofNullable(commonType),
                TypeCoercionUtils.findPrimitiveCommonType(left, right),
                "left: " + left + ", right: " + right);
    }

    @Test
    public void testCastIfNotSameType() {
        Assertions.assertEquals(new DoubleLiteral(5L),
                TypeCoercionUtils.castIfNotMatchType(new DoubleLiteral(5L), DoubleType.INSTANCE));
        Assertions.assertEquals(new Cast(new DoubleLiteral(5L), BooleanType.INSTANCE),
                TypeCoercionUtils.castIfNotMatchType(new DoubleLiteral(5L), BooleanType.INSTANCE));
    }
}
