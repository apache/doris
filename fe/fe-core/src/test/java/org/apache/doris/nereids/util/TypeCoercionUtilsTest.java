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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
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
        DecimalType decimalType = DecimalType.createDecimalType(27, 9);
        Assertions.assertEquals(DecimalType.forType(bigIntType),
                TypeCoercionUtils.implicitCast(bigIntType, decimalType).get());
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
        DecimalType decimalType = DecimalType.SYSTEM_DEFAULT;
        Assertions.assertEquals(decimalType, TypeCoercionUtils.implicitCast(stringType, decimalType).get());
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
    public void testCannotImplicitCast() {
        BigIntType bigIntType = BigIntType.INSTANCE;
        NullType nullType = NullType.INSTANCE;
        Assertions.assertFalse(TypeCoercionUtils.implicitCast(bigIntType, nullType).isPresent());
    }

    @Test
    public void testCanHandleTypeCoercion() {
        DecimalType decimalType = DecimalType.SYSTEM_DEFAULT;
        NullType nullType = NullType.INSTANCE;
        SmallIntType smallIntType = SmallIntType.INSTANCE;
        IntegerType integerType = IntegerType.INSTANCE;
        Assertions.assertTrue(TypeCoercionUtils.canHandleTypeCoercion(decimalType, nullType));
        Assertions.assertTrue(TypeCoercionUtils.canHandleTypeCoercion(nullType, decimalType));
        Assertions.assertTrue(TypeCoercionUtils.canHandleTypeCoercion(smallIntType, integerType));
        Assertions.assertTrue(TypeCoercionUtils.canHandleTypeCoercion(integerType, decimalType));
        Assertions.assertTrue(TypeCoercionUtils.canHandleTypeCoercion(decimalType, integerType));
        Assertions.assertFalse(TypeCoercionUtils.canHandleTypeCoercion(integerType, integerType));
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
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DecimalType.SYSTEM_DEFAULT));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(CharType.createCharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(VarcharType.createVarcharType(10)));
        Assertions.assertTrue(TypeCoercionUtils.hasCharacterType(StringType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateTimeType.INSTANCE));
        Assertions.assertFalse(TypeCoercionUtils.hasCharacterType(DateType.INSTANCE));
    }

    @Test
    public void testFindTightestCommonType() {
        testFindTightestCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testFindTightestCommonType(IntegerType.INSTANCE, NullType.INSTANCE, IntegerType.INSTANCE);
        testFindTightestCommonType(IntegerType.INSTANCE, IntegerType.INSTANCE, NullType.INSTANCE);
        testFindTightestCommonType(DecimalType.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalType.SYSTEM_DEFAULT);
        testFindTightestCommonType(DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT, IntegerType.INSTANCE);
        testFindTightestCommonType(BigIntType.INSTANCE, BigIntType.INSTANCE, IntegerType.INSTANCE);
        testFindTightestCommonType(BigIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE);
        testFindTightestCommonType(StringType.INSTANCE, StringType.INSTANCE, IntegerType.INSTANCE);
        testFindTightestCommonType(StringType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE);
        testFindTightestCommonType(null, DecimalType.SYSTEM_DEFAULT, DecimalType.createDecimalType(2, 1));
        testFindTightestCommonType(VarcharType.createVarcharType(10), CharType.createCharType(8), CharType.createCharType(10));
        testFindTightestCommonType(VarcharType.createVarcharType(10), VarcharType.createVarcharType(8), VarcharType.createVarcharType(10));
        testFindTightestCommonType(VarcharType.createVarcharType(10), VarcharType.createVarcharType(8), CharType.createCharType(10));
        testFindTightestCommonType(VarcharType.createVarcharType(10), VarcharType.createVarcharType(10), CharType.createCharType(8));
        testFindTightestCommonType(StringType.INSTANCE, VarcharType.createVarcharType(10), StringType.INSTANCE);
        testFindTightestCommonType(StringType.INSTANCE, CharType.createCharType(8), StringType.INSTANCE);
    }

    private void testFindTightestCommonType(DataType commonType, DataType left, DataType right) {
        Assertions.assertEquals(Optional.ofNullable(commonType), TypeCoercionUtils.findTightestCommonType(left, right));
    }

    @Test
    public void testFindWiderTypeForDecimal() {
        Assertions.assertEquals(DecimalType.SYSTEM_DEFAULT,
                TypeCoercionUtils.findWiderTypeForDecimal(
                        DecimalType.SYSTEM_DEFAULT, DecimalType.SYSTEM_DEFAULT).get());
        Assertions.assertEquals(DecimalType.SYSTEM_DEFAULT,
                TypeCoercionUtils.findWiderTypeForDecimal(
                        DecimalType.SYSTEM_DEFAULT, TinyIntType.INSTANCE).get());
        Assertions.assertEquals(DecimalType.SYSTEM_DEFAULT,
                TypeCoercionUtils.findWiderTypeForDecimal(
                        TinyIntType.INSTANCE, DecimalType.SYSTEM_DEFAULT).get());
        Assertions.assertEquals(DoubleType.INSTANCE,
                TypeCoercionUtils.findWiderTypeForDecimal(
                        DecimalType.SYSTEM_DEFAULT, FloatType.INSTANCE).get());
        Assertions.assertEquals(DoubleType.INSTANCE,
                TypeCoercionUtils.findWiderTypeForDecimal(
                        DoubleType.INSTANCE, DecimalType.SYSTEM_DEFAULT).get());
        Assertions.assertFalse(TypeCoercionUtils.findWiderTypeForDecimal(
                StringType.INSTANCE, DecimalType.SYSTEM_DEFAULT).isPresent());
        Assertions.assertFalse(TypeCoercionUtils.findWiderTypeForDecimal(
                DecimalType.SYSTEM_DEFAULT, StringType.INSTANCE).isPresent());
    }

    @Test
    public void testCharacterPromotion() {
        Assertions.assertEquals(StringType.INSTANCE,
                TypeCoercionUtils.characterPromotion(StringType.INSTANCE, IntegerType.INSTANCE).get());
        Assertions.assertEquals(StringType.INSTANCE,
                TypeCoercionUtils.characterPromotion(IntegerType.INSTANCE, StringType.INSTANCE).get());
        Assertions.assertFalse(TypeCoercionUtils.characterPromotion(
                StringType.INSTANCE, BooleanType.INSTANCE).isPresent());
        Assertions.assertFalse(TypeCoercionUtils.characterPromotion(
                BooleanType.INSTANCE, StringType.INSTANCE).isPresent());
        Assertions.assertFalse(TypeCoercionUtils.characterPromotion(
                IntegerType.INSTANCE, IntegerType.INSTANCE).isPresent());
    }

    @Test
    public void testCastIfNotSameType() {
        Assertions.assertEquals(new DoubleLiteral(5L),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), DoubleType.INSTANCE));
        Assertions.assertEquals(new Cast(new DoubleLiteral(5L), BooleanType.INSTANCE),
                TypeCoercionUtils.castIfNotSameType(new DoubleLiteral(5L), BooleanType.INSTANCE));
    }
}
