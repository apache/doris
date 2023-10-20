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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class AbstractDataTypeTest {
    @Test
    public void testAnyAccept() {
        AnyDataType dateType = AnyDataType.INSTANCE;
        Assertions.assertTrue(dateType.acceptsType(NullType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(FloatType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertTrue(dateType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertTrue(dateType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertTrue(dateType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertTrue(dateType.acceptsType(StringType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(DateType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testNullTypeAccept() {
        NullType dateType = NullType.INSTANCE;
        Assertions.assertTrue(dateType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dateType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dateType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dateType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dateType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testBooleanAccept() {
        BooleanType dateType = BooleanType.INSTANCE;
        Assertions.assertFalse(dateType.acceptsType(NullType.INSTANCE));
        Assertions.assertTrue(dateType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dateType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dateType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dateType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dateType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dateType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testNumericAccept() {
        NumericType dataType = NumericType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertTrue(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testIntegralAccept() {
        IntegralType dataType = IntegralType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testTinyIntAccept() {
        TinyIntType dataType = TinyIntType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testSmallIntAccept() {
        SmallIntType dataType = SmallIntType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testIntAccept() {
        IntegerType dataType = IntegerType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testBigIntAccept() {
        BigIntType dataType = BigIntType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testLargeIntAccept() {
        LargeIntType dataType = LargeIntType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testFractionalAccept() {
        FractionalType dataType = FractionalType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertTrue(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testFloatAccept() {
        FloatType dataType = FloatType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testDoubleAccept() {
        DoubleType dataType = DoubleType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testDecimalAccept() {
        DecimalV2Type dataType = DecimalV2Type.SYSTEM_DEFAULT;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertTrue(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testCharAccept() {
        CharType dataType = CharType.createCharType(10);
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertTrue(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testVarcharAccept() {
        VarcharType dataType = VarcharType.SYSTEM_DEFAULT;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertTrue(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testStringAccept() {
        StringType dataType = StringType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertTrue(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertTrue(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testDateAccept() {
        DateType dataType = DateType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testDateTimeAccept() {
        DateTimeType dataType = DateTimeType.INSTANCE;
        Assertions.assertFalse(dataType.acceptsType(NullType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BooleanType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(TinyIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(SmallIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(IntegerType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(BigIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(LargeIntType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(FloatType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DoubleType.INSTANCE));
        int precision = Math.abs(new Random().nextInt() % (DecimalV2Type.MAX_PRECISION - 1)) + 1;
        int scale = Math.min(precision, Math.abs(new Random().nextInt() % DecimalV2Type.MAX_SCALE));
        Assertions.assertFalse(dataType.acceptsType(new DecimalV2Type(precision, scale)));
        Assertions.assertFalse(dataType.acceptsType(new CharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(new VarcharType(new Random().nextInt())));
        Assertions.assertFalse(dataType.acceptsType(StringType.INSTANCE));
        Assertions.assertFalse(dataType.acceptsType(DateType.INSTANCE));
        Assertions.assertTrue(dataType.acceptsType(DateTimeType.INSTANCE));
    }

    @Test
    public void testSimpleName() {
        Assertions.assertEquals("null", NullType.INSTANCE.simpleString());
        Assertions.assertEquals("boolean", BooleanType.INSTANCE.simpleString());
        Assertions.assertEquals("numeric", NumericType.INSTANCE.simpleString());
        Assertions.assertEquals("integral", IntegralType.INSTANCE.simpleString());
        Assertions.assertEquals("tinyint", TinyIntType.INSTANCE.simpleString());
        Assertions.assertEquals("smallint", SmallIntType.INSTANCE.simpleString());
        Assertions.assertEquals("int", IntegerType.INSTANCE.simpleString());
        Assertions.assertEquals("bigint", BigIntType.INSTANCE.simpleString());
        Assertions.assertEquals("largeint", LargeIntType.INSTANCE.simpleString());
        Assertions.assertEquals("fractional", FractionalType.INSTANCE.simpleString());
        Assertions.assertEquals("float", FloatType.INSTANCE.simpleString());
        Assertions.assertEquals("double", DoubleType.INSTANCE.simpleString());
        Assertions.assertEquals("decimal", DecimalV2Type.SYSTEM_DEFAULT.simpleString());
        Assertions.assertEquals("char", new CharType(10).simpleString());
        Assertions.assertEquals("varchar", VarcharType.SYSTEM_DEFAULT.simpleString());
        Assertions.assertEquals("string", StringType.INSTANCE.simpleString());
        Assertions.assertEquals("date", DateType.INSTANCE.simpleString());
        Assertions.assertEquals("datetime", DateTimeType.INSTANCE.simpleString());
    }

    @Test
    public void testDefaultConcreteType() {
        Assertions.assertEquals(NullType.INSTANCE, NullType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(BooleanType.INSTANCE, BooleanType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DoubleType.INSTANCE, NumericType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(BigIntType.INSTANCE, IntegralType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(TinyIntType.INSTANCE, TinyIntType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(SmallIntType.INSTANCE, SmallIntType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(IntegerType.INSTANCE, IntegerType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(BigIntType.INSTANCE, BigIntType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(LargeIntType.INSTANCE, LargeIntType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DoubleType.INSTANCE, FractionalType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(FloatType.INSTANCE, FloatType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DoubleType.INSTANCE, DoubleType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT.defaultConcreteType());
        Assertions.assertEquals(new CharType(10), new CharType(10).defaultConcreteType());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT.defaultConcreteType());
        Assertions.assertEquals(StringType.INSTANCE, StringType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DateType.INSTANCE, DateType.INSTANCE.defaultConcreteType());
        Assertions.assertEquals(DateTimeType.INSTANCE, DateTimeType.INSTANCE.defaultConcreteType());
    }
}
