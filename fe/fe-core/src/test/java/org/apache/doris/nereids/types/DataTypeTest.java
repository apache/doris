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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class DataTypeTest {
    @Test
    public void testDataTypeEquals() {
        DecimalV2Type decimalV2Type1 = DecimalV2Type.createDecimalV2Type(27, 9);
        DecimalV2Type decimalV2Type2 = DecimalV2Type.createDecimalV2Type(27, 9);
        DecimalV2Type decimalV2Type3 = DecimalV2Type.createDecimalV2Type(26, 9);
        DecimalV2Type decimalV2Type4 = DecimalV2Type.createDecimalV2Type(27, 8);
        Assertions.assertEquals(decimalV2Type1, decimalV2Type2);
        Assertions.assertEquals(decimalV2Type1.hashCode(), decimalV2Type2.hashCode());
        Assertions.assertNotEquals(decimalV2Type1, decimalV2Type3);
        Assertions.assertNotEquals(decimalV2Type1.hashCode(), decimalV2Type3.hashCode());
        Assertions.assertNotEquals(decimalV2Type1, decimalV2Type4);
        Assertions.assertNotEquals(decimalV2Type1.hashCode(), decimalV2Type4.hashCode());

        CharType charType1 = new CharType(10);
        CharType charType2 = new CharType(10);
        CharType charType3 = new CharType(15);
        Assertions.assertEquals(charType1, charType2);
        Assertions.assertEquals(charType1.hashCode(), charType2.hashCode());
        Assertions.assertNotEquals(charType1, charType3);
        Assertions.assertNotEquals(charType1.hashCode(), charType3.hashCode());

        VarcharType varcharType1 = new VarcharType(32);
        VarcharType varcharType2 = new VarcharType(32);
        VarcharType varcharType3 = new VarcharType(64);
        Assertions.assertEquals(varcharType1, varcharType2);
        Assertions.assertEquals(varcharType1.hashCode(), varcharType2.hashCode());
        Assertions.assertNotEquals(varcharType1, varcharType3);
        Assertions.assertNotEquals(varcharType1.hashCode(), varcharType3.hashCode());
    }

    @Test
    void testFromPrimitiveType() {
        Assertions.assertEquals(DataType.fromCatalogType(Type.STRING), StringType.INSTANCE);
    }

    @Test
    void testConvertFromString() {
        // boolean
        Assertions.assertEquals(BooleanType.INSTANCE, DataType.convertFromString("boolean"));
        // tinyint
        Assertions.assertEquals(TinyIntType.INSTANCE, DataType.convertFromString("tinyint"));
        // smallint
        Assertions.assertEquals(SmallIntType.INSTANCE, DataType.convertFromString("smallint"));
        // int
        Assertions.assertEquals(IntegerType.INSTANCE, DataType.convertFromString("int"));
        // bigint
        Assertions.assertEquals(BigIntType.INSTANCE, DataType.convertFromString("bigint"));
        // largeint
        Assertions.assertEquals(LargeIntType.INSTANCE, DataType.convertFromString("largeint"));
        // float
        Assertions.assertEquals(FloatType.INSTANCE, DataType.convertFromString("float"));
        // double
        Assertions.assertEquals(DoubleType.INSTANCE, DataType.convertFromString("double"));
        // decimalv2
        Assertions.assertEquals(DecimalV2Type.createDecimalV2Type(13, 9),
                DataType.convertFromString("decimal(13, 9)"));
        // decimalv3
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(13, 9),
                DataType.convertFromString("decimalv3(13, 9)"));
        // text
        Assertions.assertEquals(StringType.INSTANCE, DataType.convertFromString("text"));
        // string
        Assertions.assertEquals(StringType.INSTANCE, DataType.convertFromString("string"));
        // char
        Assertions.assertEquals(CharType.createCharType(10), DataType.convertFromString("char(10)"));
        Assertions.assertEquals(CharType.SYSTEM_DEFAULT, DataType.convertFromString("character"));
        // varchar
        Assertions.assertEquals(VarcharType.createVarcharType(10), DataType.convertFromString("varchar(10)"));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, DataType.convertFromString("varchar(*)"));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, DataType.convertFromString("varchar"));
        // date
        Assertions.assertEquals(DateType.INSTANCE, DataType.convertFromString("date"));
        // datev2
        Assertions.assertEquals(DateV2Type.INSTANCE, DataType.convertFromString("datev2"));
        // time
        Assertions.assertEquals(TimeType.INSTANCE, DataType.convertFromString("time"));

        // datetime
        Assertions.assertEquals(DateTimeType.INSTANCE, DataType.convertFromString("datetime"));

        // datetimev2
        Assertions.assertEquals(DateTimeV2Type.of(3), DataType.convertFromString("datetimev2(3)"));
        // hll
        Assertions.assertEquals(HllType.INSTANCE, DataType.convertFromString("hll"));
        // bitmap
        Assertions.assertEquals(BitmapType.INSTANCE, DataType.convertFromString("bitmap"));
        // quantile_state
        Assertions.assertEquals(QuantileStateType.INSTANCE, DataType.convertFromString("quantile_state"));
        // json
        Assertions.assertEquals(JsonType.INSTANCE, DataType.convertFromString("json"));
        // array
        Assertions.assertEquals(ArrayType.of(IntegerType.INSTANCE), DataType.convertFromString("array<int>"));
        // map
        Assertions.assertEquals(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), DataType.convertFromString("map<int, int>"));
        // struct
        Assertions.assertEquals(new StructType(ImmutableList.of(new StructField("a", IntegerType.INSTANCE, true, ""))), DataType.convertFromString("struct<a: int>"));

    }

    @Test
    public void testAnyAccept() {
        AnyDataType dateType = AnyDataType.INSTANCE_WITHOUT_INDEX;
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
        Assertions.assertTrue(dateType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dateType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dateType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertTrue(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertTrue(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertTrue(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertFalse(dataType.acceptsType(DecimalV2Type.createDecimalV2Type(precision, scale)));
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
        Assertions.assertEquals("signed", BigIntType.SIGNED.simpleString());
        Assertions.assertEquals("largeint", LargeIntType.INSTANCE.simpleString());
        Assertions.assertEquals("unsigned", LargeIntType.UNSIGNED.simpleString());
        Assertions.assertEquals("fractional", FractionalType.INSTANCE.simpleString());
        Assertions.assertEquals("float", FloatType.INSTANCE.simpleString());
        Assertions.assertEquals("double", DoubleType.INSTANCE.simpleString());
        Assertions.assertEquals("decimal", DecimalV2Type.SYSTEM_DEFAULT.simpleString());
        Assertions.assertEquals("char", new CharType(10).simpleString());
        Assertions.assertEquals("varchar", VarcharType.SYSTEM_DEFAULT.simpleString());
        Assertions.assertEquals("text", StringType.INSTANCE.simpleString());
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
