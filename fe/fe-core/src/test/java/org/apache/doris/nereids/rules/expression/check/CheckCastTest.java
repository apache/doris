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

package org.apache.doris.nereids.rules.expression.check;

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
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CheckCastTest {
    @Test
    public void testCastFromBoolean() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BooleanType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BooleanType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromTinyInt() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TinyIntType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TinyIntType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromSmallInt() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(SmallIntType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(SmallIntType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromInteger() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IntegerType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IntegerType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromBigInt() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BigIntType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BigIntType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromLargeInt() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(LargeIntType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(LargeIntType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromFloat() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(FloatType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(FloatType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromDouble() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DoubleType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DoubleType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromDecimal() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DecimalV3Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromDate() {
        // Strict mode
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, QuantileStateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateType.INSTANCE, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateV2Type.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateV2Type.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromDateTime() {
        // Strict mode
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, QuantileStateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeType.INSTANCE, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(DateTimeV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromTime() {
        // Strict mode
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(TimeV2Type.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(TimeV2Type.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromIPv4() {
        // Strict mode
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv4Type.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv4Type.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromIPv6() {
        // Strict mode
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(IPv6Type.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(IPv6Type.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromChar() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(HllType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(QuantileStateType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), true));
        List<StructField> fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", HllType.INSTANCE, false, ""));
        StructType structType1 = new StructType(fields1);
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, structType1, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(CharType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(HllType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, ArrayType.of(QuantileStateType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), false));
        Assertions.assertFalse(CheckCast.check(CharType.SYSTEM_DEFAULT, structType1, false));
    }

    @Test
    public void testCastFromVarchar() {
        // Strict mode
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(HllType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(QuantileStateType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), true));
        List<StructField> fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", HllType.INSTANCE, false, ""));
        StructType structType1 = new StructType(fields1);
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, structType1, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(VarcharType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(HllType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, ArrayType.of(QuantileStateType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), false));
        Assertions.assertFalse(CheckCast.check(VarcharType.SYSTEM_DEFAULT, structType1, false));
    }

    @Test
    public void testCastFromString() {
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, QuantileStateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(HllType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), true));
        List<StructField> fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", HllType.INSTANCE, false, ""));
        StructType structType1 = new StructType(fields1);
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, structType1, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StringType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(HllType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, MapType.of(IntegerType.INSTANCE, ArrayType.of(QuantileStateType.INSTANCE)), false));
        Assertions.assertFalse(CheckCast.check(StringType.INSTANCE, structType1, false));
    }

    @Test
    public void testCastFromJson() {
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(JsonType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(JsonType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromHll() {
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(HllType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(HllType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(HllType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromBitmap() {
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(BitmapType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(BitmapType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(BitmapType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromQuantile() {
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BitmapType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(QuantileStateType.INSTANCE, BitmapType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, false));
    }

    @Test
    public void testCastFromArray() {
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(ArrayType.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(ArrayType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(ArrayType.of(IntegerType.INSTANCE), ArrayType.of(StringType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(ArrayType.of(IntegerType.INSTANCE), ArrayType.of(IPv6Type.INSTANCE), true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(ArrayType.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(ArrayType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(ArrayType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(ArrayType.of(IntegerType.INSTANCE), ArrayType.of(StringType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(ArrayType.of(IntegerType.INSTANCE), ArrayType.of(IPv6Type.INSTANCE), false));
    }

    @Test
    public void testCastFromMap() {
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(MapType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(IPv6Type.INSTANCE, StringType.INSTANCE), true));
        Assertions.assertFalse(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(StringType.INSTANCE, IPv6Type.INSTANCE), true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(MapType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(MapType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(IPv6Type.INSTANCE, StringType.INSTANCE), false));
        Assertions.assertFalse(CheckCast.check(MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE), MapType.of(StringType.INSTANCE, IPv6Type.INSTANCE), false));
    }

    @Test
    public void testCastFromStruct() {
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BooleanType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IntegerType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BigIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, FloatType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DoubleType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, TimeV2Type.MAX, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, StringType.INSTANCE, true));
        Assertions.assertTrue(CheckCast.check(StructType.SYSTEM_DEFAULT, JsonType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, VariantType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, true));
        Assertions.assertTrue(CheckCast.check(StructType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, HllType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BitmapType.INSTANCE, true));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, true));
        List<StructField> fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", IntegerType.INSTANCE, false, ""));
        List<StructField> fields2 = Lists.newArrayList();
        fields2.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        StructType structType1 = new StructType(fields1);
        StructType structType2 = new StructType(fields2);
        Assertions.assertFalse(CheckCast.check(structType1, structType2, true));
        List<StructField> fields3 = Lists.newArrayList();
        fields3.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields3.add(new StructField("2", IPv6Type.INSTANCE, false, ""));
        StructType structType3 = new StructType(fields3);
        Assertions.assertFalse(CheckCast.check(structType1, structType3, true));
        List<StructField> fields4 = Lists.newArrayList();
        fields4.add(new StructField("1", StringType.INSTANCE, false, ""));
        fields4.add(new StructField("2", BigIntType.INSTANCE, false, ""));
        StructType structType4 = new StructType(fields4);
        Assertions.assertTrue(CheckCast.check(structType1, structType4, true));

        // Un-strict mode
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BooleanType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, TinyIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, SmallIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IntegerType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BigIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, LargeIntType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, FloatType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DoubleType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateV2Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateTimeType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, DateTimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, TimeV2Type.MAX, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IPv4Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, IPv6Type.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, CharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, StringType.INSTANCE, false));
        Assertions.assertTrue(CheckCast.check(StructType.SYSTEM_DEFAULT, JsonType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, VariantType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, ArrayType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, MapType.SYSTEM_DEFAULT, false));
        Assertions.assertTrue(CheckCast.check(StructType.SYSTEM_DEFAULT, StructType.SYSTEM_DEFAULT, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, HllType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, BitmapType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(StructType.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, false));
        Assertions.assertFalse(CheckCast.check(structType1, structType2, false));
        Assertions.assertFalse(CheckCast.check(structType1, structType3, false));
        Assertions.assertTrue(CheckCast.check(structType1, structType4, false));
    }

    @Test
    public void testCheckComplexTypeContainsType() {
        Assertions.assertFalse(CheckCast.checkTypeContainsType(IntegerType.INSTANCE, MapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, MapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, MapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, MapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(MapType.of(IntegerType.INSTANCE, StringType.INSTANCE), MapType.class));

        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.of(DateV2Type.INSTANCE), MapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(ArrayType.of(MapType.SYSTEM_DEFAULT), MapType.class));

        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, MapType.class));

        List<StructField> fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", MapType.SYSTEM_DEFAULT, false, ""));
        StructType structType1 = new StructType(fields1);
        Assertions.assertTrue(CheckCast.checkTypeContainsType(structType1, MapType.class));

        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, HllType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, HllType.class));

        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, BitmapType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, BitmapType.class));

        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(ArrayType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(MapType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(StructType.SYSTEM_DEFAULT, QuantileStateType.class));

        Assertions.assertTrue(CheckCast.checkTypeContainsType(ArrayType.of(QuantileStateType.INSTANCE), QuantileStateType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(ArrayType.of(BitmapType.INSTANCE), BitmapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(ArrayType.of(HllType.INSTANCE), HllType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(MapType.of(QuantileStateType.INSTANCE, IntegerType.INSTANCE), QuantileStateType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(MapType.of(IntegerType.INSTANCE, BitmapType.INSTANCE), BitmapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(MapType.of(HllType.INSTANCE, IntegerType.INSTANCE), HllType.class));
        fields1 = Lists.newArrayList();
        fields1.add(new StructField("1", IntegerType.INSTANCE, false, ""));
        fields1.add(new StructField("2", HllType.INSTANCE, false, ""));
        structType1 = new StructType(fields1);
        Assertions.assertFalse(CheckCast.checkTypeContainsType(structType1, QuantileStateType.class));
        Assertions.assertFalse(CheckCast.checkTypeContainsType(structType1, BitmapType.class));
        Assertions.assertTrue(CheckCast.checkTypeContainsType(structType1, HllType.class));
    }
}
