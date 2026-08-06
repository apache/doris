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

import org.apache.doris.nereids.types.AggStateType;
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
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.qe.VariableMgr;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class TypeCoercionMatrixTest {
    @Test
    public void testProcessComparisonPredicateForNullType() {
        testProcessComparisonPredicate(NullType.INSTANCE, NullType.INSTANCE, NullType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV2Type.CATALOG_DEFAULT);
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(23, 3));
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(38, 30));
        testProcessComparisonPredicate(NullType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DateType.INSTANCE, DateType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DateTimeType.INSTANCE, DateTimeType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(NullType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(NullType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(NullType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(NullType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(NullType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(NullType.INSTANCE, IPv4Type.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, JsonType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, CharType.createCharType(5), CharType.createCharType(5));
        testProcessComparisonPredicate(NullType.INSTANCE, VarcharType.createVarcharType(5), VarcharType.createVarcharType(5));
        testProcessComparisonPredicate(NullType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(NullType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(NullType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        testProcessComparisonPredicate(NullType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(NullType.INSTANCE, VariantType.INSTANCE, VariantType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, QuantileStateType.INSTANCE, QuantileStateType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true));
    }

    @Test
    public void testProcessComparisonPredicateForBooleanType() {
        testProcessComparisonPredicate(BooleanType.INSTANCE, NullType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(9, 0));
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV2Type.createDecimalV2Type(2, 2), DecimalV3Type.createDecimalV3Type(3, 2));
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV3Type.createDecimalV3Type(1, 1), DecimalV3Type.createDecimalV3Type(2, 1));
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(38, 30));
        testProcessComparisonPredicate(BooleanType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, 37));
        testProcessComparisonPredicate(BooleanType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, DateTimeV2Type.of(0), null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, TimeV2Type.of(0), null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, JsonType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, CharType.createCharType(5), BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, VarcharType.createVarcharType(5), BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, StringType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, VariantType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(BooleanType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(BooleanType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForTinyIntType() {
        testProcessComparisonPredicate(TinyIntType.INSTANCE, NullType.INSTANCE, TinyIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, BooleanType.INSTANCE, TinyIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, TinyIntType.INSTANCE, TinyIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(9, 0));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(2, 1), DecimalV3Type.createDecimalV3Type(4, 1));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(3, 3), DecimalV3Type.createDecimalV3Type(6, 3));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 37), DecimalV3Type.createDecimalV3Type(38, 35));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, 35));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(TinyIntType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(TinyIntType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForSmallIntType() {
        testProcessComparisonPredicate(SmallIntType.INSTANCE, NullType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, BooleanType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, TinyIntType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, SmallIntType.INSTANCE, SmallIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(9, 0));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(2, 1), DecimalV3Type.createDecimalV3Type(6, 1));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(3, 3), DecimalV3Type.createDecimalV3Type(8, 3));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 37), DecimalV3Type.createDecimalV3Type(38, 33));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, 33));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(SmallIntType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(SmallIntType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForIntegerType() {
        testProcessComparisonPredicate(IntegerType.INSTANCE, NullType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, BooleanType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, TinyIntType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, SmallIntType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(10, 0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(2, 1), DecimalV3Type.createDecimalV3Type(11, 1));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(3, 3), DecimalV3Type.createDecimalV3Type(13, 3));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 37), DecimalV3Type.createDecimalV3Type(38, 28));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, 28));
        testProcessComparisonPredicate(IntegerType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(IntegerType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(IntegerType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(IntegerType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(IntegerType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(IntegerType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(IntegerType.INSTANCE, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(IntegerType.INSTANCE, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(IntegerType.INSTANCE, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(IntegerType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(IntegerType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(IntegerType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForBigIntType() {
        testProcessComparisonPredicate(BigIntType.INSTANCE, NullType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, BooleanType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, TinyIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, SmallIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, IntegerType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(29, 9));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(20, 0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(2, 1), DecimalV3Type.createDecimalV3Type(21, 1));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(3, 3), DecimalV3Type.createDecimalV3Type(23, 3));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 37), DecimalV3Type.createDecimalV3Type(38, 18));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, 18));
        testProcessComparisonPredicate(BigIntType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(BigIntType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(BigIntType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(BigIntType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(BigIntType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(BigIntType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(BigIntType.INSTANCE, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(BigIntType.INSTANCE, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(BigIntType.INSTANCE, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(BigIntType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(BigIntType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(BigIntType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForLargeIntType() {
        testProcessComparisonPredicate(LargeIntType.INSTANCE, NullType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, BooleanType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, TinyIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, SmallIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, IntegerType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, BigIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, LargeIntType.INSTANCE, LargeIntType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(38, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(38, 0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(2, 1), DecimalV3Type.createDecimalV3Type(38, 1));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(3, 3), DecimalV3Type.createDecimalV3Type(38, 3));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 37), DecimalV3Type.createDecimalV3Type(38, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 38), DecimalV3Type.createDecimalV3Type(38, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION,
                VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(LargeIntType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(LargeIntType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDecimalV2Type() {
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, NullType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, BooleanType.INSTANCE, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(27, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(29, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DecimalV2Type.CATALOG_DEFAULT, DecimalV2Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(29, 9));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(38, 20));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateV2Type.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.of(0), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.of(3), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, TimeV2Type.of(0), TimeV2Type.of(0));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, TimeV2Type.of(3), TimeV2Type.of(3));
        testProcessComparisonPredicate(DecimalV2Type.CATALOG_DEFAULT, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, HllType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV2Type.SYSTEM_DEFAULT, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDecimalV3Type() {
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), NullType.INSTANCE, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), BooleanType.INSTANCE, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(35, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(33, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(30, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(35, 15));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(38, 23));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateV2Type.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateTimeType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateTimeV2Type.of(0), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateTimeV2Type.of(3), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), TimeV2Type.of(0), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), TimeV2Type.of(3), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), TimeV2Type.of(0), TimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), TimeV2Type.of(3), TimeV2Type.of(4));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 4), TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), CharType.createCharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), VarcharType.createVarcharType(5), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), VariantType.INSTANCE, DecimalV3Type.SYSTEM_DEFAULT);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), HllType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DecimalV3Type.createDecimalV3Type(30, 15), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForFloatType() {
        testProcessComparisonPredicate(FloatType.INSTANCE, NullType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, BooleanType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, TinyIntType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, SmallIntType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, IntegerType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, LargeIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, FloatType.INSTANCE, FloatType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(FloatType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(FloatType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(FloatType.INSTANCE, JsonType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, CharType.createCharType(5), DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, VarcharType.createVarcharType(5), DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, StringType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(FloatType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(FloatType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(FloatType.INSTANCE, VariantType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(FloatType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(FloatType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(FloatType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(FloatType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDoubleType() {
        testProcessComparisonPredicate(DoubleType.INSTANCE, NullType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, BooleanType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, TinyIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, SmallIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, IntegerType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, LargeIntType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, TimeV2Type.of(0), TimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, TimeV2Type.of(3), TimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(DoubleType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, JsonType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, CharType.createCharType(5), DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, VarcharType.createVarcharType(5), DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, StringType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, VariantType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(DoubleType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DoubleType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDateV1Type() {
        testProcessComparisonPredicate(DateType.INSTANCE, NullType.INSTANCE, DateType.INSTANCE);
        testProcessComparisonPredicate(DateType.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, TinyIntType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateType.INSTANCE, SmallIntType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateType.INSTANCE, IntegerType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, BigIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, LargeIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, FloatType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, DoubleType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, DateType.INSTANCE, DateType.INSTANCE);
        testProcessComparisonPredicate(DateType.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, TimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateType.INSTANCE, TimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateType.INSTANCE, TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, CharType.createCharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, VarcharType.createVarcharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, StringType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DateType.INSTANCE, VariantType.INSTANCE, DateTimeV2Type.MAX);
        testProcessComparisonPredicate(DateType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DateType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDateV2Type() {
        testProcessComparisonPredicate(DateV2Type.INSTANCE, NullType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, TinyIntType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, SmallIntType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, IntegerType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, BigIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, LargeIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, FloatType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DoubleType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateType.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateV2Type.INSTANCE, DateV2Type.INSTANCE);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, TimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, TimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, CharType.createCharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, VarcharType.createVarcharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, StringType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateV2Type.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, VariantType.INSTANCE, DateTimeV2Type.MAX);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DateV2Type.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDateTimeType() {
        testProcessComparisonPredicate(DateTimeType.INSTANCE, NullType.INSTANCE, DateTimeType.INSTANCE);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, TinyIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, SmallIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, IntegerType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, BigIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, LargeIntType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, FloatType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DoubleType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateTimeType.INSTANCE, DateTimeType.INSTANCE);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, TimeV2Type.of(0), DateTimeV2Type.of(0));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, TimeV2Type.of(3), DateTimeV2Type.of(3));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, CharType.createCharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, VarcharType.createVarcharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, StringType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, VariantType.INSTANCE, DateTimeV2Type.MAX);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForDateTimeV2Type() {
        testProcessComparisonPredicate(DateTimeV2Type.of(4), NullType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), TinyIntType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), SmallIntType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), IntegerType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), BigIntType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), LargeIntType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DecimalV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DecimalV2Type.CATALOG_DEFAULT, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DecimalV3Type.createDecimalV3Type(23, 3), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DecimalV3Type.createDecimalV3Type(38, 30), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), FloatType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DoubleType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateV2Type.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateTimeType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateTimeV2Type.of(0), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateTimeV2Type.of(3), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), TimeV2Type.of(0), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), TimeV2Type.of(3), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), JsonType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), CharType.createCharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), VarcharType.createVarcharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), StringType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(DateTimeV2Type.of(4), ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), VariantType.INSTANCE, DateTimeV2Type.MAX);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), HllType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(DateTimeV2Type.of(4), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForTimeType() {
        testProcessComparisonPredicate(TimeV2Type.of(4), NullType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), TinyIntType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), SmallIntType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), IntegerType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), BigIntType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), LargeIntType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DecimalV2Type.SYSTEM_DEFAULT, TimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), DecimalV2Type.CATALOG_DEFAULT, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DecimalV3Type.createDecimalV3Type(23, 3), TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DecimalV3Type.createDecimalV3Type(38, 30), TimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), FloatType.INSTANCE, TimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), DoubleType.INSTANCE, TimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateV2Type.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateTimeType.INSTANCE, DateTimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateTimeV2Type.of(0), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateTimeV2Type.of(3), DateTimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), TimeV2Type.of(0), TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), TimeV2Type.of(3), TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), TimeV2Type.of(6), TimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), JsonType.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), CharType.createCharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), VarcharType.createVarcharType(5), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), StringType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(TimeV2Type.of(4), ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(TimeV2Type.of(4), MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(TimeV2Type.of(4), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(TimeV2Type.of(4), VariantType.INSTANCE, TimeV2Type.of(4));
        testProcessComparisonPredicate(TimeV2Type.of(4), HllType.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(TimeV2Type.of(4), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForIPv4Type() {
        testProcessComparisonPredicate(IPv4Type.INSTANCE, NullType.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, IPv4Type.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, CharType.createCharType(5), IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, VarcharType.createVarcharType(5), IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, StringType.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, VariantType.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(IPv4Type.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForIPv6Type() {
        testProcessComparisonPredicate(IPv6Type.INSTANCE, NullType.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, IPv4Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, CharType.createCharType(5), IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, VarcharType.createVarcharType(5), IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, StringType.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, VariantType.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(IPv6Type.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForJsonType() {
        testProcessComparisonPredicate(JsonType.INSTANCE, NullType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(JsonType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(JsonType.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(JsonType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, CharType.createCharType(5), JsonType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, VarcharType.createVarcharType(5), JsonType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, StringType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(JsonType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(JsonType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(JsonType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(JsonType.INSTANCE, VariantType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(JsonType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(JsonType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForCharType() {
        testProcessComparisonPredicate(CharType.createCharType(3), NullType.INSTANCE, CharType.createCharType(3));
        testProcessComparisonPredicate(CharType.createCharType(3), BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(CharType.createCharType(3), FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), TimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), TimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(CharType.createCharType(3), IPv4Type.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), JsonType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), CharType.createCharType(3), CharType.createCharType(3));
        testProcessComparisonPredicate(CharType.createCharType(3), CharType.createCharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), VarcharType.createVarcharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), StringType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(CharType.createCharType(3), ArrayType.of(ArrayType.of(StringType.INSTANCE)), ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(CharType.createCharType(3), MapType.of(StringType.INSTANCE, StringType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        testProcessComparisonPredicate(CharType.createCharType(3), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(CharType.createCharType(3), VariantType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(CharType.createCharType(3), HllType.INSTANCE, null);
        testProcessComparisonPredicate(CharType.createCharType(3), BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(CharType.createCharType(3), QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(CharType.createCharType(3), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForVarcharType() {
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), NullType.INSTANCE, VarcharType.createVarcharType(3));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), TimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), TimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), IPv4Type.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), JsonType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), CharType.createCharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), VarcharType.createVarcharType(3), VarcharType.createVarcharType(3));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), VarcharType.createVarcharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), StringType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), ArrayType.of(ArrayType.of(StringType.INSTANCE)), ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), MapType.of(StringType.INSTANCE, StringType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), VariantType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), HllType.INSTANCE, null);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(VarcharType.createVarcharType(3), new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForStringType() {
        testProcessComparisonPredicate(StringType.INSTANCE, NullType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, BooleanType.INSTANCE, BooleanType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, TinyIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, SmallIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, IntegerType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, BigIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, LargeIntType.INSTANCE, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale));
        testProcessComparisonPredicate(StringType.INSTANCE, FloatType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, DoubleType.INSTANCE, DoubleType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, DateType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, DateV2Type.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, DateTimeType.INSTANCE, DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, DateTimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, DateTimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, DateTimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, TimeV2Type.of(0), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, TimeV2Type.of(3), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, TimeV2Type.of(6), DateTimeV2Type.of(6));
        testProcessComparisonPredicate(StringType.INSTANCE, IPv4Type.INSTANCE, IPv4Type.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, IPv6Type.INSTANCE, IPv6Type.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, JsonType.INSTANCE, JsonType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, CharType.createCharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, VarcharType.createVarcharType(5), StringType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, ArrayType.of(StringType.INSTANCE), ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(StringType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(StringType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        testProcessComparisonPredicate(StringType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(StringType.INSTANCE, VariantType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(StringType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(StringType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(StringType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(StringType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForArrayType() {
        ArrayType arrayType = ArrayType.of(IntegerType.INSTANCE);

        testProcessComparisonPredicate(arrayType, NullType.INSTANCE, arrayType);
        testProcessComparisonPredicate(arrayType, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(arrayType, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(arrayType, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(arrayType, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DateType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(arrayType, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(arrayType, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, JsonType.INSTANCE, arrayType);
        testProcessComparisonPredicate(arrayType, CharType.createCharType(5), arrayType);
        testProcessComparisonPredicate(arrayType, VarcharType.createVarcharType(5), arrayType);
        testProcessComparisonPredicate(arrayType, StringType.INSTANCE, arrayType);
        testProcessComparisonPredicate(arrayType, ArrayType.of(StringType.INSTANCE), ArrayType.of(DecimalV3Type.createDecimalV3Type(DecimalV3Type.MAX_DECIMAL128_PRECISION, VariableMgr.getDefaultSessionVariable().decimalOverflowScale)));
        testProcessComparisonPredicate(arrayType, ArrayType.of(ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(arrayType, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(arrayType, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(arrayType, VariantType.INSTANCE, ArrayType.of(DecimalV3Type.SYSTEM_DEFAULT));
        testProcessComparisonPredicate(arrayType, HllType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(arrayType, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForMapType() {
        MapType mapType = MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE);

        testProcessComparisonPredicate(mapType, NullType.INSTANCE, mapType);
        testProcessComparisonPredicate(mapType, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(mapType, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(mapType, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(mapType, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DateType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(mapType, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(mapType, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(mapType, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(mapType, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, CharType.createCharType(5), mapType);
        testProcessComparisonPredicate(mapType, VarcharType.createVarcharType(5), mapType);
        testProcessComparisonPredicate(mapType, StringType.INSTANCE, mapType);
        testProcessComparisonPredicate(mapType, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(mapType, MapType.of(BigIntType.INSTANCE, SmallIntType.INSTANCE), MapType.of(BigIntType.INSTANCE, IntegerType.INSTANCE));
        testProcessComparisonPredicate(mapType, MapType.of(StringType.INSTANCE, ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(mapType, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(mapType, VariantType.INSTANCE, MapType.of(DecimalV3Type.SYSTEM_DEFAULT, DecimalV3Type.SYSTEM_DEFAULT));
        testProcessComparisonPredicate(mapType, HllType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(mapType, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForStructType() {
        StructType structType = new StructType(ImmutableList.of(new StructField("c1", IntegerType.INSTANCE, true, "")));

        testProcessComparisonPredicate(structType, NullType.INSTANCE, structType);
        testProcessComparisonPredicate(structType, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(structType, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(structType, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(structType, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(structType, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(structType, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(structType, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(structType, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(structType, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(structType, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(structType, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(structType, DateType.INSTANCE, null);
        testProcessComparisonPredicate(structType, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(structType, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(structType, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(structType, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(structType, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(structType, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(structType, JsonType.INSTANCE, structType);
        testProcessComparisonPredicate(structType, CharType.createCharType(5), structType);
        testProcessComparisonPredicate(structType, VarcharType.createVarcharType(5), structType);
        testProcessComparisonPredicate(structType, StringType.INSTANCE, structType);
        testProcessComparisonPredicate(structType, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(structType, MapType.of(StringType.INSTANCE, ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(structType, new StructType(ImmutableList.of(new StructField("c1", BigIntType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", BigIntType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(structType, new StructType(ImmutableList.of(new StructField("c2", BigIntType.INSTANCE, true, ""))), new StructType(ImmutableList.of(new StructField("c1", BigIntType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(structType, new StructType(ImmutableList.of(new StructField("c1", BigIntType.INSTANCE, true, ""), new StructField("c2", BigIntType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(structType, VariantType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", DecimalV3Type.SYSTEM_DEFAULT, true, ""))));
        testProcessComparisonPredicate(structType, HllType.INSTANCE, null);
        testProcessComparisonPredicate(structType, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(structType, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(structType, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForHllType() {
        testProcessComparisonPredicate(HllType.INSTANCE, NullType.INSTANCE, HllType.INSTANCE);
        testProcessComparisonPredicate(HllType.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(HllType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(HllType.INSTANCE, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateTimeV2Type.of(0), null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateTimeV2Type.of(3), null);
        testProcessComparisonPredicate(HllType.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(HllType.INSTANCE, TimeV2Type.of(0), null);
        testProcessComparisonPredicate(HllType.INSTANCE, TimeV2Type.of(3), null);
        testProcessComparisonPredicate(HllType.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(HllType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, CharType.createCharType(5), null);
        testProcessComparisonPredicate(HllType.INSTANCE, VarcharType.createVarcharType(5), null);
        testProcessComparisonPredicate(HllType.INSTANCE, StringType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(HllType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(HllType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(HllType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(HllType.INSTANCE, VariantType.INSTANCE, HllType.INSTANCE);
        testProcessComparisonPredicate(HllType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE);
        testProcessComparisonPredicate(HllType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(HllType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForBitmapType() {
        testProcessComparisonPredicate(BitmapType.INSTANCE, NullType.INSTANCE, BitmapType.INSTANCE);
        testProcessComparisonPredicate(BitmapType.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateTimeV2Type.of(0), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateTimeV2Type.of(3), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, TimeV2Type.of(0), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, TimeV2Type.of(3), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, CharType.createCharType(5), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, VarcharType.createVarcharType(5), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, StringType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, VariantType.INSTANCE, BitmapType.INSTANCE);
        testProcessComparisonPredicate(BitmapType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE);
        testProcessComparisonPredicate(BitmapType.INSTANCE, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(BitmapType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForQuantileStateType() {
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, NullType.INSTANCE, QuantileStateType.INSTANCE);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateTimeV2Type.of(0), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateTimeV2Type.of(3), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, TimeV2Type.of(0), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, TimeV2Type.of(3), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, CharType.createCharType(5), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, VarcharType.createVarcharType(5), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, StringType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, VariantType.INSTANCE, QuantileStateType.INSTANCE);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, HllType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, QuantileStateType.INSTANCE, QuantileStateType.INSTANCE);
        testProcessComparisonPredicate(QuantileStateType.INSTANCE, new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    @Test
    public void testProcessComparisonPredicateForAggStateType() {
        AggStateType aggStateType = new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true);
        testProcessComparisonPredicate(aggStateType, NullType.INSTANCE, aggStateType);
        testProcessComparisonPredicate(aggStateType, BooleanType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, TinyIntType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, SmallIntType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, IntegerType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, BigIntType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, LargeIntType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DecimalV2Type.SYSTEM_DEFAULT, null);
        testProcessComparisonPredicate(aggStateType, DecimalV2Type.CATALOG_DEFAULT, null);
        testProcessComparisonPredicate(aggStateType, DecimalV3Type.createDecimalV3Type(23, 3), null);
        testProcessComparisonPredicate(aggStateType, DecimalV3Type.createDecimalV3Type(38, 30), null);
        testProcessComparisonPredicate(aggStateType, FloatType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DoubleType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DateType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DateV2Type.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DateTimeType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, DateTimeV2Type.of(0), null);
        testProcessComparisonPredicate(aggStateType, DateTimeV2Type.of(3), null);
        testProcessComparisonPredicate(aggStateType, DateTimeV2Type.of(6), null);
        testProcessComparisonPredicate(aggStateType, TimeV2Type.of(0), null);
        testProcessComparisonPredicate(aggStateType, TimeV2Type.of(3), null);
        testProcessComparisonPredicate(aggStateType, TimeV2Type.of(6), null);
        testProcessComparisonPredicate(aggStateType, IPv4Type.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, IPv6Type.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, JsonType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, CharType.createCharType(5), null);
        testProcessComparisonPredicate(aggStateType, VarcharType.createVarcharType(5), null);
        testProcessComparisonPredicate(aggStateType, StringType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, ArrayType.of(StringType.INSTANCE), null);
        testProcessComparisonPredicate(aggStateType, ArrayType.of(ArrayType.of(StringType.INSTANCE)), null);
        testProcessComparisonPredicate(aggStateType, MapType.of(StringType.INSTANCE, StringType.INSTANCE), null);
        testProcessComparisonPredicate(aggStateType, new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))), null);
        testProcessComparisonPredicate(aggStateType, VariantType.INSTANCE, aggStateType);
        testProcessComparisonPredicate(aggStateType, HllType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, BitmapType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, QuantileStateType.INSTANCE, null);
        testProcessComparisonPredicate(aggStateType, aggStateType, aggStateType);
        testProcessComparisonPredicate(aggStateType, new AggStateType("avg", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true), null);
    }

    private void testProcessComparisonPredicate(DataType leftType, DataType rightType, DataType commonType) {
        // SlotReference c1 = new SlotReference("c1", leftType);
        // SlotReference c2 = new SlotReference("c2", rightType);
        // EqualTo equalTo = new EqualTo(c1, c2);
        // GreaterThan greaterThan = new GreaterThan(c1, c2);
        // equalTo = (EqualTo) TypeCoercionUtils.processComparisonPredicate(equalTo);
        // greaterThan = (GreaterThan) TypeCoercionUtils.processComparisonPredicate(greaterThan);
        Optional<DataType> result = TypeCoercionUtils.findWiderTypeForTwo(leftType, rightType, false, false);
        Assertions.assertEquals(Optional.ofNullable(commonType), result);
    }
}
