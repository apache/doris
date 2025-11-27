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
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV3Type.createDecimalV3Type(23, 3),
                DecimalV3Type.createDecimalV3Type(23, 3));
        testProcessComparisonPredicate(NullType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 30),
                DecimalV3Type.createDecimalV3Type(38, 30));
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
        testProcessComparisonPredicate(NullType.INSTANCE, VarcharType.createVarcharType(5),
                VarcharType.createVarcharType(5));
        testProcessComparisonPredicate(NullType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, ArrayType.of(StringType.INSTANCE),
                ArrayType.of(StringType.INSTANCE));
        testProcessComparisonPredicate(NullType.INSTANCE, ArrayType.of(ArrayType.of(StringType.INSTANCE)),
                ArrayType.of(ArrayType.of(StringType.INSTANCE)));
        testProcessComparisonPredicate(NullType.INSTANCE, MapType.of(StringType.INSTANCE, StringType.INSTANCE),
                MapType.of(StringType.INSTANCE, StringType.INSTANCE));
        testProcessComparisonPredicate(NullType.INSTANCE,
                new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))),
                new StructType(ImmutableList.of(new StructField("c1", StringType.INSTANCE, true, ""))));
        testProcessComparisonPredicate(NullType.INSTANCE, VariantType.INSTANCE, VariantType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, HllType.INSTANCE, HllType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, BitmapType.INSTANCE, BitmapType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE, QuantileStateType.INSTANCE, QuantileStateType.INSTANCE);
        testProcessComparisonPredicate(NullType.INSTANCE,
                new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true)),
                new AggStateType("sum", ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true)));
    }

    private void testProcessComparisonPredicate(DataType leftType, DataType rightType, DataType commonType) {
        // SlotReference c1 = new SlotReference("c1", leftType);
        // SlotReference c2 = new SlotReference("c2", rightType);
        // EqualTo equalTo = new EqualTo(c1, c2);
        // GreaterThan greaterThan = new GreaterThan(c1, c2);
        // equalTo = (EqualTo) TypeCoercionUtils.processComparisonPredicate(equalTo);
        // greaterThan = (GreaterThan) TypeCoercionUtils.processComparisonPredicate(greaterThan);
        Optional<DataType> result = TypeCoercionUtils.findWiderTypeForTwoForCaseWhen(leftType, rightType);
        Assertions.assertEquals(Optional.ofNullable(commonType), result);
    }
}
