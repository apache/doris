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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DateTimeV2Type;
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
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TypeOfTest {

    @Test
    void testTypeNames() {
        Assertions.assertEquals("unknown", TypeOf.typeName(NullType.INSTANCE));
        Assertions.assertEquals("boolean", TypeOf.typeName(BooleanType.INSTANCE));
        Assertions.assertEquals("tinyint", TypeOf.typeName(TinyIntType.INSTANCE));
        Assertions.assertEquals("smallint", TypeOf.typeName(SmallIntType.INSTANCE));
        Assertions.assertEquals("integer", TypeOf.typeName(IntegerType.INSTANCE));
        Assertions.assertEquals("bigint", TypeOf.typeName(BigIntType.INSTANCE));
        Assertions.assertEquals("decimal(38,0)", TypeOf.typeName(LargeIntType.INSTANCE));
        Assertions.assertEquals("real", TypeOf.typeName(FloatType.INSTANCE));
        Assertions.assertEquals("double", TypeOf.typeName(DoubleType.INSTANCE));
        Assertions.assertEquals("varchar", TypeOf.typeName(StringType.INSTANCE));
        Assertions.assertEquals("varchar(10)", TypeOf.typeName(VarcharType.createVarcharType(10)));
        Assertions.assertEquals("varchar(0)", TypeOf.typeName(VarcharType.createVarcharType(0)));
        Assertions.assertEquals("char(4)", TypeOf.typeName(CharType.createCharType(4)));
        Assertions.assertEquals("date", TypeOf.typeName(DateV2Type.INSTANCE));
        Assertions.assertEquals("timestamp", TypeOf.typeName(DateTimeV2Type.SYSTEM_DEFAULT));
        Assertions.assertEquals("time", TypeOf.typeName(TimeV2Type.SYSTEM_DEFAULT));
        Assertions.assertEquals("decimal(5,1)", TypeOf.typeName(DecimalV3Type.createDecimalV3Type(5, 1)));
        Assertions.assertEquals("decimal(12,3)", TypeOf.typeName(DecimalV2Type.createDecimalV2Type(12, 3)));
        Assertions.assertEquals("timestamp", TypeOf.typeName(TimeStampNsType.INSTANCE));
        Assertions.assertEquals("timestamp with time zone", TypeOf.typeName(TimeStampTzType.SYSTEM_DEFAULT));
        Assertions.assertEquals("ipv4", TypeOf.typeName(IPv4Type.INSTANCE));
        Assertions.assertEquals("ipv6", TypeOf.typeName(IPv6Type.INSTANCE));
        Assertions.assertEquals("varbinary", TypeOf.typeName(VarBinaryType.INSTANCE));
        Assertions.assertEquals("json", TypeOf.typeName(JsonType.INSTANCE));
        Assertions.assertEquals("variant", TypeOf.typeName(VariantType.INSTANCE));
        Assertions.assertEquals("bitmap", TypeOf.typeName(BitmapType.INSTANCE));
        Assertions.assertEquals("hll", TypeOf.typeName(HllType.INSTANCE));
        Assertions.assertEquals("quantile_state", TypeOf.typeName(QuantileStateType.INSTANCE));
        Assertions.assertEquals("array(integer)", TypeOf.typeName(ArrayType.of(IntegerType.INSTANCE)));
        Assertions.assertEquals("map(varchar, bigint)", TypeOf.typeName(
                MapType.of(StringType.INSTANCE, BigIntType.INSTANCE)));
        Assertions.assertEquals("row(\"id\" integer, \"name\" varchar)",
                TypeOf.typeName(new StructType(java.util.Arrays.asList(
                    new StructField("id", IntegerType.INSTANCE, true, ""),
                    new StructField("name", StringType.INSTANCE, true, "")))));
    }

    @Test
    void testNestedTypeNames() {
        StructType row = new StructType(java.util.Arrays.asList(
                new StructField("items", ArrayType.of(IntegerType.INSTANCE), true, ""),
                new StructField("values", MapType.of(StringType.INSTANCE,
                        DecimalV3Type.createDecimalV3Type(12, 3)), true, "")));
        Assertions.assertEquals("array(row(\"items\" array(integer), \"values\" map(varchar, decimal(12,3))))",
                TypeOf.typeName(ArrayType.of(row)));
    }
}
