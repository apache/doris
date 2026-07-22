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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.VarBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class HiveTypeToDorisTypeTest {

    @Test
    public void testBasicTypes() {
        Assertions.assertEquals(Type.BOOLEAN, hiveTypeToDorisType("boolean", 0));
        Assertions.assertEquals(Type.TINYINT, hiveTypeToDorisType("tinyint", 0));
        Assertions.assertEquals(Type.TINYINT, hiveTypeToDorisType("tinyint(3,0)", 0));
        Assertions.assertEquals(Type.SMALLINT, hiveTypeToDorisType("smallint", 0));
        Assertions.assertEquals(Type.INT, hiveTypeToDorisType("int", 0));
        Assertions.assertEquals(Type.INT, hiveTypeToDorisType("int(10,0)", 0));
        Assertions.assertEquals(Type.BIGINT, hiveTypeToDorisType("bigint", 0));
        Assertions.assertEquals(Type.BIGINT, hiveTypeToDorisType("bigint(19,0)", 0));
        Assertions.assertEquals(ScalarType.createDateV2Type(), hiveTypeToDorisType("date", 0));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3), hiveTypeToDorisType("timestamp", 3));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3), hiveTypeToDorisType("timestamp(19)", 3));
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3), hiveTypeToDorisType("datetime", 3));
        Assertions.assertEquals(Type.FLOAT, hiveTypeToDorisType("float", 0));
        Assertions.assertEquals(Type.DOUBLE, hiveTypeToDorisType("double", 0));
        Assertions.assertEquals(ScalarType.createStringType(), hiveTypeToDorisType("string", 0));
        Assertions.assertEquals(ScalarType.createVarbinaryType(VarBinaryType.MAX_VARBINARY_LENGTH),
                hiveTypeToDorisType("binary", 0));
        Assertions.assertEquals(ScalarType.createStringType(),
                HiveMetaStoreClientHelper.hiveTypeToDorisType("binary", 0, false, true));
    }

    @Test
    public void testArrayType() {
        Type expectedType = ArrayType.create(Type.INT, true);
        Assertions.assertEquals(expectedType, hiveTypeToDorisType("array<int>", 0));
    }

    @Test
    public void testMapType() {
        Type expectedType = new MapType(Type.STRING, Type.INT);
        Assertions.assertEquals(expectedType, hiveTypeToDorisType("map<string,int>", 0));
    }

    @Test
    public void testStructType() {
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(new StructField("col1", Type.STRING));
        fields.add(new StructField("col2", Type.INT));
        Type expectedType = new StructType(fields);
        Assertions.assertEquals(expectedType, hiveTypeToDorisType("struct<col1:string,col2:int>", 0));
    }

    @Test
    public void testCharType() {
        Assertions.assertEquals(ScalarType.createType(PrimitiveType.CHAR, 10, 0, 0),
                hiveTypeToDorisType("char(10)", 0));
        Assertions.assertEquals(ScalarType.createType(PrimitiveType.CHAR), hiveTypeToDorisType("char", 0));
    }

    @Test
    public void testVarcharType() {
        Assertions.assertEquals(ScalarType.createType(PrimitiveType.VARCHAR, 20, 0, 0),
                hiveTypeToDorisType("varchar(20)", 0));
        Assertions.assertEquals(ScalarType.createType(PrimitiveType.VARCHAR), hiveTypeToDorisType("varchar", 0));
    }

    @Test
    public void testDecimalType() {
        Assertions.assertEquals(ScalarType.createDecimalV3Type(10, 2),
                hiveTypeToDorisType("decimal(10,2)", 0));
        Assertions.assertEquals(
                ScalarType.createDecimalV3Type(ScalarType.DEFAULT_PRECISION, ScalarType.DEFAULT_SCALE),
                hiveTypeToDorisType("decimal", 0));
    }

    @Test
    public void testUnsupportedType() {
        Assertions.assertEquals(Type.UNSUPPORTED, hiveTypeToDorisType("unsupported_type", 0));
    }

    private Type hiveTypeToDorisType(String hiveType, int timeScale) {
        return HiveMetaStoreClientHelper.hiveTypeToDorisType(hiveType, timeScale, true, true);
    }
}
