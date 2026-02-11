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

package org.apache.doris.datasource.fluss;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.junit.Assert;
import org.junit.Test;

public class FlussUtilsTest {

    @Test
    public void testPrimitiveTypes() {
        Assert.assertEquals(Type.BOOLEAN, FlussUtils.flussTypeToDorisType("BOOLEAN"));
        Assert.assertEquals(Type.BOOLEAN, FlussUtils.flussTypeToDorisType("BOOL"));
        Assert.assertEquals(Type.TINYINT, FlussUtils.flussTypeToDorisType("TINYINT"));
        Assert.assertEquals(Type.TINYINT, FlussUtils.flussTypeToDorisType("INT8"));
        Assert.assertEquals(Type.SMALLINT, FlussUtils.flussTypeToDorisType("SMALLINT"));
        Assert.assertEquals(Type.SMALLINT, FlussUtils.flussTypeToDorisType("INT16"));
        Assert.assertEquals(Type.INT, FlussUtils.flussTypeToDorisType("INT"));
        Assert.assertEquals(Type.INT, FlussUtils.flussTypeToDorisType("INT32"));
        Assert.assertEquals(Type.INT, FlussUtils.flussTypeToDorisType("INTEGER"));
        Assert.assertEquals(Type.BIGINT, FlussUtils.flussTypeToDorisType("BIGINT"));
        Assert.assertEquals(Type.BIGINT, FlussUtils.flussTypeToDorisType("INT64"));
        Assert.assertEquals(Type.FLOAT, FlussUtils.flussTypeToDorisType("FLOAT"));
        Assert.assertEquals(Type.DOUBLE, FlussUtils.flussTypeToDorisType("DOUBLE"));
    }

    @Test
    public void testStringTypes() {
        Type stringType = FlussUtils.flussTypeToDorisType("STRING");
        Assert.assertTrue(stringType.isStringType());

        Type varcharType = FlussUtils.flussTypeToDorisType("VARCHAR(100)");
        Assert.assertTrue(varcharType.isVarchar());
        Assert.assertEquals(100, ((ScalarType) varcharType).getLength());

        Type charType = FlussUtils.flussTypeToDorisType("CHAR(32)");
        Assert.assertTrue(charType.isVarchar());
    }

    @Test
    public void testBinaryTypes() {
        Type binaryType = FlussUtils.flussTypeToDorisType("BINARY");
        Assert.assertTrue(binaryType.isStringType());

        Type bytesType = FlussUtils.flussTypeToDorisType("BYTES");
        Assert.assertTrue(bytesType.isStringType());
    }

    @Test
    public void testDecimalType() {
        Type decimalType = FlussUtils.flussTypeToDorisType("DECIMAL(10,2)");
        Assert.assertTrue(decimalType.isDecimalV3Type());
        Assert.assertEquals(10, ((ScalarType) decimalType).getScalarPrecision());
        Assert.assertEquals(2, ((ScalarType) decimalType).getScalarScale());

        Type defaultDecimal = FlussUtils.flussTypeToDorisType("DECIMAL");
        Assert.assertTrue(defaultDecimal.isDecimalV3Type());
    }

    @Test
    public void testDateTimeTypes() {
        Type dateType = FlussUtils.flussTypeToDorisType("DATE");
        Assert.assertTrue(dateType.isDateV2Type());

        Type timeType = FlussUtils.flussTypeToDorisType("TIME");
        Assert.assertTrue(timeType.isTime());

        Type timestampType = FlussUtils.flussTypeToDorisType("TIMESTAMP");
        Assert.assertTrue(timestampType.isDatetimeV2());

        Type timestampLtzType = FlussUtils.flussTypeToDorisType("TIMESTAMP_LTZ");
        Assert.assertTrue(timestampLtzType.isDatetimeV2());
    }

    @Test
    public void testArrayType() {
        Type arrayType = FlussUtils.flussTypeToDorisType("ARRAY<INT>");
        Assert.assertTrue(arrayType.isArrayType());
        org.apache.doris.catalog.ArrayType array = (org.apache.doris.catalog.ArrayType) arrayType;
        Assert.assertEquals(Type.INT, array.getItemType());

        Type nestedArray = FlussUtils.flussTypeToDorisType("ARRAY<STRING>");
        Assert.assertTrue(nestedArray.isArrayType());
    }

    @Test
    public void testMapType() {
        Type mapType = FlussUtils.flussTypeToDorisType("MAP<STRING, INT>");
        Assert.assertTrue(mapType.isMapType());
        org.apache.doris.catalog.MapType map = (org.apache.doris.catalog.MapType) mapType;
        Assert.assertTrue(map.getKeyType().isStringType());
        Assert.assertEquals(Type.INT, map.getValueType());
    }

    @Test
    public void testUnknownTypeDefaultsToString() {
        Type unknownType = FlussUtils.flussTypeToDorisType("UNKNOWN_TYPE");
        Assert.assertEquals(Type.STRING, unknownType);
    }

    @Test
    public void testNullAndEmptyType() {
        Type nullType = FlussUtils.flussTypeToDorisType(null);
        Assert.assertEquals(Type.STRING, nullType);

        Type emptyType = FlussUtils.flussTypeToDorisType("");
        Assert.assertEquals(Type.STRING, emptyType);
    }

    @Test
    public void testCaseInsensitive() {
        Assert.assertEquals(Type.BOOLEAN, FlussUtils.flussTypeToDorisType("boolean"));
        Assert.assertEquals(Type.INT, FlussUtils.flussTypeToDorisType("int"));
        Assert.assertEquals(Type.BIGINT, FlussUtils.flussTypeToDorisType("bigint"));
    }
}
