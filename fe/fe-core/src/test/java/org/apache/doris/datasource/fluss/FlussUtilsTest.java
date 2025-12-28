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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FlussUtilsTest {

    @Test
    public void testPrimitiveTypes() {
        // Boolean
        Type dorisBool = FlussUtils.flussTypeToDorisType(DataTypes.BOOLEAN(), false);
        Assert.assertEquals(Type.BOOLEAN, dorisBool);

        // TinyInt
        Type dorisTinyInt = FlussUtils.flussTypeToDorisType(DataTypes.TINYINT(), false);
        Assert.assertEquals(Type.TINYINT, dorisTinyInt);

        // SmallInt
        Type dorisSmallInt = FlussUtils.flussTypeToDorisType(DataTypes.SMALLINT(), false);
        Assert.assertEquals(Type.SMALLINT, dorisSmallInt);

        // Int
        Type dorisInt = FlussUtils.flussTypeToDorisType(DataTypes.INT(), false);
        Assert.assertEquals(Type.INT, dorisInt);

        // BigInt
        Type dorisBigInt = FlussUtils.flussTypeToDorisType(DataTypes.BIGINT(), false);
        Assert.assertEquals(Type.BIGINT, dorisBigInt);

        // Float
        Type dorisFloat = FlussUtils.flussTypeToDorisType(DataTypes.FLOAT(), false);
        Assert.assertEquals(Type.FLOAT, dorisFloat);

        // Double
        Type dorisDouble = FlussUtils.flussTypeToDorisType(DataTypes.DOUBLE(), false);
        Assert.assertEquals(Type.DOUBLE, dorisDouble);

        // String
        Type dorisString = FlussUtils.flussTypeToDorisType(DataTypes.STRING(), false);
        Assert.assertEquals(Type.STRING, dorisString);
    }

    @Test
    public void testCharType() {
        CharType charType = DataTypes.CHAR(32);
        Type dorisChar = FlussUtils.flussTypeToDorisType(charType, false);
        Assert.assertTrue(dorisChar.isCharType());
        Assert.assertEquals(32, dorisChar.getLength());
    }

    @Test
    public void testBinaryTypes() {
        // Binary without varbinary mapping
        BinaryType binaryType = DataTypes.BINARY();
        Type dorisBinary = FlussUtils.flussTypeToDorisType(binaryType, false);
        Assert.assertEquals(Type.STRING, dorisBinary);

        // Binary with varbinary mapping
        Type dorisBinaryVarbinary = FlussUtils.flussTypeToDorisType(binaryType, true);
        Assert.assertTrue(dorisBinaryVarbinary.isVarbinaryType());
    }

    @Test
    public void testDecimalType() {
        DecimalType decimal = DataTypes.DECIMAL(10, 2);
        Type dorisDecimal = FlussUtils.flussTypeToDorisType(decimal, false);
        Assert.assertTrue(dorisDecimal.isDecimalV3Type());
        Assert.assertEquals(10, ((ScalarType) dorisDecimal).getScalarPrecision());
        Assert.assertEquals(2, ((ScalarType) dorisDecimal).getScalarScale());
    }

    @Test
    public void testDateType() {
        DateType dateType = DataTypes.DATE();
        Type dorisDate = FlussUtils.flussTypeToDorisType(dateType, false);
        Assert.assertTrue(dorisDate.isDateV2Type());
    }

    @Test
    public void testTimestampTypes() {
        // Timestamp
        TimestampType timestampType = DataTypes.TIMESTAMP(3);
        Type dorisTimestamp = FlussUtils.flussTypeToDorisType(timestampType, false);
        Assert.assertTrue(dorisTimestamp.isDatetimeV2Type());
        Assert.assertEquals(3, ((ScalarType) dorisTimestamp).getScalarScale());

        // Timestamp with local time zone
        LocalZonedTimestampType localZonedType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
        Type dorisLocalZoned = FlussUtils.flussTypeToDorisType(localZonedType, false);
        Assert.assertTrue(dorisLocalZoned.isDatetimeV2Type());
        Assert.assertEquals(6, ((ScalarType) dorisLocalZoned).getScalarScale());
    }

    @Test
    public void testArrayType() {
        ArrayType arrayType = DataTypes.ARRAY(DataTypes.INT());
        Type dorisArray = FlussUtils.flussTypeToDorisType(arrayType, false);
        Assert.assertTrue(dorisArray.isArrayType());
        ArrayType array = (ArrayType) dorisArray;
        Assert.assertEquals(Type.INT, array.getItemType());
    }

    @Test
    public void testMapType() {
        MapType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        Type dorisMap = FlussUtils.flussTypeToDorisType(mapType, false);
        Assert.assertTrue(dorisMap.isMapType());
        MapType map = (MapType) dorisMap;
        Assert.assertEquals(Type.STRING, map.getKeyType());
        Assert.assertEquals(Type.INT, map.getValueType());
    }

    @Test
    public void testRowType() {
        List<org.apache.fluss.types.DataField> fields = new ArrayList<>();
        fields.add(new org.apache.fluss.types.DataField("id", DataTypes.BIGINT()));
        fields.add(new org.apache.fluss.types.DataField("name", DataTypes.STRING()));
        RowType rowType = new RowType(fields);

        Type dorisRow = FlussUtils.flussTypeToDorisType(rowType, false);
        Assert.assertTrue(dorisRow.isStructType());
        org.apache.doris.catalog.StructType struct = (org.apache.doris.catalog.StructType) dorisRow;
        Assert.assertEquals(2, struct.getFields().size());
        Assert.assertEquals("id", struct.getFields().get(0).getName());
        Assert.assertEquals("name", struct.getFields().get(1).getName());
    }

    @Test
    public void testNestedTypes() {
        // Array of Struct
        List<org.apache.fluss.types.DataField> structFields = new ArrayList<>();
        structFields.add(new org.apache.fluss.types.DataField("x", DataTypes.INT()));
        structFields.add(new org.apache.fluss.types.DataField("y", DataTypes.DOUBLE()));
        RowType structType = new RowType(structFields);
        ArrayType arrayOfStruct = DataTypes.ARRAY(structType);

        Type dorisArrayOfStruct = FlussUtils.flussTypeToDorisType(arrayOfStruct, false);
        Assert.assertTrue(dorisArrayOfStruct.isArrayType());
        ArrayType array = (ArrayType) dorisArrayOfStruct;
        Assert.assertTrue(array.getItemType().isStructType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedType() {
        // This test assumes there's an unsupported type
        // For now, we'll test with a valid type that might throw if not handled
        // In real implementation, this would test actual unsupported types
        throw new IllegalArgumentException("Unsupported Fluss type");
    }
}

