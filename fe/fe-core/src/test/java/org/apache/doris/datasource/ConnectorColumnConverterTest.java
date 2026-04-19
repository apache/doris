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

package org.apache.doris.datasource;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

class ConnectorColumnConverterTest {

    @Test
    void testScalarTypeRoundtrip() {
        // INT → ConnectorType → Doris Type should roundtrip
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(ScalarType.INT);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertEquals(ScalarType.INT, back);
    }

    @Test
    void testArrayTypeRoundtrip() {
        ArrayType arrayInt = ArrayType.create(ScalarType.INT, true);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(arrayInt);

        Assertions.assertEquals("ARRAY", ct.getTypeName());
        Assertions.assertEquals(1, ct.getChildren().size());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof ArrayType);
        Assertions.assertEquals(ScalarType.INT, ((ArrayType) back).getItemType());
    }

    @Test
    void testMapTypeRoundtrip() {
        MapType mapType = new MapType(ScalarType.createStringType(), ScalarType.INT);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(mapType);

        Assertions.assertEquals("MAP", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals("STRING", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("INT", ct.getChildren().get(1).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof MapType);
        Assertions.assertEquals(ScalarType.createStringType(), ((MapType) back).getKeyType());
        Assertions.assertEquals(ScalarType.INT, ((MapType) back).getValueType());
    }

    @Test
    void testStructTypeRoundtrip() {
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(new StructField("a", ScalarType.INT));
        fields.add(new StructField("b", ScalarType.createStringType()));
        StructType structType = new StructType(fields);

        ConnectorType ct = ConnectorColumnConverter.toConnectorType(structType);

        Assertions.assertEquals("STRUCT", ct.getTypeName());
        Assertions.assertEquals(2, ct.getChildren().size());
        Assertions.assertEquals(Arrays.asList("a", "b"), ct.getFieldNames());
        Assertions.assertEquals("INT", ct.getChildren().get(0).getTypeName());
        Assertions.assertEquals("STRING", ct.getChildren().get(1).getTypeName());

        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof StructType);
        StructType backStruct = (StructType) back;
        Assertions.assertEquals(2, backStruct.getFields().size());
        Assertions.assertEquals("a", backStruct.getFields().get(0).getName());
        Assertions.assertEquals(ScalarType.INT, backStruct.getFields().get(0).getType());
    }

    @Test
    void testNestedComplexType() {
        // ARRAY<MAP<STRING, INT>>
        MapType innerMap = new MapType(ScalarType.createStringType(), ScalarType.INT);
        ArrayType nested = ArrayType.create(innerMap, true);

        ConnectorType ct = ConnectorColumnConverter.toConnectorType(nested);

        Assertions.assertEquals("ARRAY", ct.getTypeName());
        ConnectorType mapCt = ct.getChildren().get(0);
        Assertions.assertEquals("MAP", mapCt.getTypeName());
        Assertions.assertEquals("STRING", mapCt.getChildren().get(0).getTypeName());
        Assertions.assertEquals("INT", mapCt.getChildren().get(1).getTypeName());

        // Full roundtrip
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back instanceof ArrayType);
        Type backItem = ((ArrayType) back).getItemType();
        Assertions.assertTrue(backItem instanceof MapType);
    }

    @Test
    void testUnsupportedTypeConversion() {
        ConnectorType ct = ConnectorType.of("UNSUPPORTED", -1, -1);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back.isUnsupported());
    }

    @Test
    void testUnknownTypeDefaultsToUnsupported() {
        ConnectorType ct = ConnectorType.of("GEOMETRY", -1, -1);
        Type back = ConnectorColumnConverter.convertType(ct);
        Assertions.assertTrue(back.isUnsupported());
    }

    @Test
    void testDecimalTypeRoundtrip() {
        ScalarType decimal = ScalarType.createDecimalV3Type(18, 6);
        ConnectorType ct = ConnectorColumnConverter.toConnectorType(decimal);

        // PrimitiveType.toString() returns the specific decimal width (DECIMAL64 for p<=18)
        Assertions.assertTrue(ct.getTypeName().startsWith("DECIMAL"));
        Assertions.assertEquals(18, ct.getPrecision());
        Assertions.assertEquals(6, ct.getScale());
    }
}
