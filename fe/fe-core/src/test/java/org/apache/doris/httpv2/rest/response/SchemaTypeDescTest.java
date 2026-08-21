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

package org.apache.doris.httpv2.rest.response;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class SchemaTypeDescTest {

    @Test
    public void testNestedType() {
        ScalarType decimalType = ScalarType.createDecimalV3Type(18, 4);
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("price", decimalType, "", false)));
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(new ArrayType(structType));

        Assert.assertEquals("ARRAY", typeDesc.getKind());
        Assert.assertTrue(typeDesc.getContainsNull());
        Assert.assertEquals("STRUCT", typeDesc.getElement().getKind());
        Assert.assertEquals(1, typeDesc.getElement().getFields().size());

        SchemaTypeDesc.StructFieldDesc field = typeDesc.getElement().getFields().get(0);
        Assert.assertEquals("price", field.getName());
        Assert.assertFalse(field.isContainsNull());
        Assert.assertEquals("DECIMAL64", field.getType().getKind());
        Assert.assertEquals(Integer.valueOf(18), field.getType().getPrecision());
        Assert.assertEquals(Integer.valueOf(4), field.getType().getScale());
    }

    @Test
    public void testMapType() {
        MapType mapType = new MapType(Type.STRING,
                new ArrayType(ScalarType.createDecimalV3Type(9, 2)), false, true);
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(mapType);

        Assert.assertEquals("MAP", typeDesc.getKind());
        Assert.assertFalse(typeDesc.getKeyContainsNull());
        Assert.assertTrue(typeDesc.getValueContainsNull());
        Assert.assertEquals("STRING", typeDesc.getKey().getKind());
        Assert.assertEquals("ARRAY", typeDesc.getValue().getKind());
        Assert.assertEquals(Integer.valueOf(9), typeDesc.getValue().getElement().getPrecision());
        Assert.assertEquals(Integer.valueOf(2), typeDesc.getValue().getElement().getScale());
    }

    @Test
    public void testDeeplyNestedArrayMapStructType() {
        StructType leafStruct = new StructType(Lists.newArrayList(
                new StructField("id", Type.BIGINT, "", false),
                new StructField("amounts",
                        new ArrayType(ScalarType.createDecimalV3Type(18, 4)), "", true)));
        MapType mapType = new MapType(ScalarType.createVarcharType(16), leafStruct, false, true);
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(new ArrayType(mapType));

        Assert.assertEquals("ARRAY", typeDesc.getKind());
        Assert.assertTrue(typeDesc.getContainsNull());

        SchemaTypeDesc mapDesc = typeDesc.getElement();
        Assert.assertEquals("MAP", mapDesc.getKind());
        Assert.assertFalse(mapDesc.getKeyContainsNull());
        Assert.assertTrue(mapDesc.getValueContainsNull());
        Assert.assertEquals(Integer.valueOf(16), mapDesc.getKey().getLength());

        SchemaTypeDesc structDesc = mapDesc.getValue();
        Assert.assertEquals("STRUCT", structDesc.getKind());
        Assert.assertEquals(2, structDesc.getFields().size());
        Assert.assertFalse(structDesc.getFields().get(0).isContainsNull());
        Assert.assertEquals("BIGINT", structDesc.getFields().get(0).getType().getKind());
        Assert.assertTrue(structDesc.getFields().get(1).isContainsNull());

        SchemaTypeDesc amountsDesc = structDesc.getFields().get(1).getType();
        Assert.assertEquals("ARRAY", amountsDesc.getKind());
        Assert.assertTrue(amountsDesc.getContainsNull());
        Assert.assertEquals("DECIMAL64", amountsDesc.getElement().getKind());
        Assert.assertEquals(Integer.valueOf(18), amountsDesc.getElement().getPrecision());
        Assert.assertEquals(Integer.valueOf(4), amountsDesc.getElement().getScale());
    }

    @Test
    public void testNestedArraysReportNullableElementsAtEveryLevel() {
        Type nestedArrays = new ArrayType(new ArrayType(new ArrayType(Type.INT)));
        SchemaTypeDesc outer = SchemaTypeDesc.fromType(nestedArrays);

        Assert.assertTrue(outer.getContainsNull());
        Assert.assertTrue(outer.getElement().getContainsNull());
        Assert.assertTrue(outer.getElement().getElement().getContainsNull());
        Assert.assertEquals("INT", outer.getElement().getElement().getElement().getKind());
    }

    @Test
    public void testDeeplyNestedJsonSerialization() {
        StructType leafStruct = new StructType(Lists.newArrayList(
                new StructField("created_at", ScalarType.createDatetimeV2Type(6), "", false)));
        Type nestedType = new StructType(Lists.newArrayList(
                new StructField("events",
                        new ArrayType(new MapType(Type.STRING, leafStruct, false, true)),
                        "", true)));
        JsonNode json = new ObjectMapper().valueToTree(SchemaTypeDesc.fromType(nestedType));

        JsonNode eventsField = json.path("fields").path(0);
        Assert.assertEquals("events", eventsField.path("name").asText());
        Assert.assertTrue(eventsField.path("contains_null").asBoolean());

        JsonNode map = eventsField.path("type").path("element");
        Assert.assertFalse(map.path("key_contains_null").asBoolean());
        Assert.assertTrue(map.path("value_contains_null").asBoolean());
        Assert.assertEquals("STRING", map.path("key").path("kind").asText());

        JsonNode createdAt = map.path("value").path("fields").path(0);
        Assert.assertEquals("created_at", createdAt.path("name").asText());
        Assert.assertFalse(createdAt.path("contains_null").asBoolean());
        Assert.assertEquals("DATETIMEV2", createdAt.path("type").path("kind").asText());
        Assert.assertEquals(6, createdAt.path("type").path("scale").asInt());
        Assert.assertFalse(createdAt.path("type").has("fields"));
        Assert.assertFalse(createdAt.path("type").has("element"));
    }

    @Test
    public void testScalarAttributes() {
        SchemaTypeDesc character = SchemaTypeDesc.fromType(ScalarType.createCharType(16));
        SchemaTypeDesc varchar = SchemaTypeDesc.fromType(ScalarType.createVarcharType(32));
        SchemaTypeDesc datetime = SchemaTypeDesc.fromType(ScalarType.createDatetimeV2Type(3));
        SchemaTypeDesc time = SchemaTypeDesc.fromType(ScalarType.createTimeV2Type(5));
        SchemaTypeDesc timestamp = SchemaTypeDesc.fromType(ScalarType.createTimeStampTzType(6));

        Assert.assertEquals(Integer.valueOf(16), character.getLength());
        Assert.assertEquals(Integer.valueOf(32), varchar.getLength());
        Assert.assertEquals(Integer.valueOf(3), datetime.getScale());
        Assert.assertEquals(Integer.valueOf(5), time.getScale());
        Assert.assertEquals(Integer.valueOf(6), timestamp.getScale());
        Assert.assertNull(SchemaTypeDesc.fromType(Type.STRING).getLength());
    }

    @Test
    public void testDecimalStorageKinds() {
        assertDecimalType(ScalarType.createDecimalV3Type(9, 1), "DECIMAL32", 9, 1);
        assertDecimalType(ScalarType.createDecimalV3Type(18, 2), "DECIMAL64", 18, 2);
        assertDecimalType(ScalarType.createDecimalV3Type(38, 3), "DECIMAL128", 38, 3);
    }

    @Test
    public void testJsonUsesSnakeCaseAndOmitsNullFields() {
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(
                new ArrayType(ScalarType.createDecimalV3Type(18, 4)));
        JsonNode json = new ObjectMapper().valueToTree(typeDesc);

        Assert.assertTrue(json.path("contains_null").asBoolean());
        Assert.assertFalse(json.has("containsNull"));
        Assert.assertFalse(json.has("precision"));
        Assert.assertEquals("array<decimalv3(18,4)>", json.path("sql").asText());
        Assert.assertEquals("decimalv3(18,4)", json.path("element").path("sql").asText());
        Assert.assertEquals(18, json.path("element").path("precision").asInt());
        Assert.assertEquals(4, json.path("element").path("scale").asInt());
    }

    @Test
    public void testPrimitiveJsonOnlyContainsKindAndSql() {
        JsonNode json = new ObjectMapper().valueToTree(SchemaTypeDesc.fromType(Type.BIGINT));

        Assert.assertEquals(2, json.size());
        Assert.assertEquals("BIGINT", json.path("kind").asText());
        Assert.assertEquals("bigint", json.path("sql").asText());
    }

    private void assertDecimalType(ScalarType type, String kind, int precision, int scale) {
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(type);
        Assert.assertEquals(kind, typeDesc.getKind());
        Assert.assertEquals(Integer.valueOf(precision), typeDesc.getPrecision());
        Assert.assertEquals(Integer.valueOf(scale), typeDesc.getScale());
    }
}
