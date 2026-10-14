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

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.VariantType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaTypeDescTest {

    @Test
    public void testNestedType() {
        ScalarType decimalType = ScalarType.createDecimalV3Type(18, 4);
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("price", decimalType, "", false)));
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(new ArrayType(structType));

        Assertions.assertEquals("ARRAY", typeDesc.getKind());
        Assertions.assertTrue(typeDesc.getContainsNull());
        Assertions.assertEquals("STRUCT", typeDesc.getElement().getKind());
        Assertions.assertEquals(1, typeDesc.getElement().getFields().size());

        SchemaTypeDesc.StructFieldDesc field = typeDesc.getElement().getFields().get(0);
        Assertions.assertEquals("price", field.getName());
        Assertions.assertFalse(field.isContainsNull());
        Assertions.assertEquals("DECIMAL64", field.getType().getKind());
        Assertions.assertEquals(Integer.valueOf(18), field.getType().getPrecision());
        Assertions.assertEquals(Integer.valueOf(4), field.getType().getScale());
    }

    @Test
    public void testMapType() {
        MapType mapType = new MapType(Type.STRING,
                new ArrayType(ScalarType.createDecimalV3Type(9, 2)), false, true);
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(mapType);

        Assertions.assertEquals("MAP", typeDesc.getKind());
        Assertions.assertFalse(typeDesc.getKeyContainsNull());
        Assertions.assertTrue(typeDesc.getValueContainsNull());
        Assertions.assertEquals("STRING", typeDesc.getKey().getKind());
        Assertions.assertEquals("ARRAY", typeDesc.getValue().getKind());
        Assertions.assertEquals(Integer.valueOf(9), typeDesc.getValue().getElement().getPrecision());
        Assertions.assertEquals(Integer.valueOf(2), typeDesc.getValue().getElement().getScale());
    }

    @Test
    public void testDeeplyNestedArrayMapStructType() {
        StructType leafStruct = new StructType(Lists.newArrayList(
                new StructField("id", Type.BIGINT, "", false),
                new StructField("amounts",
                        new ArrayType(ScalarType.createDecimalV3Type(18, 4)), "", true)));
        MapType mapType = new MapType(ScalarType.createVarcharType(16), leafStruct, false, true);
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(new ArrayType(mapType));

        Assertions.assertEquals("ARRAY", typeDesc.getKind());
        Assertions.assertTrue(typeDesc.getContainsNull());

        SchemaTypeDesc mapDesc = typeDesc.getElement();
        Assertions.assertEquals("MAP", mapDesc.getKind());
        Assertions.assertFalse(mapDesc.getKeyContainsNull());
        Assertions.assertTrue(mapDesc.getValueContainsNull());
        Assertions.assertEquals(Integer.valueOf(16), mapDesc.getKey().getLength());

        SchemaTypeDesc structDesc = mapDesc.getValue();
        Assertions.assertEquals("STRUCT", structDesc.getKind());
        Assertions.assertEquals(2, structDesc.getFields().size());
        Assertions.assertFalse(structDesc.getFields().get(0).isContainsNull());
        Assertions.assertEquals("BIGINT", structDesc.getFields().get(0).getType().getKind());
        Assertions.assertTrue(structDesc.getFields().get(1).isContainsNull());

        SchemaTypeDesc amountsDesc = structDesc.getFields().get(1).getType();
        Assertions.assertEquals("ARRAY", amountsDesc.getKind());
        Assertions.assertTrue(amountsDesc.getContainsNull());
        Assertions.assertEquals("DECIMAL64", amountsDesc.getElement().getKind());
        Assertions.assertEquals(Integer.valueOf(18), amountsDesc.getElement().getPrecision());
        Assertions.assertEquals(Integer.valueOf(4), amountsDesc.getElement().getScale());
    }

    @Test
    public void testNestedArraysPreserveElementChain() {
        Type nestedArrays = new ArrayType(new ArrayType(new ArrayType(Type.INT)));
        SchemaTypeDesc outer = SchemaTypeDesc.fromType(nestedArrays);

        Assertions.assertEquals("ARRAY", outer.getKind());
        Assertions.assertEquals("ARRAY", outer.getElement().getKind());
        Assertions.assertEquals("ARRAY", outer.getElement().getElement().getKind());
        Assertions.assertEquals("INT", outer.getElement().getElement().getElement().getKind());
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
        Assertions.assertEquals("events", eventsField.path("name").asText());
        assertBooleanField(eventsField, "contains_null", true);

        JsonNode map = eventsField.path("type").path("element");
        assertBooleanField(map, "key_contains_null", false);
        assertBooleanField(map, "value_contains_null", true);
        Assertions.assertEquals("STRING", map.path("key").path("kind").asText());

        JsonNode createdAt = map.path("value").path("fields").path(0);
        Assertions.assertEquals("created_at", createdAt.path("name").asText());
        assertBooleanField(createdAt, "contains_null", false);
        Assertions.assertEquals("DATETIMEV2", createdAt.path("type").path("kind").asText());
        Assertions.assertEquals(6, createdAt.path("type").path("scale").asInt());
        Assertions.assertFalse(createdAt.path("type").has("fields"));
        Assertions.assertFalse(createdAt.path("type").has("element"));
    }

    @Test
    public void testStructFieldComments() {
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("documented", Type.INT, "unit price", true),
                new StructField("undocumented", Type.INT),
                new StructField("empty_comment", Type.INT, "", true, true)));
        JsonNode fields = new ObjectMapper().valueToTree(
                SchemaTypeDesc.fromType(structType)).path("fields");

        Assertions.assertEquals("unit price", fields.path(0).path("comment").asText());
        Assertions.assertFalse(fields.path(1).has("comment"));
        Assertions.assertTrue(fields.path(2).has("comment"));
        Assertions.assertEquals("", fields.path(2).path("comment").asText());
    }

    @Test
    public void testScalarAttributes() {
        SchemaTypeDesc character = SchemaTypeDesc.fromType(ScalarType.createCharType(16));
        SchemaTypeDesc varchar = SchemaTypeDesc.fromType(ScalarType.createVarcharType(32));
        SchemaTypeDesc varbinary = SchemaTypeDesc.fromType(ScalarType.createVarbinaryType(64));
        SchemaTypeDesc datetime = SchemaTypeDesc.fromType(ScalarType.createDatetimeV2Type(3));
        SchemaTypeDesc time = SchemaTypeDesc.fromType(ScalarType.createTimeV2Type(5));
        SchemaTypeDesc timestamp = SchemaTypeDesc.fromType(ScalarType.createTimeStampTzType(6));

        Assertions.assertEquals(Integer.valueOf(16), character.getLength());
        Assertions.assertEquals(Integer.valueOf(32), varchar.getLength());
        Assertions.assertEquals(Integer.valueOf(64), varbinary.getLength());
        Assertions.assertEquals(Integer.valueOf(3), datetime.getScale());
        Assertions.assertEquals(Integer.valueOf(5), time.getScale());
        Assertions.assertEquals(Integer.valueOf(6), timestamp.getScale());
        Assertions.assertNull(SchemaTypeDesc.fromType(Type.STRING).getLength());
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

        assertBooleanField(json, "contains_null", true);
        Assertions.assertFalse(json.has("containsNull"));
        Assertions.assertFalse(json.has("precision"));
        Assertions.assertEquals("array<decimalv3(18,4)>", json.path("sql").asText());
        Assertions.assertEquals("decimalv3(18,4)", json.path("element").path("sql").asText());
        Assertions.assertEquals(18, json.path("element").path("precision").asInt());
        Assertions.assertEquals(4, json.path("element").path("scale").asInt());
    }

    @Test
    public void testVarbinaryJsonContainsLength() {
        JsonNode json = new ObjectMapper().valueToTree(
                SchemaTypeDesc.fromType(ScalarType.createVarbinaryType(64)));

        Assertions.assertEquals("VARBINARY", json.path("kind").asText());
        Assertions.assertEquals("varbinary(64)", json.path("sql").asText());
        Assertions.assertEquals(64, json.path("length").asInt());
        Assertions.assertEquals(3, json.size());
    }

    @Test
    public void testVariantPredefinedFieldsAreStructuredRecursively() {
        VariantType variantType = new VariantType(Lists.newArrayList(
                new VariantField("amount", ScalarType.createDecimalV3Type(18, 4),
                        "monetary value", PatternType.MATCH_NAME),
                new VariantField("tags_*", new ArrayType(ScalarType.createVarcharType(32)),
                        "", PatternType.MATCH_NAME_GLOB)));
        JsonNode json = new ObjectMapper().valueToTree(SchemaTypeDesc.fromType(variantType));

        JsonNode amount = json.path("predefined_fields").path(0);
        Assertions.assertEquals("amount", amount.path("pattern").asText());
        Assertions.assertEquals("MATCH_NAME", amount.path("pattern_type").asText());
        Assertions.assertEquals("monetary value", amount.path("comment").asText());
        Assertions.assertEquals("DECIMAL64", amount.path("type").path("kind").asText());
        Assertions.assertEquals(18, amount.path("type").path("precision").asInt());
        Assertions.assertEquals(4, amount.path("type").path("scale").asInt());

        JsonNode tags = json.path("predefined_fields").path(1);
        Assertions.assertEquals("tags_*", tags.path("pattern").asText());
        Assertions.assertEquals("MATCH_NAME_GLOB", tags.path("pattern_type").asText());
        Assertions.assertEquals("ARRAY", tags.path("type").path("kind").asText());
        Assertions.assertEquals(32, tags.path("type").path("element").path("length").asInt());
    }

    @Test
    public void testAggStateSubTypesAreStructuredRecursively() {
        AggStateType aggStateType = new AggStateType("weighted_sum", false,
                Lists.newArrayList(Type.INT,
                        new ArrayType(ScalarType.createDecimalV3Type(18, 4))),
                Lists.newArrayList(true, false));
        JsonNode json = new ObjectMapper().valueToTree(SchemaTypeDesc.fromType(aggStateType));

        Assertions.assertEquals("AGG_STATE", json.path("kind").asText());
        Assertions.assertEquals("weighted_sum", json.path("function_name").asText());
        assertBooleanField(json, "result_is_nullable", false);

        JsonNode integer = json.path("sub_types").path(0);
        assertBooleanField(integer, "contains_null", true);
        Assertions.assertEquals("INT", integer.path("type").path("kind").asText());

        JsonNode amounts = json.path("sub_types").path(1);
        assertBooleanField(amounts, "contains_null", false);
        Assertions.assertEquals("ARRAY", amounts.path("type").path("kind").asText());
        Assertions.assertEquals(18, amounts.path("type").path("element").path("precision").asInt());
        Assertions.assertEquals(4, amounts.path("type").path("element").path("scale").asInt());
    }

    @Test
    public void testPrimitiveJsonOnlyContainsKindAndSql() {
        JsonNode json = new ObjectMapper().valueToTree(SchemaTypeDesc.fromType(Type.BIGINT));

        Assertions.assertEquals(2, json.size());
        Assertions.assertEquals("BIGINT", json.path("kind").asText());
        Assertions.assertEquals("bigint", json.path("sql").asText());
    }

    @Test
    public void testUnsupportedTypeOmitsSql() {
        SchemaTypeDesc unsupported = SchemaTypeDesc.fromType(Type.UNSUPPORTED);
        JsonNode json = new ObjectMapper().valueToTree(unsupported);

        Assertions.assertEquals("UNSUPPORTED_TYPE", unsupported.getKind());
        Assertions.assertNull(unsupported.getSql());
        Assertions.assertEquals(1, json.size());
        Assertions.assertEquals("UNSUPPORTED_TYPE", json.path("kind").asText());
        Assertions.assertFalse(json.has("sql"));
    }

    @Test
    public void testComplexTypeContainingUnsupportedTypeOmitsSql() {
        SchemaTypeDesc array = SchemaTypeDesc.fromType(new ArrayType(Type.UNSUPPORTED));
        JsonNode json = new ObjectMapper().valueToTree(array);

        Assertions.assertNull(array.getSql());
        Assertions.assertNull(array.getElement().getSql());
        Assertions.assertFalse(json.has("sql"));
        Assertions.assertFalse(json.path("element").has("sql"));
        Assertions.assertEquals("UNSUPPORTED_TYPE", json.path("element").path("kind").asText());
    }

    private void assertBooleanField(JsonNode node, String fieldName, boolean expected) {
        Assertions.assertTrue(node.has(fieldName), "Missing field: " + fieldName);
        JsonNode value = node.get(fieldName);
        Assertions.assertTrue(value.isBoolean(), "Field is not boolean: " + fieldName);
        Assertions.assertEquals(expected, value.booleanValue(),
                "Unexpected value for field: " + fieldName);
    }

    private void assertDecimalType(ScalarType type, String kind, int precision, int scale) {
        SchemaTypeDesc typeDesc = SchemaTypeDesc.fromType(type);
        Assertions.assertEquals(kind, typeDesc.getKind());
        Assertions.assertEquals(Integer.valueOf(precision), typeDesc.getPrecision());
        Assertions.assertEquals(Integer.valueOf(scale), typeDesc.getScale());
    }
}
