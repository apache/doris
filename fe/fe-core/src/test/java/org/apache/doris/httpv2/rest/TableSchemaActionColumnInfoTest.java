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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.httpv2.rest.response.SchemaTypeDesc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TableSchemaActionColumnInfoTest {

    @Test
    public void testArrayDecimalColumnInfo() {
        Column column = new Column("array_decimal",
                new ArrayType(ScalarType.createDecimalV3Type(18, 4)), false, null, true, null,
                "nested decimal");
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(column);

        Assertions.assertEquals("array_decimal", columnInfo.get("name"));
        Assertions.assertEquals("ARRAY", columnInfo.get("type"));
        Assertions.assertEquals("array<decimalv3(18,4)>", columnInfo.get("type_sql"));
        Assertions.assertEquals("Yes", columnInfo.get("is_nullable"));
        Assertions.assertEquals("No", columnInfo.get("is_key"));
        Assertions.assertEquals("nested decimal", columnInfo.get("comment"));
        Assertions.assertFalse(columnInfo.containsKey("precision"));
        Assertions.assertFalse(columnInfo.containsKey("scale"));

        SchemaTypeDesc typeDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assertions.assertEquals("ARRAY", typeDesc.getKind());
        Assertions.assertTrue(typeDesc.getContainsNull());
        Assertions.assertEquals("DECIMAL64", typeDesc.getElement().getKind());
        Assertions.assertEquals(Integer.valueOf(18), typeDesc.getElement().getPrecision());
        Assertions.assertEquals(Integer.valueOf(4), typeDesc.getElement().getScale());

        JsonNode json = new ObjectMapper().valueToTree(columnInfo);
        Assertions.assertEquals("ARRAY", json.path("type").asText());
        assertBooleanField(json.path("type_desc"), "contains_null", true);
        Assertions.assertEquals(4, json.path("type_desc").path("element").path("scale").asInt());
    }

    @Test
    public void testScalarDecimalKeepsLegacyAttributes() {
        Column column = new Column("amount", ScalarType.createDecimalV3Type(18, 4));
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(column);

        Assertions.assertEquals("DECIMAL64", columnInfo.get("type"));
        Assertions.assertEquals("18", columnInfo.get("precision"));
        Assertions.assertEquals("4", columnInfo.get("scale"));
        SchemaTypeDesc typeDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assertions.assertEquals(Integer.valueOf(18), typeDesc.getPrecision());
        Assertions.assertEquals(Integer.valueOf(4), typeDesc.getScale());
    }

    @Test
    public void testVarbinaryColumnInfoContainsStructuredLength() {
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(
                new Column("payload", ScalarType.createVarbinaryType(64)));

        Assertions.assertEquals("VARBINARY", columnInfo.get("type"));
        Assertions.assertEquals("varbinary(64)", columnInfo.get("type_sql"));

        JsonNode json = new ObjectMapper().valueToTree(columnInfo);
        Assertions.assertEquals("VARBINARY", json.path("type_desc").path("kind").asText());
        Assertions.assertEquals("varbinary(64)", json.path("type_desc").path("sql").asText());
        Assertions.assertEquals(64, json.path("type_desc").path("length").asInt());
    }

    @Test
    public void testVariantAndAggStateColumnInfoContainsStructuredChildren() {
        VariantType variantType = new VariantType(Lists.newArrayList(
                new VariantField("event_id", Type.BIGINT, "event identifier")));
        JsonNode variantJson = new ObjectMapper().valueToTree(TableSchemaAction.buildColumnInfo(
                new Column("payload", variantType)));

        JsonNode variantField = variantJson.path("type_desc").path("predefined_fields").path(0);
        Assertions.assertEquals("event_id", variantField.path("pattern").asText());
        Assertions.assertEquals("BIGINT", variantField.path("type").path("kind").asText());

        AggStateType aggStateType = new AggStateType("sum", true,
                Lists.newArrayList(Type.BIGINT), Lists.newArrayList(false));
        JsonNode aggStateJson = new ObjectMapper().valueToTree(TableSchemaAction.buildColumnInfo(
                new Column("sum_state", aggStateType)));

        JsonNode typeDesc = aggStateJson.path("type_desc");
        Assertions.assertEquals("sum", typeDesc.path("function_name").asText());
        assertBooleanField(typeDesc, "result_is_nullable", true);
        assertBooleanField(typeDesc.path("sub_types").path(0), "contains_null", false);
        Assertions.assertEquals("BIGINT", typeDesc.path("sub_types").path(0).path("type").path("kind").asText());
    }

    @Test
    public void testUnsupportedColumnInfoOmitsSql() {
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(
                new Column("geometry", Type.UNSUPPORTED));

        Assertions.assertEquals("UNSUPPORTED_TYPE", columnInfo.get("type"));
        Assertions.assertFalse(columnInfo.containsKey("type_sql"));

        SchemaTypeDesc typeDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assertions.assertEquals("UNSUPPORTED_TYPE", typeDesc.getKind());
        Assertions.assertNull(typeDesc.getSql());

        JsonNode json = new ObjectMapper().valueToTree(columnInfo);
        Assertions.assertFalse(json.has("type_sql"));
        Assertions.assertEquals("UNSUPPORTED_TYPE", json.path("type_desc").path("kind").asText());
        Assertions.assertFalse(json.path("type_desc").has("sql"));
    }

    @Test
    public void testStructAndMapColumnInfo() {
        MapType mapType = new MapType(Type.STRING,
                new ArrayType(ScalarType.createDecimalV3Type(9, 2)), false, true);
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("attributes", mapType, "map attributes", false),
                new StructField("tags", new ArrayType(Type.STRING))));
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(
                new Column("detail", structType));

        SchemaTypeDesc structDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assertions.assertEquals("STRUCT", structDesc.getKind());
        Assertions.assertEquals(2, structDesc.getFields().size());
        Assertions.assertFalse(structDesc.getFields().get(0).isContainsNull());
        Assertions.assertEquals("map attributes", structDesc.getFields().get(0).getComment());
        Assertions.assertNull(structDesc.getFields().get(1).getComment());

        SchemaTypeDesc mapDesc = structDesc.getFields().get(0).getType();
        Assertions.assertEquals("MAP", mapDesc.getKind());
        Assertions.assertFalse(mapDesc.getKeyContainsNull());
        Assertions.assertTrue(mapDesc.getValueContainsNull());
        Assertions.assertEquals("STRING", mapDesc.getKey().getKind());
        Assertions.assertEquals("DECIMAL32", mapDesc.getValue().getElement().getKind());
        Assertions.assertEquals(Integer.valueOf(2), mapDesc.getValue().getElement().getScale());
    }

    private void assertBooleanField(JsonNode node, String fieldName, boolean expected) {
        Assertions.assertTrue(node.has(fieldName), "Missing field: " + fieldName);
        JsonNode value = node.get(fieldName);
        Assertions.assertTrue(value.isBoolean(), "Field is not boolean: " + fieldName);
        Assertions.assertEquals(expected, value.booleanValue(),
                "Unexpected value for field: " + fieldName);
    }
}
