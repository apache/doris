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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.httpv2.rest.response.SchemaTypeDesc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TableSchemaActionColumnInfoTest {

    @Test
    public void testArrayDecimalColumnInfo() {
        Column column = new Column("array_decimal",
                new ArrayType(ScalarType.createDecimalV3Type(18, 4)), false, null, true, null,
                "nested decimal");
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(column);

        Assert.assertEquals("array_decimal", columnInfo.get("name"));
        Assert.assertEquals("ARRAY", columnInfo.get("type"));
        Assert.assertEquals("array<decimalv3(18,4)>", columnInfo.get("type_sql"));
        Assert.assertEquals("Yes", columnInfo.get("is_nullable"));
        Assert.assertEquals("No", columnInfo.get("is_key"));
        Assert.assertEquals("nested decimal", columnInfo.get("comment"));
        Assert.assertFalse(columnInfo.containsKey("precision"));
        Assert.assertFalse(columnInfo.containsKey("scale"));

        SchemaTypeDesc typeDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assert.assertEquals("ARRAY", typeDesc.getKind());
        Assert.assertTrue(typeDesc.getContainsNull());
        Assert.assertEquals("DECIMAL64", typeDesc.getElement().getKind());
        Assert.assertEquals(Integer.valueOf(18), typeDesc.getElement().getPrecision());
        Assert.assertEquals(Integer.valueOf(4), typeDesc.getElement().getScale());

        JsonNode json = new ObjectMapper().valueToTree(columnInfo);
        Assert.assertEquals("ARRAY", json.path("type").asText());
        Assert.assertTrue(json.path("type_desc").path("contains_null").asBoolean());
        Assert.assertEquals(4, json.path("type_desc").path("element").path("scale").asInt());
    }

    @Test
    public void testScalarDecimalKeepsLegacyAttributes() {
        Column column = new Column("amount", ScalarType.createDecimalV3Type(18, 4));
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(column);

        Assert.assertEquals("DECIMAL64", columnInfo.get("type"));
        Assert.assertEquals("18", columnInfo.get("precision"));
        Assert.assertEquals("4", columnInfo.get("scale"));
        SchemaTypeDesc typeDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assert.assertEquals(Integer.valueOf(18), typeDesc.getPrecision());
        Assert.assertEquals(Integer.valueOf(4), typeDesc.getScale());
    }

    @Test
    public void testStructAndMapColumnInfo() {
        MapType mapType = new MapType(Type.STRING,
                new ArrayType(ScalarType.createDecimalV3Type(9, 2)), false, true);
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("attributes", mapType, "", false),
                new StructField("tags", new ArrayType(Type.STRING))));
        Map<String, Object> columnInfo = TableSchemaAction.buildColumnInfo(
                new Column("detail", structType));

        SchemaTypeDesc structDesc = (SchemaTypeDesc) columnInfo.get("type_desc");
        Assert.assertEquals("STRUCT", structDesc.getKind());
        Assert.assertEquals(2, structDesc.getFields().size());
        Assert.assertFalse(structDesc.getFields().get(0).isContainsNull());

        SchemaTypeDesc mapDesc = structDesc.getFields().get(0).getType();
        Assert.assertEquals("MAP", mapDesc.getKind());
        Assert.assertFalse(mapDesc.getKeyContainsNull());
        Assert.assertTrue(mapDesc.getValueContainsNull());
        Assert.assertEquals("STRING", mapDesc.getKey().getKind());
        Assert.assertEquals("DECIMAL32", mapDesc.getValue().getElement().getKind());
        Assert.assertEquals(Integer.valueOf(2), mapDesc.getValue().getElement().getScale());
    }
}
