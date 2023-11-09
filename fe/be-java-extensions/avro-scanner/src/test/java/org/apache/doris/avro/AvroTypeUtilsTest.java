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

package org.apache.doris.avro;

import org.apache.doris.common.jni.vec.TableSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class AvroTypeUtilsTest {
    private Schema allTypesRecordSchema;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String result;

    @Before
    public void setUp() {
        result = "[{\"name\":\"aBoolean\",\"type\":2,\"childColumns\":null},{\"name\":\"aInt\",\"type\":5,"
                + "\"childColumns\":null},{\"name\":\"aLong\",\"type\":6,\"childColumns\":null},{\"name\":\""
                + "aFloat\",\"type\":7,\"childColumns\":null},{\"name\":\"aDouble\",\"type\":8,\"childColumns\""
                + ":null},{\"name\":\"aString\",\"type\":23,\"childColumns\":null},{\"name\":\"aBytes\",\"type\""
                + ":11,\"childColumns\":null},{\"name\":\"aFixed\",\"type\":11,\"childColumns\":null},{\"name\""
                + ":\"anArray\",\"type\":20,\"childColumns\":[{\"name\":null,\"type\":5,\"childColumns\":null}]}"
                + ",{\"name\":\"aMap\",\"type\":21,\"childColumns\":[{\"name\":null,\"type\":23,\"childColumns\""
                + ":null},{\"name\":null,\"type\":5,\"childColumns\":null}]},{\"name\":\"anEnum\",\"type\":23"
                + ",\"childColumns\":null},{\"name\":\"aRecord\",\"type\":22,\"childColumns\":[{\"name\":\"a\","
                + "\"type\":5,\"childColumns\":null},{\"name\":\"b\",\"type\":8,\"childColumns\":null},{\"name\":"
                + "\"c\",\"type\":23,\"childColumns\":null}]},{\"name\":\"aUnion\",\"type\":22,\"childColumns\":"
                + "[{\"name\":\"string\",\"type\":23,\"childColumns\":null}]}]\n";

        Schema simpleEnumSchema = SchemaBuilder.enumeration("myEnumType").symbols("A", "B", "C");
        Schema simpleRecordSchema = SchemaBuilder.record("simpleRecord")
                .fields()
                .name("a")
                .type().intType().noDefault()
                .name("b")
                .type().doubleType().noDefault()
                .name("c")
                .type().stringType().noDefault()
                .endRecord();

        allTypesRecordSchema = SchemaBuilder.builder()
                .record("all")
                .fields()
                .name("aBoolean")
                .type().booleanType().noDefault()
                .name("aInt")
                .type().intType().noDefault()
                .name("aLong")
                .type().longType().noDefault()
                .name("aFloat")
                .type().floatType().noDefault()
                .name("aDouble")
                .type().doubleType().noDefault()
                .name("aString")
                .type().stringType().noDefault()
                .name("aBytes")
                .type().bytesType().noDefault()
                .name("aFixed")
                .type().fixed("myFixedType").size(16).noDefault()
                .name("anArray")
                .type().array().items().intType().noDefault()
                .name("aMap")
                .type().map().values().intType().noDefault()
                .name("anEnum")
                .type(simpleEnumSchema).noDefault()
                .name("aRecord")
                .type(simpleRecordSchema).noDefault()
                .name("aUnion")
                .type().optional().stringType()
                .endRecord();
    }

    @Test
    public void testParseTableSchema() throws IOException {
        TableSchema tableSchema = AvroTypeUtils.parseTableSchema(allTypesRecordSchema);
        String tableSchemaTableSchema = tableSchema.getTableSchema();
        JsonNode tableSchemaTree = objectMapper.readTree(tableSchemaTableSchema);

        JsonNode resultSchemaTree = objectMapper.readTree(result);
        Assert.assertEquals(resultSchemaTree, tableSchemaTree);
    }

}
