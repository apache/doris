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

import org.apache.doris.regression.util.Http

suite("test_schema_api") {

    def thisDb = sql """select database()""";
    thisDb = thisDb[0][0];
    logger.info("current database is ${thisDb}");

    def tbName = "test_schema_api"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
        CREATE TABLE ${tbName}
        (
            `id` LARGEINT NOT NULL COMMENT "id",
            `c1` DECIMAL(10, 2) COMMENT "decimal columns",
            `c2` date NOT NULL COMMENT "date columns",
            `c3` VARCHAR(20) COMMENT "nullable columns",
            `c4` VARCHAR COMMENT "varchar columns",
            `c5` BIGINT DEFAULT "0" COMMENT "test columns",
            `c6` ARRAY<DECIMAL(18, 4)> COMMENT "array column",
            `c7` MAP<VARCHAR(16), BIGINT> COMMENT "map column",
            `c8` STRUCT<
                price:DECIMAL(18, 4) COMMENT 'unit price',
                tags:ARRAY<VARCHAR(32)>
            > COMMENT "struct column"
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    //exist table
    def url = String.format("http://%s/api/%s/%s/_schema", context.config.feHttpAddress, thisDb, tbName)
    Boolean enableTLS = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false
    if (enableTLS) {
        Http.configure(enableTLS, 
            context.config.otherConfigs.get("tlsVerifyMode"),
            context.config.otherConfigs.get("trustStorePath"),
            context.config.otherConfigs.get("trustStorePassword"),
            context.config.otherConfigs.get("trustStoreType"),
            context.config.otherConfigs.get("keyStorePath"),
            context.config.otherConfigs.get("keyStorePassword"),
            context.config.otherConfigs.get("keyStoreType")
        )
    }
    logger.info("url: ${url}")
    def result = Http.GET(url, true)
    assertTrue(result.code == 0)
    assertEquals(result.msg, "success")
    // parsing
    def resultList = result.data.properties
    assertEquals(9, resultList.size())

    def columns = resultList.collectEntries { [(it.name): it] }

    def arrayColumn = columns.c6
    assertEquals("ARRAY", arrayColumn.type)
    assertEquals("array<decimalv3(18,4)>", arrayColumn.type_sql)
    def arrayDesc = arrayColumn.type_desc
    assertEquals("ARRAY", arrayDesc.kind)
    assertTrue(arrayDesc.containsKey("contains_null"))
    assertFalse(arrayDesc.containsKey("containsNull"))
    assertTrue(arrayDesc.contains_null)
    assertEquals("DECIMAL64", arrayDesc.element.kind)
    assertEquals(18, arrayDesc.element.precision)
    assertEquals(4, arrayDesc.element.scale)

    def mapColumn = columns.c7
    assertEquals("MAP", mapColumn.type)
    assertEquals("map<varchar(16),bigint>", mapColumn.type_sql)
    def mapDesc = mapColumn.type_desc
    assertEquals("MAP", mapDesc.kind)
    assertTrue(mapDesc.containsKey("key_contains_null"))
    assertTrue(mapDesc.containsKey("value_contains_null"))
    assertFalse(mapDesc.containsKey("keyContainsNull"))
    assertFalse(mapDesc.containsKey("valueContainsNull"))
    assertTrue(mapDesc.key_contains_null)
    assertTrue(mapDesc.value_contains_null)
    assertEquals("VARCHAR", mapDesc.key.kind)
    assertEquals(16, mapDesc.key.length)
    assertEquals("BIGINT", mapDesc.value.kind)

    def structColumn = columns.c8
    assertEquals("STRUCT", structColumn.type)
    def structDesc = structColumn.type_desc
    assertEquals("STRUCT", structDesc.kind)
    def structFields = structDesc.fields.collectEntries { [(it.name): it] }
    assertTrue(structFields.price.containsKey("contains_null"))
    assertFalse(structFields.price.containsKey("containsNull"))
    assertTrue(structFields.price.contains_null)
    assertEquals("unit price", structFields.price.comment)
    assertEquals("DECIMAL64", structFields.price.type.kind)
    assertEquals(18, structFields.price.type.precision)
    assertEquals(4, structFields.price.type.scale)
    assertEquals("ARRAY", structFields.tags.type.kind)
    assertTrue(structFields.tags.type.containsKey("contains_null"))
    assertFalse(structFields.tags.type.containsKey("containsNull"))
    assertEquals("VARCHAR", structFields.tags.type.element.kind)
    assertEquals(32, structFields.tags.type.element.length)

    // not exist catalog
    def url2 = String.format("http://%s/api/%s/%s/%s/_schema", context.config.feHttpAddress, "notexistctl", thisDb, tbName)
    def result2 = Http.GET(url2, true)
    assertTrue(result2.code != 0)
    assertTrue(result2.data.contains("Unknown catalog"))

}
