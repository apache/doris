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

suite("test_iceberg_nested_schema_evolution_ddl", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_nested_schema_evolution_ddl"
    String dbName = "iceberg_nested_schema_evolution_db"
    String tableName = "iceberg_nested_evolution"

    sql """drop catalog if exists ${catalogName}"""
    sql """
    CREATE CATALOG ${catalogName} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${restPort}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalogName};"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName};"""

    sql """set enable_fallback_to_original_planner=false;"""
    sql """set show_column_comment_in_describe=true;"""

    sql """drop table if exists ${tableName}"""
    sql """
    CREATE TABLE ${tableName} (
        id INT NOT NULL,
        s STRUCT<a:INT, b:STRING>,
        arr ARRAY<STRUCT<x:INT>>,
        m MAP<STRING, STRUCT<v:INT>>
    );
    """

    sql """
    INSERT INTO ${tableName} VALUES (
        1,
        STRUCT(10, 'old'),
        ARRAY(STRUCT(100)),
        MAP('k', STRUCT(1000))
    )
    """

    sql """ALTER TABLE ${tableName} ADD COLUMN s.c STRING NULL COMMENT 'new nested field'"""
    sql """ALTER TABLE ${tableName} ADD COLUMN s.first_pos STRING NULL FIRST"""
    sql """ALTER TABLE ${tableName} ADD COLUMN s.after_a STRING NULL AFTER a"""
    sql """ALTER TABLE ${tableName} ADD COLUMN arr.element.y INT NULL"""
    sql """ALTER TABLE ${tableName} ADD COLUMN arr.element.after_x INT NULL AFTER x"""
    sql """ALTER TABLE ${tableName} ADD COLUMN m.value.y INT NULL"""
    sql """ALTER TABLE ${tableName} ADD COLUMN m.value.after_v INT NULL AFTER v"""
    sql """ALTER TABLE ${tableName} ADD COLUMN s.drop_me STRING NULL"""
    sql """ALTER TABLE ${tableName} ADD COLUMN arr.element.drop_me INT NULL"""
    sql """ALTER TABLE ${tableName} ADD COLUMN m.value.drop_me INT NULL"""

    test {
        sql """ALTER TABLE ${tableName} ADD COLUMN s.required_field INT NOT NULL"""
        exception "Field 'required_field' doesn't have a default value"
    }

    sql """ALTER TABLE ${tableName} MODIFY COLUMN s.a BIGINT"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN arr.element.x BIGINT"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN m.value.v BIGINT"""

    sql """ALTER TABLE ${tableName} RENAME COLUMN s.c TO c2"""
    sql """ALTER TABLE ${tableName} RENAME COLUMN arr.element.y TO y2"""
    sql """ALTER TABLE ${tableName} RENAME COLUMN m.value.y TO y2"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN s.c2 COMMENT 'renamed struct field'"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN arr.element.y2 COMMENT 'renamed array element field'"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN m.value.y2 COMMENT 'renamed map value field'"""
    sql """ALTER TABLE ${tableName} DROP COLUMN s.drop_me"""
    sql """ALTER TABLE ${tableName} DROP COLUMN arr.element.drop_me"""
    sql """ALTER TABLE ${tableName} DROP COLUMN m.value.drop_me"""

    sql """
    INSERT INTO ${tableName} VALUES (
        2,
        STRUCT('first', 20, 'after_a', 'new', 'c2'),
        ARRAY(STRUCT(200, 202, 201)),
        MAP('k', STRUCT(2000, 2002, 2001))
    )
    """

    qt_desc """DESC ${tableName}"""

    def descRows = sql """DESC ${tableName}"""
    def normalizeType = { String s -> s.toLowerCase().replaceAll("\\s+", "") }
    def typeOf = { String name ->
        def row = descRows.find { it[0].toString().equalsIgnoreCase(name) }
        assertTrue(row != null, "column not found: ${name}")
        return normalizeType(row[1].toString())
    }

    def sType = typeOf("s")
    assertTrue(sType.contains("first_pos:text"), sType)
    assertTrue(sType.contains("a:bigint"), sType)
    assertTrue(sType.contains("after_a:text"), sType)
    assertTrue(sType.contains("c2:text"), sType)
    assertTrue(!sType.contains("drop_me"), sType)

    def arrType = typeOf("arr")
    assertTrue(arrType.contains("x:bigint"), arrType)
    assertTrue(arrType.contains("after_x:int"), arrType)
    assertTrue(arrType.contains("y2:int"), arrType)
    assertTrue(!arrType.contains("drop_me"), arrType)

    def mType = typeOf("m")
    assertTrue(mType.contains("v:bigint"), mType)
    assertTrue(mType.contains("after_v:int"), mType)
    assertTrue(mType.contains("y2:int"), mType)
    assertTrue(!mType.contains("drop_me"), mType)

    order_qt_query_rows """
        SELECT id,
               element_at(s, 'first_pos'),
               element_at(s, 'a'),
               element_at(s, 'after_a'),
               element_at(s, 'c2'),
               element_at(arr[1], 'x'),
               element_at(arr[1], 'after_x'),
               element_at(arr[1], 'y2'),
               element_at(m['k'], 'v'),
               element_at(m['k'], 'after_v'),
               element_at(m['k'], 'y2')
        FROM ${tableName}
        ORDER BY id
    """

    sql """drop table if exists ${tableName}"""
    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
