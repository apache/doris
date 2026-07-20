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

import groovy.json.JsonSlurper

suite("test_iceberg_nested_schema_evolution_spark_doris_interop", "p0,external,iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_nested_schema_evolution_spark_doris_interop"
    String dbName = "iceberg_nested_schema_evolution_interop_db"
    String dorisTable = "doris_nested_evolution_to_spark"
    String commentTable = "doris_nested_comment_semantics"
    String sparkTable = "spark_nested_evolution_to_doris"
    String mixedCaseTable = "spark_mixed_case_nested_collision"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0"
        );
    """

    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner=false"""

    spark_iceberg """CREATE DATABASE IF NOT EXISTS demo.${dbName}"""

    sql """
        CREATE TABLE ${commentTable} (
            id INT,
            info STRUCT<
                metric:INT COMMENT 'metric doc',
                note:STRING COMMENT 'old note',
                clear_me:STRING COMMENT 'clear doc',
                payload:STRUCT<
                    name:STRING COMMENT 'name doc',
                    count:INT COMMENT 'count doc'
                > COMMENT 'payload doc'
            >
        )
    """
    sql """ALTER TABLE ${commentTable} MODIFY COLUMN info.note COMMENT 'new note'"""
    sql """ALTER TABLE ${commentTable} MODIFY COLUMN info.metric BIGINT"""
    sql """ALTER TABLE ${commentTable} MODIFY COLUMN info.clear_me STRING COMMENT ''"""
    sql """
        ALTER TABLE ${commentTable} MODIFY COLUMN info.payload
        STRUCT<name:STRING COMMENT '', count:BIGINT>
    """

    String loadTableUrl = "http://${externalEnvIp}:${restPort}/v1/namespaces/${dbName}/tables/${commentTable}"
    def loadTableResponse = new JsonSlurper().parseText(new URL(loadTableUrl).getText("UTF-8"))
    def tableMetadata = loadTableResponse.metadata
    def currentSchema = tableMetadata.schemas.find {
        it["schema-id"] == tableMetadata["current-schema-id"]
    }
    assertNotNull(currentSchema, "current schema should exist in Iceberg metadata")
    def infoColumn = currentSchema.fields.find { it.name == "info" }
    assertNotNull(infoColumn, "info column should exist in Iceberg metadata")
    def infoFields = infoColumn.type.fields.collectEntries { [(it.name): it] }
    assertEquals("long", infoFields.metric.type)
    assertEquals("metric doc", infoFields.metric.doc)
    assertEquals("new note", infoFields.note.doc)
    assertTrue(infoFields.clear_me.doc == null || infoFields.clear_me.doc == "")
    assertEquals("payload doc", infoFields.payload.doc)
    def payloadFields = infoFields.payload.type.fields.collectEntries { [(it.name): it] }
    assertTrue(payloadFields.name.doc == null || payloadFields.name.doc == "")
    assertEquals("long", payloadFields.count.type)
    assertEquals("count doc", payloadFields.count.doc)

    sql """
        CREATE TABLE ${dorisTable} (
            id INT NOT NULL,
            info STRUCT<metric:INT, label:STRING>,
            events ARRAY<STRUCT<score:INT>>,
            attrs MAP<STRING, STRUCT<code:INT>>
        )
    """
    sql """
        INSERT INTO ${dorisTable} VALUES (
            1,
            STRUCT(10, 'doris_before'),
            ARRAY(STRUCT(100)),
            MAP('k', STRUCT(1000))
        )
    """

    sql """ALTER TABLE ${dorisTable} ADD COLUMN info.doris_added STRING NULL COMMENT 'added by doris'"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN info.doris_first STRING NULL FIRST"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN info.doris_after_metric STRING NULL AFTER metric"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN events.element.doris_score INT NULL"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN events.element.doris_first_score INT NULL FIRST"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN events.element.doris_after_score INT NULL AFTER score"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN attrs.value.doris_code INT NULL"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN attrs.value.doris_first_code INT NULL FIRST"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN attrs.value.doris_after_code INT NULL AFTER code"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN info.drop_me STRING NULL"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN events.element.drop_me INT NULL"""
    sql """ALTER TABLE ${dorisTable} ADD COLUMN attrs.value.drop_me INT NULL"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN info.metric BIGINT"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN events.element.score BIGINT"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN attrs.value.code BIGINT"""
    sql """ALTER TABLE ${dorisTable} RENAME COLUMN info.doris_added TO doris_renamed"""
    sql """ALTER TABLE ${dorisTable} RENAME COLUMN events.element.doris_score TO doris_score_renamed"""
    sql """ALTER TABLE ${dorisTable} RENAME COLUMN attrs.value.doris_code TO doris_code_renamed"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN info.doris_renamed COMMENT 'renamed by doris'"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN events.element.doris_score_renamed COMMENT 'renamed by doris'"""
    sql """ALTER TABLE ${dorisTable} MODIFY COLUMN attrs.value.doris_code_renamed COMMENT 'renamed by doris'"""
    sql """ALTER TABLE ${dorisTable} DROP COLUMN info.drop_me"""
    sql """ALTER TABLE ${dorisTable} DROP COLUMN events.element.drop_me"""
    sql """ALTER TABLE ${dorisTable} DROP COLUMN attrs.value.drop_me"""

    spark_iceberg """REFRESH TABLE demo.${dbName}.${dorisTable}"""
    spark_iceberg """
        INSERT INTO demo.${dbName}.${dorisTable} VALUES (
            2,
            NAMED_STRUCT('doris_first', 'spark_first',
                         'metric', CAST(20 AS BIGINT),
                         'doris_after_metric', 'spark_after_metric',
                         'label', 'spark_after_doris',
                         'doris_renamed', 'spark_can_write_doris_field'),
            ARRAY(NAMED_STRUCT('doris_first_score', 202,
                               'score', CAST(200 AS BIGINT),
                               'doris_after_score', 203,
                               'doris_score_renamed', 201)),
            MAP('k', NAMED_STRUCT('doris_first_code', 2002,
                                  'code', CAST(2000 AS BIGINT),
                                  'doris_after_code', 2003,
                                  'doris_code_renamed', 2001))
        )
    """

    sql """refresh table ${dbName}.${dorisTable}"""
    def dorisDrivenSparkRows = spark_iceberg """
        SELECT id,
               info.doris_first,
               info.metric,
               info.doris_after_metric,
               info.label,
               info.doris_renamed,
               events[0].doris_first_score,
               events[0].score,
               events[0].doris_after_score,
               events[0].doris_score_renamed,
               attrs['k'].doris_first_code,
               attrs['k'].code,
               attrs['k'].doris_after_code,
               attrs['k'].doris_code_renamed
        FROM demo.${dbName}.${dorisTable}
        ORDER BY id
    """
    String dorisDrivenDorisQuery = """
        SELECT id,
               element_at(info, 'doris_first'),
               element_at(info, 'metric'),
               element_at(info, 'doris_after_metric'),
               element_at(info, 'label'),
               element_at(info, 'doris_renamed'),
               element_at(events[1], 'doris_first_score'),
               element_at(events[1], 'score'),
               element_at(events[1], 'doris_after_score'),
               element_at(events[1], 'doris_score_renamed'),
               element_at(attrs['k'], 'doris_first_code'),
               element_at(attrs['k'], 'code'),
               element_at(attrs['k'], 'doris_after_code'),
               element_at(attrs['k'], 'doris_code_renamed')
        FROM ${dorisTable}
        ORDER BY id
    """
    order_qt_doris_driven_rows dorisDrivenDorisQuery
    def dorisDrivenDorisRows = sql dorisDrivenDorisQuery
    assertSparkDorisResultEquals(dorisDrivenSparkRows, dorisDrivenDorisRows)

    spark_iceberg_multi """
        DROP TABLE IF EXISTS demo.${dbName}.${sparkTable};
        DROP TABLE IF EXISTS demo.${dbName}.${mixedCaseTable};
        CREATE TABLE demo.${dbName}.${mixedCaseTable} (
            Id INT,
            Label STRING,
            Info STRUCT<Metric:INT, Label:STRING>
        ) USING iceberg;
        CREATE TABLE demo.${dbName}.${sparkTable} (
            id INT,
            info STRUCT<metric:INT, label:STRING>,
            events ARRAY<STRUCT<score:INT>>,
            attrs MAP<STRING, STRUCT<code:INT>>
        ) USING iceberg;
        INSERT INTO demo.${dbName}.${sparkTable} VALUES (
            1,
            NAMED_STRUCT('metric', 10, 'label', 'spark_before'),
            ARRAY(NAMED_STRUCT('score', 100)),
            MAP('k', NAMED_STRUCT('code', 1000))
        );
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN info.spark_added STRING;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN info.spark_first STRING FIRST;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN info.spark_after_metric STRING AFTER metric;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN events.element.spark_score INT;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN events.element.spark_first_score INT FIRST;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN events.element.spark_after_score INT AFTER score;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN attrs.value.spark_code INT;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN attrs.value.spark_first_code INT FIRST;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN attrs.value.spark_after_code INT AFTER code;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN info.drop_me STRING;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN events.element.drop_me INT;
        ALTER TABLE demo.${dbName}.${sparkTable} ADD COLUMN attrs.value.drop_me INT;
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN info.metric TYPE BIGINT;
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN events.element.score TYPE BIGINT;
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN attrs.value.code TYPE BIGINT;
        ALTER TABLE demo.${dbName}.${sparkTable} RENAME COLUMN info.spark_added TO spark_renamed;
        ALTER TABLE demo.${dbName}.${sparkTable} RENAME COLUMN events.element.spark_score TO spark_score_renamed;
        ALTER TABLE demo.${dbName}.${sparkTable} RENAME COLUMN attrs.value.spark_code TO spark_code_renamed;
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN info.spark_renamed COMMENT 'renamed by spark';
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN events.element.spark_score_renamed COMMENT 'renamed by spark';
        ALTER TABLE demo.${dbName}.${sparkTable} ALTER COLUMN attrs.value.spark_code_renamed COMMENT 'renamed by spark';
        ALTER TABLE demo.${dbName}.${sparkTable} DROP COLUMN info.drop_me;
        ALTER TABLE demo.${dbName}.${sparkTable} DROP COLUMN events.element.drop_me;
        ALTER TABLE demo.${dbName}.${sparkTable} DROP COLUMN attrs.value.drop_me;
        INSERT INTO demo.${dbName}.${sparkTable} VALUES (
            2,
            NAMED_STRUCT('spark_first', 'spark_first_field',
                         'metric', CAST(20 AS BIGINT),
                         'spark_after_metric', 'spark_after_metric_field',
                         'label', 'spark_after',
                         'spark_renamed', 'spark_new_field'),
            ARRAY(NAMED_STRUCT('spark_first_score', 202,
                               'score', CAST(200 AS BIGINT),
                               'spark_after_score', 203,
                               'spark_score_renamed', 201)),
            MAP('k', NAMED_STRUCT('spark_first_code', 2002,
                                  'code', CAST(2000 AS BIGINT),
                                  'spark_after_code', 2003,
                                  'spark_code_renamed', 2001))
        );
    """

    sql """refresh catalog ${catalogName}"""
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    test {
        sql """ALTER TABLE ${mixedCaseTable} ADD COLUMN id STRING NULL"""
        exception "conflicts with existing Iceberg field 'Id' (case-insensitive)"
    }
    test {
        sql """ALTER TABLE ${mixedCaseTable} ADD COLUMN (id STRING)"""
        exception "conflicts with existing Iceberg field 'Id' (case-insensitive)"
    }
    test {
        sql """ALTER TABLE ${mixedCaseTable} ADD COLUMN (new_field STRING, NEW_FIELD STRING)"""
        exception "conflicts with another requested column (case-insensitive)"
    }
    test {
        sql """ALTER TABLE ${mixedCaseTable} RENAME COLUMN label TO id"""
        exception "conflicts with existing Iceberg field 'Id' (case-insensitive)"
    }
    sql """ALTER TABLE ${mixedCaseTable} RENAME COLUMN label TO label"""

    test {
        sql """ALTER TABLE ${mixedCaseTable} ADD COLUMN info.metric STRING NULL"""
        exception "conflicts with existing Iceberg field 'Info.Metric' (case-insensitive)"
    }
    test {
        sql """ALTER TABLE ${mixedCaseTable} RENAME COLUMN info.label TO metric"""
        exception "conflicts with existing Iceberg field 'Info.Metric' (case-insensitive)"
    }

    qt_spark_driven_schema """
        SELECT COLUMN_NAME, COLUMN_TYPE
        FROM ${catalogName}.information_schema.columns
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = '${sparkTable}'
        ORDER BY ORDINAL_POSITION
    """

    String sparkDrivenDorisQuery = """
        SELECT id,
               element_at(info, 'spark_first'),
               element_at(info, 'metric'),
               element_at(info, 'spark_after_metric'),
               element_at(info, 'label'),
               element_at(info, 'spark_renamed'),
               element_at(events[1], 'spark_first_score'),
               element_at(events[1], 'score'),
               element_at(events[1], 'spark_after_score'),
               element_at(events[1], 'spark_score_renamed'),
               element_at(attrs['k'], 'spark_first_code'),
               element_at(attrs['k'], 'code'),
               element_at(attrs['k'], 'spark_after_code'),
               element_at(attrs['k'], 'spark_code_renamed')
        FROM ${sparkTable}
        ORDER BY id
    """
    order_qt_spark_driven_rows_before_write sparkDrivenDorisQuery

    sql """
        INSERT INTO ${sparkTable} VALUES (
            3,
            STRUCT('doris_first_field', 30, 'doris_after_metric_field',
                   'doris_after_spark', 'doris_can_write_spark_field'),
            ARRAY(STRUCT(302, 300, 303, 301)),
            MAP('k', STRUCT(3002, 3000, 3003, 3001))
        )
    """

    spark_iceberg """REFRESH TABLE demo.${dbName}.${sparkTable}"""
    def sparkDrivenSparkRows = spark_iceberg """
        SELECT id,
               info.spark_first,
               info.metric,
               info.spark_after_metric,
               info.label,
               info.spark_renamed,
               events[0].spark_first_score,
               events[0].score,
               events[0].spark_after_score,
               events[0].spark_score_renamed,
               attrs['k'].spark_first_code,
               attrs['k'].code,
               attrs['k'].spark_after_code,
               attrs['k'].spark_code_renamed
        FROM demo.${dbName}.${sparkTable}
        ORDER BY id
    """
    order_qt_spark_driven_rows_after_write sparkDrivenDorisQuery
    def sparkDrivenDorisRows = sql sparkDrivenDorisQuery
    assertSparkDorisResultEquals(sparkDrivenSparkRows, sparkDrivenDorisRows)
}
