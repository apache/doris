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

suite("test_paimon_write_variant_table_modes", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_modes_catalog"
    String dbName = "test_pw_variant_modes_db"
    String root = '$'

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_pk;
        CREATE TABLE paimon.${dbName}.t_variant_pk (
            id INT,
            payload VARIANT,
            version BIGINT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id',
            'file.format' = 'parquet'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_dynamic_bucket;
        CREATE TABLE paimon.${dbName}.t_variant_dynamic_bucket (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'file.format' = 'parquet'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_schema;
        CREATE TABLE paimon.${dbName}.t_variant_schema (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES ('file.format' = 'parquet');

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_shredded;
        CREATE TABLE paimon.${dbName}.t_variant_shredded (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'parquet.variant.shreddingSchema' =
            '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"city","type":"STRING"}]}}]}'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_non_variant;
        CREATE TABLE paimon.${dbName}.t_non_variant (
            id INT,
            payload STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_required;
        CREATE TABLE paimon.${dbName}.t_variant_required (
            id INT,
            payload VARIANT NOT NULL
        ) USING paimon
        TBLPROPERTIES ('file.format' = 'parquet');
    """

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """SWITCH ${catalogName}"""
    sql """USE ${dbName}"""
    sql """SET enable_variant_v2 = true"""

    try {
        // Fixed-bucket primary-key table: later rows replace the same key.
        sql """
            INSERT INTO t_variant_pk VALUES
                (1, parse_to_variant('{"state":"v1","n":1}'), 1),
                (2, parse_to_variant('{"state":"stable","n":2}'), 1),
                (1, parse_to_variant('{"state":"v2","n":10}'), 2)
        """
        sql """
            INSERT INTO t_variant_pk VALUES
                (1, parse_to_variant('{"state":"v3","n":100}'), 3)
        """
        def pkRows = spark_paimon """
            SELECT id,
                   variant_get(payload, '${root}.state', 'string'),
                   variant_get(payload, '${root}.n', 'int'),
                   version
            FROM paimon.${dbName}.t_variant_pk
            ORDER BY id
        """
        assertEquals([
                ["1", "v3", "100", "3"],
                ["2", "stable", "2", "1"]
        ], pkRows.collect { row -> row.collect { value -> value.toString() } })

        // Dynamic bucket routing with Variant values.
        sql """
            INSERT INTO t_variant_dynamic_bucket
            SELECT number,
                   parse_to_variant(CONCAT('{"bucket":"dynamic","id":', number, '}'))
            FROM numbers("number" = "32")
        """
        def dynamicRows = spark_paimon """
            SELECT COUNT(*),
                   SUM(variant_get(payload, '${root}.id', 'int'))
            FROM paimon.${dbName}.t_variant_dynamic_bucket
        """
        assertEquals(["32", "496"],
                dynamicRows[0].collect { value -> value.toString() })

        // Schema evolution: add Variant, write it, then add a normal column and continue writing.
        sql """INSERT INTO t_variant_schema VALUES (1, 'before')"""
        sql """ALTER TABLE t_variant_schema ADD COLUMN payload VARIANT NULL AFTER name"""
        sql """
            INSERT INTO t_variant_schema (payload, name, id) VALUES
                (parse_to_variant('{"schema":"added"}'), 'after-add', 2)
        """
        sql """ALTER TABLE t_variant_schema ADD COLUMN note STRING NULL DEFAULT 'default-note'"""
        sql """
            INSERT INTO t_variant_schema (id, name, payload) VALUES
                (3, 'after-normal-column', parse_to_variant('{"schema":"continued"}'))
        """
        spark_paimon """REFRESH TABLE paimon.${dbName}.t_variant_schema"""
        def schemaRows = spark_paimon """
            SELECT id, name,
                   variant_get(payload, '${root}.schema', 'string'),
                   note
            FROM paimon.${dbName}.t_variant_schema
            ORDER BY id
        """
        assertEquals([
                ["1", "before", null, null],
                ["2", "after-add", "added", null],
                ["3", "after-normal-column", "continued", "default-note"]
        ], schemaRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        // Paimon Parquet shredding still receives standard V2 value/metadata bytes.
        sql """
            INSERT INTO t_variant_shredded VALUES
                (1, parse_to_variant('{"age":27,"city":"Beijing"}')),
                (2, parse_to_variant('{"age":28}')),
                (3, parse_to_variant('{"city":"Hangzhou","other":"kept"}')),
                (4, parse_to_variant('"scalar"')),
                (5, parse_to_variant('{}'))
        """
        def shreddedRows = spark_paimon """
            SELECT id,
                   variant_get(payload, '${root}.age', 'int'),
                   variant_get(payload, '${root}.city', 'string'),
                   variant_get(payload, '${root}.other', 'string')
            FROM paimon.${dbName}.t_variant_shredded
            ORDER BY id
        """
        assertEquals([
                ["1", "27", "Beijing", null],
                ["2", "28", null, null],
                ["3", null, "Hangzhou", "kept"],
                ["4", null, null, null],
                ["5", null, null, null]
        ], shreddedRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        // Non-Variant Paimon writes remain unchanged while the session enables V2.
        sql """INSERT INTO t_non_variant VALUES (1, '{"plain":"string"}')"""
        def nonVariantRows = spark_paimon """
            SELECT id, payload FROM paimon.${dbName}.t_non_variant
        """
        assertEquals(["1", '{"plain":"string"}'],
                nonVariantRows[0].collect { value -> value.toString() })

        // Paimon's real NOT NULL schema is enforced by the SDK.
        test {
            sql """INSERT INTO t_variant_required VALUES (1, CAST(NULL AS VARIANT))"""
            exception "Cannot write null to non-null column(payload)"
        }
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
