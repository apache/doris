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

suite("test_paimon_write_variant_errors", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_errors_catalog"
    String dbName = "test_pw_variant_errors_db"
    String root = '$'

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_error;
        CREATE TABLE paimon.${dbName}.t_variant_error (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES ('file.format' = 'parquet');

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_nested_error;
        CREATE TABLE paimon.${dbName}.t_variant_nested_error (
            id INT,
            payloads ARRAY<VARIANT>
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
    sql """CREATE DATABASE IF NOT EXISTS internal.${dbName}"""

    try {
        // Both top-level and nested targets fail during analysis when V2 is disabled.
        sql """SET enable_variant_v2 = false"""
        test {
            sql """INSERT INTO t_variant_error VALUES
                (1, parse_to_variant('{"disabled":"top"}'))"""
            exception "set enable_variant_v2=true"
        }
        test {
            sql """INSERT INTO t_variant_nested_error VALUES
                (1, CAST(NULL AS ARRAY<VARIANT>))"""
            exception "set enable_variant_v2=true"
        }

        // Capture V1 output types without executing a V1 load, then verify that a
        // later Paimon write rejects them in analysis before reaching the BE.
        sql """DROP VIEW IF EXISTS internal.${dbName}.v_variant_v1"""
        sql """DROP VIEW IF EXISTS internal.${dbName}.v_variant_v1_nested"""
        sql """
            CREATE VIEW internal.${dbName}.v_variant_v1 AS
            SELECT 10 AS id, parse_to_variant('{"legacy":true}') AS payload
        """
        sql """
            CREATE VIEW internal.${dbName}.v_variant_v1_nested AS
            SELECT 11 AS id,
                   array(parse_to_variant('{"legacy":"nested"}')) AS payloads
        """

        sql """SET enable_variant_v2 = true"""
        test {
            sql """
                INSERT INTO t_variant_error
                SELECT id, payload
                FROM internal.${dbName}.v_variant_v1
            """
            exception "Variant V1"
        }
        test {
            sql """
                INSERT INTO t_variant_nested_error
                SELECT id, payloads
                FROM internal.${dbName}.v_variant_v1_nested
            """
            exception "Variant V1"
        }

        // Valid V2 writes still work after analysis failures in the same session.
        sql """
            INSERT INTO t_variant_error VALUES
                (20, parse_to_variant('{"recovered":true}')),
                (21, try_parse_to_variant('not-json')),
                (22, CAST(CAST('{"typed":"string"}' AS STRING) AS VARIANT))
        """
        def rows = spark_paimon """
            SELECT id, payload IS NULL,
                   variant_get(payload, '${root}.recovered', 'boolean')
            FROM paimon.${dbName}.t_variant_error
            ORDER BY id
        """
        assertEquals([
                ["20", "false", "true"],
                ["21", "true", null],
                ["22", "false", null]
        ], rows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })
    } finally {
        sql """DROP VIEW IF EXISTS internal.${dbName}.v_variant_v1"""
        sql """DROP VIEW IF EXISTS internal.${dbName}.v_variant_v1_nested"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
