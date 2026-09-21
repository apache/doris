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

suite("test_paimon_write_variant_errors", "p0,external,paimon,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_errors_catalog"
    String dbName = "test_pw_variant_errors_db"

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

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_coercion_source;
        CREATE TABLE paimon.${dbName}.t_variant_coercion_source (
            id INT,
            payload VARIANT
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

    try {
        sql """SET force_jni_scanner = true"""

        sql """
            INSERT INTO t_variant_coercion_source VALUES
                (1, parse_to_variant('{"kind":"object-source"}')),
                (2, parse_to_variant('["array-source",2]'))
        """

        // Keep master's common-type semantics: mixed Variant and numeric expressions resolve to DECIMAL.
        // Object and array Variant values therefore become SQL NULL before the connector sink converts
        // the scalar result back to Paimon's Variant type.
        sql """
            INSERT INTO t_variant_error
            SELECT 100, payload FROM t_variant_coercion_source WHERE id = 1
            UNION ALL
            SELECT 101, 1
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 102, payload FROM t_variant_coercion_source WHERE id = 2
            UNION ALL
            SELECT 103, 1
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 104, IF(TRUE, payload, 1)
            FROM t_variant_coercion_source WHERE id = 1
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 105, IF(TRUE, payload, 1)
            FROM t_variant_coercion_source WHERE id = 2
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 106, CASE WHEN TRUE THEN payload ELSE 1 END
            FROM t_variant_coercion_source WHERE id = 1
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 107, CASE WHEN TRUE THEN payload ELSE 1 END
            FROM t_variant_coercion_source WHERE id = 2
        """
        sql """
            INSERT INTO t_variant_error
            WITH RECURSIVE source(iter, payload) AS (
                SELECT CAST(0 AS INT), IF(TRUE, payload, 1)
                FROM t_variant_coercion_source WHERE id = 1
                UNION ALL
                SELECT CAST(iter + 1 AS INT), CAST(1 AS DECIMAL(38, 9))
                FROM source WHERE iter < 1
            )
            SELECT 108 + iter, payload FROM source
        """
        sql """
            INSERT INTO t_variant_error
            SELECT 110, generated_payload
            FROM (SELECT payload FROM t_variant_coercion_source WHERE id = 1) source
            LATERAL VIEW explode(ARRAY(IF(TRUE, payload, 1))) generated AS generated_payload
        """

        sql """
            INSERT INTO t_variant_error VALUES
                (20, parse_to_variant('{"recovered":true}')),
                (21, try_parse_to_variant('not-json')),
                (22, CAST(CAST('{"typed":"string"}' AS STRING) AS VARIANT))
        """
        // Primitive-to-Variant coercion remains supported for both inline VALUES and ordinary SELECT.
        sql """
            INSERT INTO t_variant_error VALUES
                (23, 7),
                (24, 'values-string'),
                (25, TRUE),
                (26, ARRAY(1, 2))
        """
        sql """INSERT INTO t_variant_error SELECT 27, 8"""
        sql """INSERT INTO t_variant_error SELECT 28, 'select-string'"""
        sql """INSERT INTO t_variant_error SELECT 29, FALSE"""
        sql """INSERT INTO t_variant_error SELECT 30, ARRAY(3, 4)"""

        spark_paimon """REFRESH TABLE paimon.${dbName}.t_variant_error"""
        def sparkPrimitiveRows = spark_paimon """
            SELECT id, to_json(payload)
            FROM paimon.${dbName}.t_variant_error
            WHERE id BETWEEN 23 AND 30
            ORDER BY id
        """
        assertEquals([
                ["23", "7"],
                ["24", '"values-string"'],
                ["25", "true"],
                ["26", "[1,2]"],
                ["27", "8"],
                ["28", '"select-string"'],
                ["29", "false"],
                ["30", "[3,4]"]
        ], sparkPrimitiveRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        def sparkCoercionRows = spark_paimon """
            SELECT id, to_json(payload)
            FROM paimon.${dbName}.t_variant_error
            WHERE id BETWEEN 100 AND 110
            ORDER BY id
        """
        assertEquals([
                ["100", null],
                ["101", "1"],
                ["102", null],
                ["103", "1"],
                ["104", null],
                ["105", null],
                ["106", null],
                ["107", null],
                ["108", null],
                ["109", "1"],
                ["110", null]
        ], sparkCoercionRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        // Invalid JSON is preserved as a Variant string unless the global
        // throw-on-invalid-JSON option is enabled.
        order_qt_variant_after_errors """
            SELECT id, payload IS NULL,
                   CAST(payload['recovered'] AS BOOLEAN),
                   payload
            FROM t_variant_error
            WHERE id BETWEEN 20 AND 30
            ORDER BY id
        """
    } finally {
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
