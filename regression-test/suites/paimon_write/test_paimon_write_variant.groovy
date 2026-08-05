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

suite("test_paimon_write_variant", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_catalog"
    String dbName = "test_pw_variant_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_basic;
        CREATE TABLE paimon.${dbName}.t_variant_basic (
            id INT,
            payload VARIANT,
            secondary VARIANT
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
        // Paimon Variant writes are deliberately V2-only.
        sql """SET enable_variant_v2 = false"""
        test {
            sql """INSERT INTO t_variant_basic VALUES
                (0, parse_to_variant('{"disabled":true}'), NULL)"""
            exception "set enable_variant_v2=true"
        }
        sql """SET enable_variant_v2 = true"""
        sql """SET force_jni_scanner = true"""

        // JSON containers, JSON null and SQL NULL are different logical values.
        sql """
            INSERT INTO t_variant_basic VALUES
                (1, parse_to_variant(CONCAT(
                        '{"object":{"name":"doris","address":{"city":"Hangzhou"}},"array":[1,true,null,"x"],"emptyObject":{},"emptyArray":[],"explicitNull":null,"escaped":"line',
                        CHAR(92), 'nquote', CHAR(92), CHAR(34), '","unicode":"中文😀"}')),
                    parse_to_variant('{"second":2}')),
                (2, parse_to_variant('{}'), parse_to_variant('[]')),
                (3, parse_to_variant('[]'), parse_to_variant('{}')),
                (4, parse_to_variant('null'), parse_to_variant('null')),
                (5, CAST(NULL AS VARIANT), CAST(NULL AS VARIANT))
        """

        // Typed scalar values exercise every V2 primitive family used by Doris.
        sql """
            INSERT INTO t_variant_basic VALUES
                (10, CAST(TRUE AS VARIANT), CAST(FALSE AS VARIANT)),
                (11, CAST(CAST(-128 AS TINYINT) AS VARIANT),
                     CAST(CAST(32767 AS SMALLINT) AS VARIANT)),
                (12, CAST(CAST(-2147483648 AS INT) AS VARIANT),
                     CAST(CAST(9223372036854775807 AS BIGINT) AS VARIANT)),
                (13, CAST(CAST(1.25 AS FLOAT) AS VARIANT),
                     CAST(CAST(-2.5 AS DOUBLE) AS VARIANT)),
                (14, CAST(CAST(123456.789 AS DECIMAL(12, 3)) AS VARIANT),
                     CAST(CAST(-0.000001 AS DECIMAL(18, 6)) AS VARIANT)),
                (15, CAST(CAST('plain-string' AS VARCHAR(32)) AS VARIANT),
                     CAST(CAST('中文😀' AS STRING) AS VARIANT)),
                (16, CAST(DATE '2024-02-29' AS VARIANT),
                     CAST(CAST('2024-02-29 12:34:56.123456' AS DATETIMEV2(6)) AS VARIANT)),
                (17, CAST(REPEAT('long-value-', 4096) AS VARIANT),
                     parse_to_variant('{"batch":"large-string"}'))
        """

        // Every VALUES row must reach Variant coercion before the inline table chooses a common
        // type. In particular, the integer in the first row must not become the string "1".
        sql """
            INSERT INTO t_variant_basic VALUES
                (20, 1, 'row-one'),
                (21, 'row-two', 2)
        """
        order_qt_variant_heterogeneous """
            SELECT id, payload, secondary
            FROM t_variant_basic
            WHERE id IN (20, 21)
            ORDER BY id
        """

        order_qt_variant_object """
            SELECT
                CAST(payload['object']['name'] AS STRING),
                CAST(payload['object']['address']['city'] AS STRING),
                CAST(payload['array'][1] AS INT),
                CAST(payload['array'][2] AS BOOLEAN),
                CAST(payload['emptyObject'] AS STRING),
                CAST(payload['emptyArray'] AS STRING),
                CAST(payload['unicode'] AS STRING),
                CAST(secondary['second'] AS INT)
            FROM t_variant_basic
            WHERE id = 1
        """

        order_qt_variant_nulls """
            SELECT id, payload, payload IS NULL, payload['missing'] IS NULL
            FROM t_variant_basic
            WHERE id IN (4, 5)
            ORDER BY id
        """

        order_qt_variant_scalars """
            SELECT id, payload, secondary
            FROM t_variant_basic
            WHERE id BETWEEN 10 AND 16
            ORDER BY id
        """

        qt_variant_long_string """
            SELECT LENGTH(CAST(payload AS STRING)), CAST(secondary['batch'] AS STRING)
            FROM t_variant_basic
            WHERE id = 17
        """

        // Refresh metadata and verify that all Doris-written rows remain readable through the
        // Paimon JNI Variant reader.
        sql """REFRESH TABLE t_variant_basic"""
        qt_variant_row_count """SELECT COUNT(*) FROM t_variant_basic"""
    } finally {
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
