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

suite("test_paimon_write_variant_nested", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_nested_catalog"
    String dbName = "test_pw_variant_nested_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_nested;
        CREATE TABLE paimon.${dbName}.t_variant_nested (
            id INT,
            variants ARRAY<VARIANT>,
            variant_map MAP<STRING, VARIANT>,
            variant_struct STRUCT<label:STRING, payload:VARIANT>,
            first_payload VARIANT,
            second_payload VARIANT
        ) USING paimon
        TBLPROPERTIES ('file.format' = 'parquet');

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_deep;
        CREATE TABLE paimon.${dbName}.t_variant_deep (
            id INT,
            deep STRUCT<
                level1:ARRAY<
                    MAP<STRING, STRUCT<
                        note:STRING,
                        payload:VARIANT
                    >>
                >
            >
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
    sql """SET force_jni_scanner = true"""

    try {
        // ARRAY, MAP, STRUCT and multiple Variant columns in one Arrow batch.
        sql """
            INSERT INTO t_variant_nested VALUES
                (
                    1,
                    array(
                        parse_to_variant('{"kind":"array-object","n":1}'),
                        parse_to_variant('null'),
                        CAST(NULL AS VARIANT),
                        CAST(CAST(7 AS INT) AS VARIANT)
                    ),
                    map(
                        'object', parse_to_variant('{"kind":"map-object","n":2}'),
                        'json_null', parse_to_variant('null'),
                        'sql_null', CAST(NULL AS VARIANT)
                    ),
                    named_struct(
                        'label', 'struct-value',
                        'payload', parse_to_variant('{"kind":"struct-object","n":3}')
                    ),
                    parse_to_variant('{"column":"first"}'),
                    parse_to_variant('["second",2]')
                ),
                (
                    2,
                    array(),
                    map(),
                    named_struct('label', 'empty', 'payload', parse_to_variant('{}')),
                    parse_to_variant('[]'),
                    CAST(NULL AS VARIANT)
                ),
                (3, NULL, NULL, NULL, NULL, NULL)
        """

        order_qt_variant_nested_values """
            SELECT
                CAST(variants[1]['kind'] AS STRING),
                variants[2],
                variants[3],
                CAST(variants[4] AS INT),
                CAST(variant_map['object']['n'] AS INT),
                variant_map['json_null'],
                variant_map['sql_null'],
                CAST(variant_struct.payload['kind'] AS STRING),
                CAST(first_payload['column'] AS STRING),
                CAST(second_payload[1] AS STRING)
            FROM t_variant_nested
            WHERE id = 1
        """

        order_qt_variant_nested_containers """
            SELECT id,
                   variants IS NULL, SIZE(variants),
                   variant_map IS NULL, SIZE(variant_map),
                   variant_struct IS NULL
            FROM t_variant_nested
            WHERE id IN (2, 3)
            ORDER BY id
        """

        // Deep nesting is P0: STRUCT -> ARRAY -> MAP -> STRUCT -> VARIANT.
        sql """
            INSERT INTO t_variant_deep VALUES
                (
                    1,
                    named_struct(
                        'level1',
                        array(
                            map(
                                'outer',
                                named_struct(
                                    'note', 'depth-1',
                                    'payload', parse_to_variant(
                                        '{"level2":{"level3":{"level4":{"value":"deep-ok"}}}}')
                                )
                            )
                        )
                    )
                ),
                (
                    2,
                    named_struct(
                        'level1',
                        array(
                            map(
                                'null-leaf',
                                named_struct(
                                    'note', 'depth-null',
                                    'payload', CAST(NULL AS VARIANT)
                                )
                            )
                        )
                    )
                )
        """

        order_qt_variant_deep_value """
            SELECT id,
                   deep.level1[1]['outer'].note,
                   CAST(deep.level1[1]['outer'].payload['level2']['level3']['level4']['value']
                        AS STRING)
            FROM t_variant_deep
            WHERE id = 1
        """

        order_qt_variant_deep_null """
            SELECT deep.level1[1]['null-leaf'].payload IS NULL
            FROM t_variant_deep
            WHERE id = 2
        """

        // Refreshing metadata must not affect nested Variant reads.
        sql """REFRESH TABLE t_variant_deep"""
        qt_variant_deep_count """SELECT COUNT(*) FROM t_variant_deep"""
    } finally {
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
