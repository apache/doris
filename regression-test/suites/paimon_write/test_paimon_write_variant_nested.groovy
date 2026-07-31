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
    String root = '$'

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

        def nestedRows = spark_paimon """
            SELECT
                variant_get(variants[0], '${root}.kind', 'string'),
                variants[1] IS NULL,
                variant_get(variants[1], '${root}', 'string') IS NULL,
                variants[2] IS NULL,
                variant_get(variants[3], '${root}', 'int'),
                variant_get(variant_map['object'], '${root}.n', 'int'),
                variant_map['sql_null'] IS NULL,
                variant_get(variant_struct.payload, '${root}.kind', 'string'),
                variant_get(first_payload, '${root}.column', 'string'),
                variant_get(second_payload, '${root}[0]', 'string')
            FROM paimon.${dbName}.t_variant_nested
            WHERE id = 1
        """
        assertEquals([
                "array-object", "false", "true", "true", "7", "2", "true",
                "struct-object", "first", "second"
        ], nestedRows[0].collect { value -> value.toString() })

        def containerRows = spark_paimon """
            SELECT id,
                   variants IS NULL, SIZE(variants),
                   variant_map IS NULL, SIZE(variant_map),
                   variant_struct IS NULL
            FROM paimon.${dbName}.t_variant_nested
            WHERE id IN (2, 3)
            ORDER BY id
        """
        assertEquals(["2", "false", "0", "false", "0", "false"],
                containerRows[0].collect { value -> value.toString() })
        assertEquals(["3", "true", null, "true", null, "true"],
                containerRows[1].collect { value -> value == null ? null : value.toString() })

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

        def deepRows = spark_paimon """
            SELECT id,
                   deep.level1[0]['outer'].note,
                   variant_get(
                       deep.level1[0]['outer'].payload,
                       '${root}.level2.level3.level4.value',
                       'string')
            FROM paimon.${dbName}.t_variant_deep
            WHERE id = 1
        """
        assertEquals(["1", "depth-1", "deep-ok"],
                deepRows[0].collect { value -> value.toString() })

        def deepNullRows = spark_paimon """
            SELECT deep.level1[0]['null-leaf'].payload IS NULL
            FROM paimon.${dbName}.t_variant_deep
            WHERE id = 2
        """
        assertEquals(true, deepNullRows[0][0])

        sql """REFRESH TABLE t_variant_deep"""
        def dorisDeepRows = sql """
            SELECT CAST(
                deep.level1[1]['outer'].payload['level2']['level3']['level4']['value']
                AS STRING)
            FROM t_variant_deep
            WHERE id = 1
        """
        assertEquals("deep-ok", dorisDeepRows[0][0])
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
