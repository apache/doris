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

suite("test_paimon_write_variant_nested", "p0,external,paimon,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    def originalWriteBackend = sql("SELECT @@paimon_write_backend")[0][0]
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_nested_catalog"
    String dbName = "test_pw_variant_nested_db"
    String nestedShreddingSchema =
            '{"type":"ROW","fields":[{"id":0,"name":"variant_struct","type":' +
            '{"type":"ROW","fields":[{"id":1,"name":"payload","type":' +
            '{"type":"ROW","fields":[{"id":2,"name":"kind","type":"STRING"}]}}]}}]}'

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
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'write-only' = 'true',
            'variant.inferShreddingSchema' = 'true',
            'variant.shredding.inferenceMode' = 'adaptive'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_nested_configured;
        CREATE TABLE paimon.${dbName}.t_variant_nested_configured (
            id INT,
            variant_struct STRUCT<label:STRING, payload:VARIANT>
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'write-only' = 'true',
            'parquet.variant.shreddingSchema' = '${nestedShreddingSchema}'
        );

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
        TBLPROPERTIES ('file.format' = 'parquet', 'write-only' = 'true');
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

    String filesTableSuffix = '$files'
    def dataFiles = { String tableName ->
        spark_paimon("""
            SELECT file_path
            FROM paimon.${dbName}.`${tableName}${filesTableSuffix}`
            ORDER BY file_path
        """).collect { row -> row[0].toString() }
    }
    def rawColumnType = { String path, String columnName ->
        def columns = sql """DESC FUNCTION S3(
            "uri" = "${path}",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true",
            "format" = "parquet"
        )"""
        def column = columns.find { it[0].toString().equalsIgnoreCase(columnName) }
        assertTrue(column != null, "No ${columnName} column in Paimon data file ${path}")
        return column[1].toString().toLowerCase()
    }
    def strings = { rows -> rows.collect { row ->
        row.collect { value -> value == null ? null : value.toString() }
    } }

    try {
        sql """SET paimon_write_backend = 'CPP'"""
        setFeConfigTemporary([enable_variant_v2: true]) {
            assertTrue(getFeConfig("enable_variant_v2").toBoolean())
            sql """SET force_jni_scanner = true"""
            explain {
                sql "INSERT INTO t_variant_nested VALUES (0, NULL, NULL, NULL, NULL, NULL)"
                contains "backend: CPP"
            }
            explain {
                sql "INSERT INTO t_variant_deep VALUES (0, NULL)"
                contains "backend: CPP"
            }
            explain {
                sql "INSERT INTO t_variant_nested_configured VALUES (0, NULL)"
                contains "backend: CPP"
            }
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

        // Explicit shredding schemas address top-level Variant columns. A schema entry naming a
        // nested container is safely ignored by both SDKs and must not force the entire write to
        // JNI.
        sql """
            INSERT INTO t_variant_nested_configured VALUES
                (1, named_struct('label', 'configured', 'payload',
                    parse_to_variant('{"kind":"configured-nested"}'))),
                (2, named_struct('label', 'null', 'payload', CAST(NULL AS VARIANT)))
        """
        assertEquals([["configured-nested"]], strings(spark_paimon("""SELECT
            try_variant_get(variant_struct.payload, '\$.kind', 'string')
            FROM paimon.${dbName}.t_variant_nested_configured WHERE id = 1""")))

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

        // Read through a different SDK/engine, including the deepest SQL container path.
        assertEquals([["array-object", "2", "struct-object", "first", "second"]],
                strings(spark_paimon("""SELECT
                    try_variant_get(variants[0], '\$.kind', 'string'),
                    try_variant_get(variant_map['object'], '\$.n', 'int'),
                    try_variant_get(variant_struct.payload, '\$.kind', 'string'),
                    try_variant_get(first_payload, '\$.column', 'string'),
                    try_variant_get(second_payload, '\$[0]', 'string')
                    FROM paimon.${dbName}.t_variant_nested WHERE id=1""")))
        assertEquals([["deep-ok"]], strings(spark_paimon("""SELECT
                    try_variant_get(deep.level1[0]['outer'].payload,
                        '\$.level2.level3.level4.value', 'string')
                    FROM paimon.${dbName}.t_variant_deep WHERE id=1""")))
        assertEquals([["1"]], strings(spark_paimon("""SELECT
                    CAST(deep.level1[0]['null-leaf'].payload IS NULL AS INT)
                    FROM paimon.${dbName}.t_variant_deep WHERE id=2""")))

        // C++ inference descends through STRUCT, so the nested payload is physically shredded.
        // It intentionally does not descend through ARRAY or MAP containers; those elements keep
        // the ordinary value/metadata representation. All layouts must remain Java-readable.
        def inferredFiles = dataFiles("t_variant_nested")
        assertTrue(!inferredFiles.isEmpty())
        inferredFiles.each { filePath ->
            String structType = rawColumnType(filePath, "variant_struct")
            assertTrue(structType.contains("payload:struct"))
            assertTrue(structType.contains("typed_value:struct"))
            assertTrue(structType.contains("kind:struct"))
            assertFalse(rawColumnType(filePath, "variants").contains("typed_value"))
            assertFalse(rawColumnType(filePath, "variant_map").contains("typed_value"))
        }
        def configuredFiles = dataFiles("t_variant_nested_configured")
        assertTrue(!configuredFiles.isEmpty())
        configuredFiles.each { filePath ->
            assertFalse(rawColumnType(filePath, "variant_struct").contains("typed_value"))
        }

        // Refreshing metadata must not affect nested Variant reads.
        sql """REFRESH TABLE t_variant_deep"""
        qt_variant_deep_count """SELECT COUNT(*) FROM t_variant_deep"""
        }
    } finally {
        sql """SET paimon_write_backend = '${originalWriteBackend}'"""
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
