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

suite("test_paimon_cpp_write_variant", "p0,external,paimon,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        logger.info("disable paimon test.")
        return
    }
    def originalBackend = sql("SELECT @@paimon_write_backend")[0][0]
    def originalScanner = sql("SELECT @@force_jni_scanner")[0][0]
    def originalBatchSize = sql("SELECT @@batch_size")[0][0]
    String ip = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "test_cpp_variant_catalog"
    String db = "test_cpp_variant_db"
    def asStrings = { rows -> rows.collect { row -> row.collect { it == null ? null : it.toString() } } }
    // Compare entire JSON values without depending on object-member order or whitespace.
    def jsonValues = { rows -> rows.collect { row ->
        [row[0].toString(), row[1] == null ? null : new JsonSlurper().parseText(row[1].toString())]
    } }
    try {
        spark_paimon "CREATE DATABASE IF NOT EXISTS paimon.${db}"
        ["t_cpp", "t_jni"].each { table ->
            spark_paimon_multi """
                DROP TABLE IF EXISTS paimon.${db}.${table};
                CREATE TABLE paimon.${db}.${table} (
                    id INT, payload VARIANT, secondary VARIANT,
                    variants ARRAY<VARIANT>, attrs MAP<STRING, VARIANT>,
                    nested STRUCT<label:STRING, payload:VARIANT>
                ) USING paimon TBLPROPERTIES (
                    'bucket'='-1', 'file.format'='parquet', 'write-only'='true',
                    'variant.inferShreddingSchema'='false', 'read.batch-size'='16384'
                );
            """
        }
        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${db}.t_required;
            CREATE TABLE paimon.${db}.t_required (id INT, payload VARIANT NOT NULL)
            USING paimon TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet', 'write-only'='true');
            DROP TABLE IF EXISTS paimon.${db}.t_map_keys;
            CREATE TABLE paimon.${db}.t_map_keys (
                id INT, int_map MAP<INT, VARIANT>, long_map MAP<BIGINT, VARIANT>
            ) USING paimon TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet', 'write-only'='true');
        """
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type'='paimon', 'paimon.catalog.type'='filesystem', 'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${ip}:${port}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 's3.path.style.access'='true'
        )"""
        sql "SWITCH ${catalog}"
        sql "USE ${db}"
        sql "SET force_jni_scanner=true"
        sql "SET batch_size=64"
        setFeConfigTemporary([enable_variant_v2: true]) {
            sql "SET paimon_write_backend='CPP'"
            explain {
                sql "INSERT INTO t_required VALUES (0, parse_to_variant('{}'))"
                contains "backend: CPP"
            }
            test {
                sql "INSERT INTO t_required VALUES (0, CAST(NULL AS VARIANT))"
                exception "CheckNullabilityMatch failed, field payload not nullable"
            }
            assertEquals([[0L]], sql("SELECT COUNT(*) FROM t_required"))
            // Variant null is a value, unlike SQL NULL, and is legal in a required column.
            sql "INSERT INTO t_required VALUES (1, parse_to_variant('null')), (2, parse_to_variant('{}'))"
            assertEquals([[1, 0], [2, 0]], sql("SELECT id, CAST(payload IS NULL AS INT) FROM t_required ORDER BY id"))
            explain {
                sql "INSERT INTO t_map_keys VALUES (0, NULL, NULL)"
                contains "backend: CPP"
            }
            sql """INSERT INTO t_map_keys VALUES
                (1, map(7, parse_to_variant('{"n":7}'), 8, CAST(NULL AS VARIANT)),
                    map(CAST(9223372036854775807 AS BIGINT), parse_to_variant('[1,true,null]'))),
                (2, map(), map()), (3, NULL, NULL)
            """
            assertEquals([["7", "1", "[1,true,null]"]], asStrings(sql("""SELECT
                CAST(int_map[7]['n'] AS BIGINT), CAST(int_map[8] IS NULL AS INT),
                CAST(long_map[CAST(9223372036854775807 AS BIGINT)] AS STRING)
                FROM t_map_keys WHERE id=1""")))
            assertEquals([["7", "1", "[1,true,null]"]], asStrings(spark_paimon("""SELECT
                try_variant_get(int_map[7], '\$.n', 'bigint'), CAST(int_map[8] IS NULL AS INT),
                CAST(long_map[CAST(9223372036854775807 AS BIGINT)] AS STRING)
                FROM paimon.${db}.t_map_keys WHERE id=1""")))
            ["CPP", "JNI"].each { backend ->
                String table = "t_${backend.toLowerCase()}"
                sql "SET paimon_write_backend='${backend}'"
                // 4097 rows span many conversion batches, including a partial final batch.
                String source = """SELECT CAST(number AS INT),
                    parse_to_variant(CONCAT('{"n":', CAST(number AS STRING),
                        ',"text":"中文😀","nested":{"items":[1,null,true]},"long":"',
                        REPEAT('x', 1024), '"}')),
                    IF(number % 2 = 0, CAST(NULL AS VARIANT), parse_to_variant('null')),
                    array(CAST(number AS VARIANT), parse_to_variant('null'), CAST(NULL AS VARIANT)),
                    map('n', CAST(number AS VARIANT), 'null', CAST(NULL AS VARIANT)),
                    named_struct('label', 'row', 'payload', CAST(number AS VARIANT))
                    FROM numbers('number'='4097')"""
                explain {
                    sql "INSERT INTO ${table} ${source}"
                    contains "backend: ${backend}"
                }
                sql "INSERT INTO ${table} ${source} WHERE number < 0"
                assertEquals([[0L]], sql("SELECT COUNT(*) FROM ${table}"))
                sql "INSERT INTO ${table} ${source}"
                // A separate append covers empty containers, parent NULLs and scalar Variant roots.
                sql """INSERT INTO ${table} VALUES
                    (5000, parse_to_variant('{}'), parse_to_variant('[]'), array(), map(),
                        named_struct('label', 'empty', 'payload', parse_to_variant('{}'))),
                    (5001, NULL, NULL, NULL, NULL, NULL),
                    (5002, parse_to_variant('42'), parse_to_variant('"scalar"'), NULL, NULL, NULL)
                """
                assertEquals([[4100L]], sql("SELECT COUNT(*) FROM ${table}"))
            }
            // The write/output batch stays at 64. The table's read.batch-size=16384 independently
            // covers this fixture's 4097 * 3 array elements in both Doris and Spark. Java SDK
            // 1.4.2 assembleVariantBatch does not grow value/metadata children when the parent
            // vector expands. This bounded reader setup is not a general fix for that SDK bug.
            String projection = """id, CAST(payload['n'] AS BIGINT),
                CAST(variants[1] AS BIGINT), CAST(attrs['n'] AS BIGINT),
                CAST(nested.payload AS BIGINT), CAST(secondary IS NULL AS INT),
                CAST(variants[3] IS NULL AS INT), LENGTH(CAST(payload['long'] AS STRING))"""
            // Independent expected values also catch a conversion bug shared by both writers.
            def expected = (0..<4097).collect { value ->
                [value, value, value, value, value, value % 2 == 0 ? 1 : 0, 1, 1024]
                    .collect { it.toString() }
            }
            assertEquals(expected, asStrings(sql("SELECT ${projection} FROM t_jni WHERE id < 4097 ORDER BY id")))
            assertEquals(expected, asStrings(sql("SELECT ${projection} FROM t_cpp WHERE id < 4097 ORDER BY id")))
            ["t_cpp", "t_jni"].each { table ->
                spark_paimon "REFRESH TABLE paimon.${db}.${table}"
                def sparkRows = spark_paimon("""SELECT id,
                    try_variant_get(payload, '\$.n', 'bigint'),
                    try_variant_get(variants[0], '\$', 'bigint'),
                    try_variant_get(attrs['n'], '\$', 'bigint'),
                    try_variant_get(nested.payload, '\$', 'bigint'),
                    CAST(secondary IS NULL AS INT), CAST(variants[2] IS NULL AS INT),
                    LENGTH(try_variant_get(payload, '\$.long', 'string'))
                    FROM paimon.${db}.${table} WHERE id < 4097 ORDER BY id""")
                assertEquals(expected, asStrings(sparkRows))
                // Check the full object, including fields not used by the projections above.
                String sample = "id IN (0, 63, 64, 4095, 4096, 5000, 5001, 5002)"
                def expectedObjects = [0, 63, 64, 4095, 4096, 5000, 5001, 5002].collect { id ->
                    def payload = id < 4097
                        ? [n: id, text: "中文😀", nested: [items: [1, null, true]], long: "x" * 1024]
                        : (id == 5000 ? [:] : (id == 5001 ? null : 42))
                    [id.toString(), payload]
                }
                def objects = jsonValues(sql("SELECT id, CAST(payload AS STRING) FROM ${table} WHERE ${sample} ORDER BY id"))
                assertEquals(expectedObjects, objects)
                assertEquals(objects, jsonValues(spark_paimon("""SELECT id, CAST(payload AS STRING)
                    FROM paimon.${db}.${table} WHERE ${sample} ORDER BY id""")))
                assertEquals(objects, jsonValues(sql("SELECT id, CAST(payload AS STRING) FROM t_jni WHERE ${sample} ORDER BY id")))
                String nulls = """id, CAST(payload IS NULL AS INT), CAST(variants IS NULL AS INT),
                    CASE WHEN variants IS NULL THEN NULL ELSE SIZE(variants) END,
                    CAST(attrs IS NULL AS INT), CASE WHEN attrs IS NULL THEN NULL ELSE SIZE(attrs) END,
                    CAST(nested IS NULL AS INT)"""
                assertEquals(asStrings(sql("SELECT ${nulls} FROM ${table} WHERE id >= 5000 ORDER BY id")),
                    asStrings(spark_paimon("SELECT ${nulls} FROM paimon.${db}.${table} WHERE id >= 5000 ORDER BY id")))
            }
            // Append with the other backend to the same table and read old and new files together.
            sql "SET paimon_write_backend='CPP'"
            sql "INSERT INTO t_jni SELECT * FROM t_cpp WHERE id=5000"
            sql "SET paimon_write_backend='JNI'"
            sql "INSERT INTO t_cpp SELECT * FROM t_jni WHERE id=5001"
            assertEquals([[4101L]], sql("SELECT COUNT(*) FROM t_cpp"))
            assertEquals([[4101L]], sql("SELECT COUNT(*) FROM t_jni"))
            spark_paimon "REFRESH TABLE paimon.${db}.t_cpp"
            spark_paimon "REFRESH TABLE paimon.${db}.t_jni"
            assertEquals([["4101"]], asStrings(spark_paimon("SELECT COUNT(*) FROM paimon.${db}.t_cpp")))
            assertEquals([["4101"]], asStrings(spark_paimon("SELECT COUNT(*) FROM paimon.${db}.t_jni")))
            // A failed native prepare must not publish any of this multi-batch Variant append.
            sql "SET paimon_write_backend='CPP'"
            String point = "CppPaimonWriteBackend.prepare.serialize_oom"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(point)
                test {
                    sql "INSERT INTO t_cpp SELECT * FROM t_jni WHERE id < 4097"
                    exception "Paimon write allocation failed"
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(point)
            }
            assertEquals([[4101L]], sql("SELECT COUNT(*) FROM t_cpp"))
            spark_paimon "REFRESH TABLE paimon.${db}.t_cpp"
            assertEquals([["4101"]], asStrings(spark_paimon("SELECT COUNT(*) FROM paimon.${db}.t_cpp")))
            sql "INSERT INTO t_cpp SELECT * FROM t_jni WHERE id=5002"
            assertEquals([[4102L]], sql("SELECT COUNT(*) FROM t_cpp"))
        }
    } finally {
        sql "SET paimon_write_backend='${originalBackend}'"
        sql "SET force_jni_scanner=${originalScanner}"
        sql "SET batch_size=${originalBatchSize}"
        sql "SWITCH internal"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
