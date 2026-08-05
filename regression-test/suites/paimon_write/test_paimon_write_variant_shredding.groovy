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

suite("test_paimon_write_variant_shredding", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_variant_shredding_catalog"
    String dbName = "test_pw_variant_shredding_db"
    String shreddingSchema =
            '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[' +
            '{"name":"age","type":"INT"},' +
            '{"name":"city","type":"STRING"},' +
            '{"name":"active","type":"BOOLEAN"},' +
            '{"name":"profile","type":{"type":"ROW","fields":[' +
            '{"name":"name","type":"STRING"},' +
            '{"name":"scores","type":{"type":"ARRAY","element":"INT"}}' +
            ']}}]}}]}'

    // TODO: Use variant.shreddingSchema after Paimon passes the global option to its Parquet
    // builder. In 1.4.2 the builder still requires the fallback spelling used below.
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_shredded;
        CREATE TABLE paimon.${dbName}.t_variant_shredded (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'write-only' = 'true',
            'parquet.variant.shreddingSchema' = '${shreddingSchema}',
            'variant.inferShreddingSchema' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_inferred;
        CREATE TABLE paimon.${dbName}.t_variant_inferred (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'write-only' = 'true',
            'variant.inferShreddingSchema' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_variant_mixed;
        CREATE TABLE paimon.${dbName}.t_variant_mixed (
            id INT,
            payload VARIANT
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet',
            'write-only' = 'true'
        );
    """

    def createDorisCatalog = {
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
    }

    def sparkValues = { rows ->
        rows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }

    String filesTableSuffix = '$files'
    def dataFiles = { String tableName ->
        String filesQuery = """
            SELECT file_path
            FROM paimon.${dbName}.`${tableName}${filesTableSuffix}`
            ORDER BY file_path
        """
        spark_paimon(filesQuery).collect { row -> row[0].toString() }
    }

    // Read the data file as ordinary Parquet, bypassing Paimon's logical Variant reader. This
    // proves that shredding produced typed_value columns rather than merely round-tripping the
    // original value/metadata pair.
    def rawParquetSource = { String path ->
        return """S3(
            "uri" = "${path}",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true",
            "format" = "parquet"
        )"""
    }
    def rawPayloadType = { String path ->
        def columns = sql """DESC FUNCTION ${rawParquetSource(path)}"""
        def payloadColumn = columns.find { it[0].toString().equalsIgnoreCase("payload") }
        assertTrue(payloadColumn != null, "No payload column in Paimon data file ${path}")
        return payloadColumn[1].toString().toLowerCase()
    }

    createDorisCatalog()
    try {
        // Cover typed fields, residual object fields, type mismatch fallback, nested ROW/ARRAY,
        // root scalars, empty objects, Variant null, and SQL null.
        sql """
            INSERT INTO t_variant_shredded VALUES
                (1, parse_to_variant('{"age":27,"city":"Beijing","active":true,"profile":{"name":"alice","scores":[10,20]},"other":"kept"}')),
                (2, parse_to_variant('{"age":28}')),
                (3, parse_to_variant('{"age":"29","active":"true"}')),
                (4, parse_to_variant('"scalar"')),
                (5, parse_to_variant('{}')),
                (6, parse_to_variant('null')),
                (7, CAST(NULL AS VARIANT)),
                (8, parse_to_variant('{"profile":{"name":"bob","scores":[30],"extra":"nested-kept"},"other":"root-kept"}'))
        """

        // Doris's Paimon reader must unshred typed and residual components back into one logical
        // Variant value.
        order_qt_variant_explicit_shredding """
            SELECT id,
                   CAST(payload['age'] AS STRING),
                   CAST(payload['city'] AS STRING),
                   CAST(payload['active'] AS BOOLEAN),
                   CAST(payload['profile']['name'] AS STRING),
                   CAST(payload['profile']['scores'][1] AS INT),
                   CAST(payload['profile']['extra'] AS STRING),
                   CAST(payload['other'] AS STRING),
                   payload IS NULL
            FROM t_variant_shredded
            ORDER BY id
        """

        def shreddedFiles = dataFiles("t_variant_shredded")
        assertTrue(!shreddedFiles.isEmpty())
        def physicalRows = []
        shreddedFiles.each { filePath ->
            String payloadType = rawPayloadType(filePath)
            assertTrue(payloadType.contains("metadata:text"))
            assertTrue(payloadType.contains("value:text"))
            assertTrue(payloadType.contains("typed_value:struct"))
            assertTrue(payloadType.contains("age:struct"))
            assertTrue(payloadType.contains("profile:struct"))
            // The explicit schema wins over inference; residual-only fields must not be promoted.
            assertFalse(payloadType.contains("other:struct"))

            physicalRows.addAll(sql("""
                SELECT id,
                       payload.typed_value.age.typed_value,
                       CAST(payload.typed_value.age.value IS NOT NULL AS INT),
                       payload.typed_value.city.typed_value,
                       CAST(payload.typed_value.active.typed_value AS INT),
                       payload.typed_value.profile.typed_value.name.typed_value,
                       payload.typed_value.profile.typed_value.scores.typed_value[1].typed_value,
                       CAST(payload.value IS NOT NULL AS INT),
                       CAST(payload.metadata IS NOT NULL AS INT)
                FROM ${rawParquetSource(filePath)}
                WHERE id IN (1, 3)
            """))
        }
        physicalRows.sort { left, right ->
            Integer.parseInt(left[0].toString()) <=> Integer.parseInt(right[0].toString())
        }
        assertEquals([
                ["1", "27", "0", "Beijing", "1", "alice", "10", "1", "1"],
                ["3", null, "1", null, null, null, null, "0", "1"]
        ], sparkValues(physicalRows))

        // First create ordinary value/metadata files, then enable shredding for the same table.
        // Paimon's reader detects the physical schema per file and must read both layouts together.
        sql """
            INSERT INTO t_variant_mixed VALUES
                (100, parse_to_variant('{"age":100,"city":"old"}')),
                (101, parse_to_variant('{"legacy":"residual"}'))
        """
        def unshreddedFiles = dataFiles("t_variant_mixed")
        assertTrue(!unshreddedFiles.isEmpty())
        unshreddedFiles.each { filePath ->
            String payloadType = rawPayloadType(filePath)
            assertTrue(payloadType.contains("value:text"))
            assertTrue(payloadType.contains("metadata:text"))
            assertFalse(payloadType.contains("typed_value"))
        }

        spark_paimon """
            ALTER TABLE paimon.${dbName}.t_variant_mixed
            SET TBLPROPERTIES ('parquet.variant.shreddingSchema' = '${shreddingSchema}')
        """
        // Reload the serialized Paimon table used by the JNI writer so the next write observes
        // the new file-format option.
        createDorisCatalog()
        sql """
            INSERT INTO t_variant_mixed VALUES
                (200, parse_to_variant('{"age":200,"city":"new","extra":"kept"}')),
                (201, parse_to_variant('{"age":"201"}'))
        """

        def mixedFiles = dataFiles("t_variant_mixed")
        def newlyShreddedFiles = mixedFiles.findAll { !unshreddedFiles.contains(it) }
        assertTrue(!newlyShreddedFiles.isEmpty())
        newlyShreddedFiles.each { filePath ->
            assertTrue(rawPayloadType(filePath).contains("typed_value:struct"))
        }

        order_qt_variant_mixed_layout """
            SELECT id,
                   CAST(payload['age'] AS STRING),
                   CAST(payload['city'] AS STRING),
                   CAST(payload['legacy'] AS STRING),
                   CAST(payload['extra'] AS STRING)
            FROM t_variant_mixed
            ORDER BY id
        """

        // Paimon 1.4 can infer one shredding schema per file writer. Doris still sends the same
        // logical value/metadata pair; the SDK buffers the rows, chooses typed fields, and writes
        // typed_value without a caller-provided schema.
        sql """
            INSERT INTO t_variant_inferred VALUES
                (300, parse_to_variant('{"age":30,"profile":{"name":"alice"},"extra":"first"}')),
                (301, parse_to_variant('{"age":31,"profile":{"name":"bob"},"extra":"second"}'))
        """
        def firstInferredFiles = dataFiles("t_variant_inferred")
        assertTrue(!firstInferredFiles.isEmpty())
        firstInferredFiles.each { filePath ->
            String payloadType = rawPayloadType(filePath)
            assertTrue(payloadType.contains("metadata:text"))
            assertTrue(payloadType.contains("value:text"))
            assertTrue(payloadType.contains("typed_value:struct"))
            assertTrue(payloadType.contains("age:struct"))
            assertTrue(payloadType.contains("profile:struct"))
            assertFalse(payloadType.contains("active:struct"))
            assertFalse(payloadType.contains("city:struct"))
        }

        // A later Doris statement opens a new Paimon file writer and may infer a different schema.
        // Keep write-only enabled so compaction cannot hide the per-file schema difference.
        sql """
            INSERT INTO t_variant_inferred VALUES
                (400, parse_to_variant('{"city":"Hangzhou","active":true,"extra":"third"}')),
                (401, parse_to_variant('{"city":"Shanghai","active":false,"extra":"fourth"}'))
        """
        def allInferredFiles = dataFiles("t_variant_inferred")
        def secondInferredFiles = allInferredFiles.findAll { !firstInferredFiles.contains(it) }
        assertTrue(!secondInferredFiles.isEmpty())
        secondInferredFiles.each { filePath ->
            String payloadType = rawPayloadType(filePath)
            assertTrue(payloadType.contains("typed_value:struct"))
            assertTrue(payloadType.contains("active:struct"))
            assertTrue(payloadType.contains("city:struct"))
            assertFalse(payloadType.contains("age:struct"))
            assertFalse(payloadType.contains("profile:struct"))
        }

        // Unshredding is a reader responsibility. It must use each file's physical schema and
        // combine typed fields with residual values into one logical Variant column.
        order_qt_variant_inferred_shredding """
            SELECT id,
                   CAST(payload['age'] AS INT),
                   CAST(payload['profile']['name'] AS STRING),
                   CAST(payload['city'] AS STRING),
                   CAST(payload['active'] AS BOOLEAN),
                   CAST(payload['extra'] AS STRING)
            FROM t_variant_inferred
            ORDER BY id
        """
    } finally {
        sql """SET force_jni_scanner = false"""
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
