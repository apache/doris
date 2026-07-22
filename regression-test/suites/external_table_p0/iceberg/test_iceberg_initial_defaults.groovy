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

suite("test_iceberg_initial_defaults", "p0,external,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mappedCatalog = "test_iceberg_initial_defaults_mapped"
    String legacyCatalog = "test_iceberg_initial_defaults_legacy"
    String namespace = "format_v3"
    String parquetTable = "initial_defaults_parquet"
    String orcTable = "initial_defaults_orc"

    def createCatalog = { String catalogName, boolean enableMapping ->
        sql """drop catalog if exists ${catalogName}"""
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'rest',
                'uri' = 'http://${externalEnvIp}:${restPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.region' = 'us-east-1',
                's3.path.style.access' = 'true',
                's3.connection.ssl.enabled' = 'false',
                'enable.mapping.varbinary' = '${enableMapping}',
                'enable.mapping.timestamp_tz' = '${enableMapping}'
            )
        """
    }

    createCatalog(mappedCatalog, true)
    createCatalog(legacyCatalog, false)

    def executeCommandWithStatus = { String cmd, int timeoutSeconds = 300,
            Boolean logFailure = true, Boolean logCommand = true ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        try {
            if (logCommand) {
                logger.info("execute ${cmd}")
            }
            def proc = new ProcessBuilder("/bin/bash", "-c", cmd).start()
            proc.consumeProcessOutput(stdout, stderr)
            proc.waitForOrKill(timeoutSeconds * 1000)
            int exitCode = proc.exitValue()
            String output = stdout.toString()
            String error = stderr.toString()
            if (exitCode != 0 && logFailure) {
                logger.info("exit code: ${exitCode}, stdout\n: ${output}\nstderr\n: ${error}")
            }
            return [exitCode: exitCode, stdout: output, stderr: error]
        } catch (IOException e) {
            assertTrue(false, "Execute failed: ${cmd}, err: ${e.message}")
        }
    }

    def executeCommand = { String cmd, Boolean mustSucceed, int timeoutSeconds = 300 ->
        def result = executeCommandWithStatus(cmd, timeoutSeconds)
        if (mustSucceed && result.exitCode != 0) {
            assertTrue(false,
                    "Execute failed: ${cmd}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}")
        }
        return result.stdout
    }

    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def listDockerContainers = {
        String containers = executeCommand(
                "${dockerCommand} ps --format '{{.ID}}\t{{.Names}}\t{{.Image}}'", true, 30) ?: ""
        return containers.readLines().collect { it.trim() }.findAll { !it.isEmpty() }
    }

    def findSparkContainer = {
        String configuredContainer = context.config.otherConfigs.get("icebergSparkContainer")
        String probeCommand = "command -v spark-sql >/dev/null && test -f /mnt/SUCCESS && " +
                "test -f /mnt/scripts/java/CreateIcebergInitialDefaultFixtures.java"
        if (configuredContainer != null && !configuredContainer.isEmpty()) {
            def probe = executeCommandWithStatus(
                    "${dockerCommand} exec ${configuredContainer} bash -lc '${probeCommand}'", 30)
            assertEquals(0, probe.exitCode,
                    "Configured Spark Iceberg container ${configuredContainer} is not usable")
            return configuredContainer
        }

        def matchedContainers = []
        listDockerContainers().each { String containerLine ->
            def fields = containerLine.split(/\t/, 3)
            assertTrue(fields.length >= 2, "Unexpected docker ps output: ${containerLine}")
            String containerId = fields[0].trim()
            String containerName = fields[1].trim()
            String containerImage = fields.length >= 3 ? fields[2].trim() : ""
            def probe = executeCommandWithStatus(
                    "${dockerCommand} exec ${containerId} bash -lc '${probeCommand}'",
                    30,
                    false,
                    false)
            if (probe.exitCode == 0) {
                matchedContainers.add(
                        [id: containerId, name: containerName, image: containerImage])
            }
        }

        assertFalse(matchedContainers.isEmpty(),
                "No usable Spark Iceberg container found. Set icebergSparkContainer or start it.")
        assertEquals(1, matchedContainers.size(),
                "Multiple usable Spark Iceberg containers found: ${matchedContainers}. " +
                        "Set icebergSparkContainer to the exact container name.")
        logger.info("use Spark Iceberg container ${matchedContainers[0].name} " +
                "(${matchedContainers[0].image})")
        return matchedContainers[0].id
    }

    String sparkContainer = findSparkContainer()
    def runInSparkContainer = { String command, int timeoutSeconds = 300 ->
        executeCommand(
                "${dockerCommand} exec ${sparkContainer} bash -lc '${command}'",
                true,
                timeoutSeconds)
    }
    def runSparkSql = { String sqlText, int timeoutSeconds = 600 ->
        String encodedSql = sqlText.getBytes("UTF-8").encodeBase64().toString()
        runInSparkContainer(
                "echo ${encodedSql} | base64 -d >/tmp/test_iceberg_initial_defaults.sql && " +
                        "spark-sql --conf spark.sql.session.timeZone=UTC " +
                        "-f /tmp/test_iceberg_initial_defaults.sql",
                timeoutSeconds)
    }

    String setupSql = """
        CREATE NAMESPACE IF NOT EXISTS demo.${namespace};
        USE demo.${namespace};

        DROP TABLE IF EXISTS ${parquetTable};
        CREATE TABLE ${parquetTable} (
            id INT,
            struct_col STRUCT<existing: STRING>,
            list_col ARRAY<STRUCT<existing: STRING>>,
            map_col MAP<STRING, STRUCT<existing: STRING>>
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.format.default' = 'parquet'
        );

        INSERT INTO ${parquetTable} VALUES
            (1, NULL, NULL, NULL),
            (2,
                named_struct('existing', 'struct-two'),
                array(named_struct('existing', 'list-two')),
                map('present', named_struct('existing', 'map-two'))),
            (3,
                named_struct('existing', CAST(NULL AS STRING)),
                CAST(array() AS ARRAY<STRUCT<existing: STRING>>),
                CAST(map() AS MAP<STRING, STRUCT<existing: STRING>>)),
            (4,
                named_struct('existing', 'struct-four'),
                array(
                    CAST(NULL AS STRUCT<existing: STRING>),
                    named_struct('existing', 'list-four')),
                map_from_arrays(
                    array('nullv', 'present'),
                    array(
                        CAST(NULL AS STRUCT<existing: STRING>),
                        named_struct('existing', 'map-four'))));

        DROP TABLE IF EXISTS ${orcTable};
        CREATE TABLE ${orcTable} (
            id INT,
            struct_col STRUCT<existing: STRING>,
            list_col ARRAY<STRUCT<existing: STRING>>,
            map_col MAP<STRING, STRUCT<existing: STRING>>
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.format.default' = 'orc'
        );

        INSERT INTO ${orcTable} VALUES
            (1, NULL, NULL, NULL),
            (2,
                named_struct('existing', 'struct-two'),
                array(named_struct('existing', 'list-two')),
                map('present', named_struct('existing', 'map-two'))),
            (3,
                named_struct('existing', CAST(NULL AS STRING)),
                CAST(array() AS ARRAY<STRUCT<existing: STRING>>),
                CAST(map() AS MAP<STRING, STRUCT<existing: STRING>>)),
            (4,
                named_struct('existing', 'struct-four'),
                array(
                    CAST(NULL AS STRUCT<existing: STRING>),
                    named_struct('existing', 'list-four')),
                map_from_arrays(
                    array('nullv', 'present'),
                    array(
                        CAST(NULL AS STRUCT<existing: STRING>),
                        named_struct('existing', 'map-four'))));
    """
    runSparkSql(setupSql)

    runInSparkContainer(
            "javac -cp \"/opt/spark/jars/*\" " +
                    "/mnt/scripts/java/CreateIcebergInitialDefaultFixtures.java && " +
                    "java -cp \"/mnt/scripts/java:/opt/spark/jars/*\" " +
                    "CreateIcebergInitialDefaultFixtures " +
                    "${namespace} ${parquetTable} ${orcTable}",
            300)

    // These rows are written after schema evolution. Spark-Iceberg 1.10.1 rejects a partial INSERT
    // column list instead of filling omitted fields, so provide all table columns in schema order.
    // The added fields therefore exist in the physical Parquet/ORC schema: id 5 stores explicit
    // NULLs and id 6 stores explicit non-default values under non-NULL struct/list/map parents.
    // Neither may be replaced by initial-default or write-default.
    String postNullStruct = """
        named_struct(
            'existing', 'post-null-struct',
            'struct_default_boolean', CAST(NULL AS BOOLEAN),
            'struct_default_int', CAST(NULL AS INT),
            'struct_default_long', CAST(NULL AS BIGINT),
            'struct_default_float', CAST(NULL AS FLOAT),
            'struct_default_double', CAST(NULL AS DOUBLE),
            'struct_default_decimal', CAST(NULL AS DECIMAL(20, 4)),
            'struct_default_date', CAST(NULL AS DATE),
            'struct_default_timestamp', CAST(NULL AS TIMESTAMP_NTZ),
            'struct_default_timestamptz', CAST(NULL AS TIMESTAMP),
            'struct_default_string', 'post-null-struct-required',
            'struct_default_uuid', CAST(NULL AS STRING),
            'struct_default_fixed', CAST(NULL AS BINARY),
            'struct_default_binary', CAST(NULL AS BINARY))
    """
    String postValueStruct = """
        named_struct(
            'existing', 'post-value-struct',
            'struct_default_boolean', CAST(NULL AS BOOLEAN),
            'struct_default_int', 706,
            'struct_default_long', CAST(NULL AS BIGINT),
            'struct_default_float', CAST(NULL AS FLOAT),
            'struct_default_double', CAST(NULL AS DOUBLE),
            'struct_default_decimal', CAST(NULL AS DECIMAL(20, 4)),
            'struct_default_date', CAST(NULL AS DATE),
            'struct_default_timestamp', CAST(NULL AS TIMESTAMP_NTZ),
            'struct_default_timestamptz', CAST(NULL AS TIMESTAMP),
            'struct_default_string', 'post-value-struct-required',
            'struct_default_uuid', CAST(NULL AS STRING),
            'struct_default_fixed', CAST(NULL AS BINARY),
            'struct_default_binary', CAST(NULL AS BINARY))
    """
    String postNullList = """
        array(named_struct(
            'existing', 'post-null-list',
            'list_default_int', CAST(NULL AS INT)))
    """
    String postValueList = """
        array(named_struct(
            'existing', 'post-value-list',
            'list_default_int', 701))
    """
    String postNullMap = """
        map('physical', named_struct(
            'existing', 'post-null-map',
            'map_default_int', CAST(NULL AS INT)))
    """
    String postValueMap = """
        map('physical', named_struct(
            'existing', 'post-value-map',
            'map_default_int', 703))
    """
    String postEvolutionSql = """
        USE demo.${namespace};
        INSERT INTO ${parquetTable} VALUES
            (5, ${postNullStruct}, ${postNullList}, ${postNullMap},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'post-null-required', NULL, NULL, NULL, NULL, NULL, NULL),
            (6, ${postValueStruct}, ${postValueList}, ${postValueMap},
                NULL, 606, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'post-value-required', NULL, NULL, NULL, NULL, NULL, NULL);
        INSERT INTO ${orcTable} VALUES
            (5, ${postNullStruct}, ${postNullList}, ${postNullMap},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'post-null-required', NULL, NULL, NULL, NULL, NULL, NULL),
            (6, ${postValueStruct}, ${postValueList}, ${postValueMap},
                NULL, 606, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'post-value-required', NULL, NULL, NULL, NULL, NULL, NULL);
    """
    runSparkSql(postEvolutionSql)

    sql """REFRESH CATALOG ${mappedCatalog}"""
    sql """REFRESH CATALOG ${legacyCatalog}"""
    sql """set time_zone = 'UTC'"""
    sql """set file_split_size = 1"""

    String topLevelProjection = """
        id,
        default_boolean,
        default_int,
        default_long,
        default_float,
        default_double,
        default_decimal,
        CAST(default_date AS STRING),
        CAST(default_timestamp AS STRING),
        CAST(default_timestamptz AS STRING),
        default_string,
        HEX(default_uuid),
        HEX(default_fixed),
        HEX(default_binary)
    """
    String structProjection = """
        id,
        element_at(struct_col, 'struct_default_boolean'),
        element_at(struct_col, 'struct_default_int'),
        element_at(struct_col, 'struct_default_long'),
        element_at(struct_col, 'struct_default_float'),
        element_at(struct_col, 'struct_default_double'),
        element_at(struct_col, 'struct_default_decimal'),
        CAST(element_at(struct_col, 'struct_default_date') AS STRING),
        CAST(element_at(struct_col, 'struct_default_timestamp') AS STRING),
        CAST(element_at(struct_col, 'struct_default_timestamptz') AS STRING),
        element_at(struct_col, 'struct_default_string'),
        HEX(element_at(struct_col, 'struct_default_uuid')),
        HEX(element_at(struct_col, 'struct_default_fixed')),
        HEX(element_at(struct_col, 'struct_default_binary'))
    """
    String complexShapeProjection = """
        id,
        element_at(struct_col, 'struct_default_int'),
        array_size(list_col),
        element_at(list_col[1], 'list_default_int'),
        element_at(list_col[2], 'list_default_int'),
        map_size(map_col),
        element_at(map_col['nullv'], 'map_default_int'),
        element_at(map_col['present'], 'map_default_int')
    """
    String legacyProjection = """
        id,
        HEX(default_uuid),
        HEX(default_fixed),
        HEX(default_binary),
        CAST(default_timestamptz AS STRING)
    """

    def enableFileScannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    assertTrue(enableFileScannerV2Rows.size() > 0,
            "Session variable enable_file_scanner_v2 is not found")
    String originalEnableFileScannerV2 = enableFileScannerV2Rows[0][1].toString()

    def tableNames = [parquet: parquetTable, orc: orcTable]
    def runChecks = { String scannerName, boolean enableFileScannerV2 ->
        sql """set enable_file_scanner_v2 = ${enableFileScannerV2}"""
        tableNames.each { String format, String tableName ->
            String prefix = "${scannerName}_${format}"

            sql """switch ${mappedCatalog}"""
            sql """use ${namespace}"""
            "order_qt_${prefix}_top_level" """
                SELECT ${topLevelProjection}
                FROM ${tableName}
                ORDER BY id
            """
            "order_qt_${prefix}_physical_value_precedence" """
                SELECT
                    id,
                    default_int,
                    default_string,
                    element_at(struct_col, 'struct_default_int'),
                    element_at(list_col[1], 'list_default_int'),
                    element_at(map_col['physical'], 'map_default_int')
                FROM ${tableName}
                WHERE id >= 5
                ORDER BY id
            """
            "order_qt_${prefix}_struct_children" """
                SELECT ${structProjection}
                FROM ${tableName}
                ORDER BY id
            """
            "order_qt_${prefix}_complex_parent_nulls" """
                SELECT ${complexShapeProjection}
                FROM ${tableName}
                ORDER BY id
            """
            "order_qt_${prefix}_whole_missing_complex_parents" """
                SELECT
                    id,
                    missing_struct_col IS NULL,
                    element_at(missing_struct_col, 'missing_struct_default_int'),
                    missing_list_col IS NULL,
                    element_at(missing_list_col[1], 'missing_list_default_int'),
                    missing_map_col IS NULL,
                    element_at(missing_map_col['missing'], 'missing_map_default_int')
                FROM ${tableName}
                WHERE id <= 4
                ORDER BY id
            """
            "order_qt_${prefix}_default_predicate" """
                SELECT id
                FROM ${tableName}
                WHERE default_int = 34
                ORDER BY id
            """
            "order_qt_${prefix}_default_is_null_predicate" """
                SELECT id
                FROM ${tableName}
                WHERE default_int IS NULL
                ORDER BY id
            """
            "order_qt_${prefix}_nested_default_predicate" """
                SELECT id
                FROM ${tableName}
                WHERE element_at(struct_col, 'struct_default_int') = 34
                ORDER BY id
            """

            sql """switch ${legacyCatalog}"""
            sql """use ${namespace}"""
            "order_qt_${prefix}_legacy_mapping" """
                SELECT ${legacyProjection}
                FROM ${tableName}
                WHERE id <= 4
                ORDER BY id
            """
        }
    }

    try {
        runChecks("v1", false)
        runChecks("v2", true)
    } finally {
        sql """set enable_file_scanner_v2 = ${originalEnableFileScannerV2}"""
    }
}
