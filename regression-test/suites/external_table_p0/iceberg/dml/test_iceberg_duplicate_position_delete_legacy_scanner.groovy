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

suite("test_iceberg_duplicate_position_delete_legacy_scanner",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_duplicate_position_delete_legacy_scanner"
    String dbName = "duplicate_position_delete_db"
    String tableName = "duplicate_position_delete_tbl"

    def executeCommandWithStatus = { String command, int timeoutSeconds = 300,
            boolean logFailure = true, boolean logCommand = true ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        try {
            if (logCommand) {
                logger.info("execute ${command}")
            }
            def process = new ProcessBuilder("/bin/bash", "-c", command).start()
            process.consumeProcessOutput(stdout, stderr)
            process.waitForOrKill(timeoutSeconds * 1000)
            int exitCode = process.exitValue()
            if (exitCode != 0 && logFailure) {
                logger.info("exit code: ${exitCode}, stdout
: ${stdout}
stderr
: ${stderr}")
            }
            return [exitCode: exitCode, stdout: stdout.toString(), stderr: stderr.toString()]
        } catch (IOException e) {
            assertTrue(false, "Execute failed: ${command}, err: ${e.message}")
        }
    }

    def executeCommand = { String command, int timeoutSeconds = 300 ->
        def result = executeCommandWithStatus(command, timeoutSeconds)
        assertEquals(0, result.exitCode,
                "Command failed
stdout:
${result.stdout}
stderr:
${result.stderr}")
        return result.stdout
    }

    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def listDockerContainers = {
        String containers =
                executeCommand("${dockerCommand} ps --format '{{.ID}}	{{.Names}}	{{.Image}}'", 30) ?: ""
        return containers.readLines().collect { it.trim() }.findAll { !it.isEmpty() }
    }

    def findSparkContainer = {
        String configuredContainer = context.config.otherConfigs.get("icebergSparkContainer")
        String probeCommand = "command -v spark-sql >/dev/null && test -f /mnt/SUCCESS && " +
                "test -f /mnt/scripts/java/CreateIcebergDuplicatePositionDeleteFixture.java"
        if (configuredContainer != null && !configuredContainer.isEmpty()) {
            def probe = executeCommandWithStatus(
                    "${dockerCommand} exec ${configuredContainer} bash -lc '${probeCommand}'", 30)
            assertEquals(0, probe.exitCode,
                    "Configured Spark Iceberg container ${configuredContainer} is not usable")
            return configuredContainer
        }

        def matchedContainers = []
        listDockerContainers().each { String containerLine ->
            def fields = containerLine.split(/	/, 3)
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
                matchedContainers.add([id: containerId, name: containerName, image: containerImage])
            }
        }

        assertFalse(matchedContainers.isEmpty(),
                "No usable Spark Iceberg container found. Set icebergSparkContainer or start it.")
        String multipleContainersMessage = "Multiple usable Spark Iceberg containers found: ${matchedContainers}. " +
                "Set icebergSparkContainer to the exact container name."
        assertEquals(1, matchedContainers.size(), multipleContainersMessage)
        logger.info("use Spark Iceberg container ${matchedContainers[0].name} " +
                "(${matchedContainers[0].image})")
        return matchedContainers[0].id
    }

    String sparkContainer = findSparkContainer()
    def runInSparkContainer = { String command, int timeoutSeconds = 300 ->
        executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '${command}'", timeoutSeconds)
    }
    def runSparkSql = { String sqlText, int timeoutSeconds = 600 ->
        String encodedSql = sqlText.getBytes("UTF-8").encodeBase64().toString()
        String sparkSqlCommand = "echo ${encodedSql} | base64 -d >/tmp/test_iceberg_duplicate_position_delete.sql && " +
                "spark-sql --conf spark.sql.session.timeZone=UTC " +
                "-f /tmp/test_iceberg_duplicate_position_delete.sql"
        runInSparkContainer(sparkSqlCommand, timeoutSeconds)
    }
    def longValue = { Object value ->
        return ((Number) value).longValue()
    }
    def enableFileScannerV2Rows = sql """show variables like 'enable_file_scanner_v2'"""
    assertTrue(enableFileScannerV2Rows.size() > 0,
            "Session variable enable_file_scanner_v2 is not found")
    String originalEnableFileScannerV2 = enableFileScannerV2Rows[0][1].toString()

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1',
            'meta.cache.iceberg.table.ttl-second' = '0',
            'meta.cache.iceberg.schema.ttl-second' = '0'
        )
    """

    try {
        runSparkSql("""
            CREATE DATABASE IF NOT EXISTS demo.${dbName};
            USE demo.${dbName};

            DROP TABLE IF EXISTS ${tableName};
            CREATE TABLE ${tableName} (
                id INT
            ) USING iceberg
            TBLPROPERTIES (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.delete.format.default' = 'parquet',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read',
                'write.distribution-mode' = 'none',
                'write.target-file-size-bytes' = '134217728',
                'write.parquet.row-group-size-bytes' = '134217728'
            );

            INSERT INTO ${tableName}
            SELECT /*+ COALESCE(1) */ id FROM VALUES
                (10), (20), (30), (40) AS t(id);
        """)

        String javaFixtureCommand = "javac -cp "/opt/spark/jars/*" " +
                "/mnt/scripts/java/CreateIcebergDuplicatePositionDeleteFixture.java && " +
                "java -cp "/mnt/scripts/java:/opt/spark/jars/*" " +
                "CreateIcebergDuplicatePositionDeleteFixture " +
                "${dbName} ${tableName} 0,0"
        runInSparkContainer(javaFixtureCommand, 300)

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table ${tableName}"""

        List<List<Object>> duplicateDeletes = sql """
                select pos, count(*)
                from ${tableName}\$position_deletes
                group by pos
                order by pos
                """
        assertEquals(1, duplicateDeletes.size())
        assertEquals(0L, longValue(duplicateDeletes[0][0]))
        assertEquals(2L, longValue(duplicateDeletes[0][1]))

        sql """set enable_file_scanner_v2 = true"""
        assertEquals([[20], [30], [40]],
                sql("""select id from ${tableName} order by id"""))

        sql """set enable_file_scanner_v2 = false"""
        sql """delete from ${tableName} where true"""

        sql """refresh table ${tableName}"""
        assertEquals([], sql("""select id from ${tableName} order by id"""))

        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        assertEquals([], spark_iceberg("""
                select id
                from demo.${dbName}.${tableName}
                order by id
                """))

        List<List<Object>> activeDeletePositions = sql """
                select pos, count(*)
                from ${tableName}\$position_deletes
                group by pos
                order by pos
                """
        assertEquals([[0L, 2L], [1L, 1L], [2L, 1L], [3L, 1L]],
                activeDeletePositions.collect { row -> [longValue(row[0]), longValue(row[1])] })
    } finally {
        sql """set enable_file_scanner_v2 = ${originalEnableFileScannerV2}"""
    }
}
