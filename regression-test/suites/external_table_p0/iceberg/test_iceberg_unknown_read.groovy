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

suite("test_iceberg_unknown_read", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql "drop catalog if exists test_iceberg_unknown_read"
    sql """
        CREATE CATALOG test_iceberg_unknown_read PROPERTIES (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1',
            's3.path.style.access' = 'true',
            's3.connection.ssl.enabled' = 'false'
        )
    """
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
                "test -f /mnt/scripts/java/CreateIcebergUnknownFixtures.java"
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

    // Generate fixtures at test runtime. The source is mounted by the existing Iceberg Docker setup.
    runInSparkContainer(
            "javac -cp \"/opt/spark/jars/*\" /mnt/scripts/java/CreateIcebergUnknownFixtures.java && " +
                    "java -cp \"/mnt/scripts/java:/opt/spark/jars/*\" CreateIcebergUnknownFixtures create format_v3")
    sql "switch test_iceberg_unknown_read"
    sql "use format_v3"
    // Exercise more than one scan batch, including a projection with no physical output column.
    sql "set batch_size = 2"

    ["parquet", "orc"].each { String format ->
        ["read", "physical"].each { String kind ->
            String table = "unknown_${kind}_${format}"
            "order_qt_${format}_${kind}_unknown_rows" "select * from ${table} order by id"
            "order_qt_${format}_${kind}_unknown_only" "select u from ${table} order by id"
            "qt_${format}_${kind}_unknown_count" "select count(*), count(u) from ${table}"
            "order_qt_${format}_${kind}_unknown_is_null" "select id from ${table} where u is null order by id"
            "order_qt_${format}_${kind}_unknown_null_safe" "select id from ${table} where u <=> null order by id"
            "qt_${format}_${kind}_unknown_is_not_null" "select count(*) from ${table} where u is not null"
            "order_qt_${format}_${kind}_unknown_nested" """
                select id, struct_element(s, 'u'),
                       struct_element(element_at(a, 1), 'u'),
                       struct_element(element_at(m, 'key'), 'u'),
                       s is null, a is null, m is null, array_size(a), map_size(m),
                       array_size(null_array), map_size(null_map), element_at(null_array, 1),
                       element_at(null_map, 'key')
                from ${table} order by id
            """
            "order_qt_${format}_${kind}_unknown_desc" "desc ${table}"
            // SHOW CREATE contains a remote fixture location; record only the relevant type clauses.
            def create = sql "show create table ${table}"
            def typeLines = create[0][1].readLines().findAll { it.trim().startsWith('`') && it.contains('unknown') }
            "qt_${format}_${kind}_unknown_show_create" "select '${typeLines.join(' ').replace("'", "''")}'"
            [
                "insert into ${table}(id) values (9)",
                "insert overwrite table ${table} select * from ${table}",
                "update ${table} set id = 9 where id = 1",
                "delete from ${table} where id = 1",
                "delete from ${table}",
                "merge into ${table} t using (select 1 id) s on t.id = s.id when matched then delete",
                "alter table ${table} execute rewrite_data_files()"
            ].each { String statement ->
                test {
                    sql statement
                    exception "unknown type columns"
                }
            }
        }
        "qt_${format}_unknown_empty" "select count(*), count(u) from unknown_empty_${format}"
    }
    sql "drop table if exists test_iceberg_unknown_create"
    test {
        sql "create table test_iceberg_unknown_create (u unknown)"
        exception "unknown"
    }

    Map<String, Long> oldSnapshots = [:]
    ["parquet", "orc"].each { String format ->
        oldSnapshots[format] = (sql "select snapshot_id from unknown_read_${format}\$snapshots")[0][0] as Long
    }
    runInSparkContainer(
            "java -cp \"/mnt/scripts/java:/opt/spark/jars/*\" CreateIcebergUnknownFixtures evolve format_v3")
    ["parquet", "orc"].each { String format ->
        sql "refresh table unknown_read_${format}"
        "order_qt_${format}_unknown_promoted" "select id, u from unknown_read_${format} order by id"
        "order_qt_${format}_unknown_promoted_desc" "desc unknown_read_${format}"
        "order_qt_${format}_unknown_historical" """
            select id, u from unknown_read_${format}
            for version as of ${oldSnapshots[format]} order by id
        """
    }
}
