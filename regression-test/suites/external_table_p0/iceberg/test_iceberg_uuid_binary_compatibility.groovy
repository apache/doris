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

suite("test_iceberg_uuid_binary_compatibility",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def executeCommand = { String command ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        def process = new ProcessBuilder("/bin/bash", "-c", command).start()
        process.consumeProcessOutput(stdout, stderr)
        process.waitForOrKill(300000)
        assertEquals(0, process.exitValue(), "Command failed: ${stderr}\n${stdout}")
        return stdout.toString()
    }
    String sparkContainer = context.config.otherConfigs.get("icebergSparkContainer")
    if (sparkContainer == null || sparkContainer.isEmpty()) {
        def containers = executeCommand("${dockerCommand} ps --format '{{.ID}}'")
        def matches = containers.readLines().findAll { String container ->
            def probe = new ProcessBuilder("/bin/bash", "-c",
                    "${dockerCommand} exec ${container} bash -lc " +
                    "'test -f /mnt/SUCCESS && command -v spark-sql >/dev/null'").start()
            probe.consumeProcessOutput(new StringBuilder(), new StringBuilder())
            probe.waitForOrKill(30000)
            return probe.exitValue() == 0
        }
        assertEquals(1, matches.size(), "Set icebergSparkContainer to the Spark Iceberg container")
        sparkContainer = matches[0]
    }
    String source = new File(context.file.parentFile,
            "uuid_data/CreateIcebergUuidFixtures.java").getText("UTF-8")
    String encoded = source.getBytes("UTF-8").encodeBase64().toString()
    String fixtureDir = "/tmp/doris_uuid_compat_${UUID.randomUUID()}"
    String fixtureOutput = executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '" +
            "mkdir -p ${fixtureDir} && echo ${encoded} | base64 -d >${fixtureDir}/CreateIcebergUuidFixtures.java && " +
            "javac -cp \"/opt/spark/jars/*\" ${fixtureDir}/CreateIcebergUuidFixtures.java && " +
            "java -cp \"${fixtureDir}:/opt/spark/jars/*\" CreateIcebergUuidFixtures uuid_binary_compatibility'")
    def snapshots = [:]
    fixtureOutput.readLines().findAll { it.startsWith("UUID_SNAPSHOT ") }.each { String line ->
        def fields = line.split(" ")
        snapshots[fields[1]] = fields[2]
    }
    assertEquals(4, snapshots.size(), "Iceberg writer/oracle did not create all format combinations")

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // Read the same immutable files through both mappings and scanners, including cached schemas.
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    for (boolean mapping : [false, true]) {
        String catalogName = "uuid_binary_compatibility_${mapping}"
        sql "DROP CATALOG IF EXISTS ${catalogName}"
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type'='iceberg', 'iceberg.catalog.type'='rest',
                'uri'='http://${externalEnvIp}:${restPort}',
                's3.access_key'='admin', 's3.secret_key'='password',
                's3.endpoint'='http://${externalEnvIp}:${minioPort}', 's3.region'='us-east-1',
                'enable.mapping.varbinary'='${mapping}'
            )
        """
        sql "SWITCH ${catalogName}"
        sql "USE uuid_binary_compatibility"
        for (boolean scannerV2 : [false, true]) {
            // V1's equality-delete StringSet cannot consume ColumnVarbinary. Its legacy STRING
            // mapping is the compatibility control; V2 exercises both mappings below.
            if (!scannerV2 && mapping) {
                continue
            }
            sql "SET enable_file_scanner_v2 = ${scannerV2}"
            for (boolean strict : [false, true]) {
                sql "SET enable_strict_cast = ${strict}"
                snapshots.each { String table, String snapshot ->
                    // V1 assumes the data file's format for equality-delete files. Use its
                    // all-Parquet path as the byte-compatibility control; V2 covers every format.
                    if (!scannerV2 && table != "uuid_parquet_parquet") {
                        return
                    }
                    String prefix = "${table}_binary_${mapping}_v2_${scannerV2}_strict_${strict}"
                    // NULL ancestors, missing scalar/nested leaves, explicit NULLs, zero, high-bit,
                    // maximum and ordinary UUIDs must have exactly the same bytes in every mode.
                    "order_qt_${prefix}_before" """
                        SELECT id, HEX(u), HEX(element_at(payload, 'u'))
                        FROM ${table} FOR VERSION AS OF ${snapshot} ORDER BY id
                    """
                    "order_qt_${prefix}_deleted" """
                        SELECT id, HEX(u), HEX(element_at(payload, 'u'))
                        FROM ${table} ORDER BY id
                    """
                    if (scannerV2) {
                        // V1 has a pre-existing synthesized-slot DCHECK when a missing equality
                        // key is hidden. Keep its UUID projection as the compatibility control;
                        // V2 must also apply deletes to hidden-key projections and COUNT pushdown.
                        "order_qt_${prefix}_ids" "SELECT id FROM ${table} ORDER BY id"
                        "qt_${prefix}_count" "SELECT COUNT(*) FROM ${table}"
                    }
                    // VARBINARY does not support SQL comparison predicates; compare its bytes
                    // through HEX. The STRING mapping exercises the direct equality predicate.
                    String predicate = mapping
                            ? "HEX(u) = '80000000000000000000000000000000'"
                            : "u = UNHEX('80000000000000000000000000000000')"
                    "order_qt_${prefix}_predicate" """
                        SELECT id FROM ${table}
                        WHERE ${predicate} ORDER BY id
                    """
                }
            }
        }
    }
}
