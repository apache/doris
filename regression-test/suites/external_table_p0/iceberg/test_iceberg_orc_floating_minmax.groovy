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

suite("test_iceberg_orc_floating_minmax", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String sdk = context.config.otherConfigs.get("icebergFixtureSdkDir")
    String warehouse = context.config.otherConfigs.get("icebergFixtureWarehouse")
    boolean local = sdk != null && warehouse != null
    assertTrue((sdk == null) == (warehouse == null), "Set both local fixture SDK and warehouse")
    if (!local && !"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    def execute = { List<String> command ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        def process = new ProcessBuilder(command).start()
        process.consumeProcessOutput(stdout, stderr)
        process.waitForOrKill(300000)
        assertEquals(0, process.exitValue(), "SDK command failed: ${stderr}\n${stdout}")
        return stdout.toString()
    }
    def shellQuote = { String value -> "'" + value.replace("'", "'\"'\"'") + "'" }
    File source = new File(context.file.parentFile, "orc_data/CreateOrcFloatingStatsFixtures.java")
    String fixtureOutput
    if (local) {
        assertTrue(warehouse.startsWith("file:"), "Local SDK mode requires a shared file warehouse")
        File classes = new File(context.file.parentFile, ".orc_stats_${UUID.randomUUID()}")
        assertTrue(classes.mkdirs())
        try {
            String bin = System.getProperty("java.home") + "/bin/"
            execute([bin + "javac", "-cp", sdk + "/*", "-d", classes.absolutePath, source.absolutePath])
            fixtureOutput = execute([bin + "java", "-cp", classes.absolutePath + ":" + sdk + "/*",
                    "CreateOrcFloatingStatsFixtures", "orc_floating_minmax", warehouse])
        } finally {
            assertTrue(classes.deleteDir())
        }
    } else {
        String docker = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
        String container = context.config.otherConfigs.get("icebergSparkContainer")
        if (container == null || container.isEmpty()) {
            def candidates = execute(["bash", "-c", "${docker} ps --format '{{.ID}}'"]).readLines()
            def matches = candidates.findAll { String candidate ->
                def probe = new ProcessBuilder("bash", "-c", "${docker} exec ${shellQuote(candidate)} " +
                        "bash -lc 'test -f /mnt/SUCCESS && command -v spark-sql >/dev/null'").start()
                probe.consumeProcessOutput(new StringBuilder(), new StringBuilder())
                probe.waitForOrKill(30000)
                return probe.exitValue() == 0
            }
            assertEquals(1, matches.size(), "Set icebergSparkContainer to the Spark Iceberg container")
            container = matches[0]
        }
        String directory = "/tmp/doris_orc_stats_${UUID.randomUUID()}"
        String encoded = source.getText("UTF-8").getBytes("UTF-8").encodeBase64().toString()
        String command = "mkdir -p ${directory} && echo ${encoded} | base64 -d > ${directory}/${source.name} && " +
                "javac -cp \"/opt/spark/jars/*\" -d ${directory} ${directory}/${source.name} && " +
                "java -cp \"${directory}:/opt/spark/jars/*\" CreateOrcFloatingStatsFixtures orc_floating_minmax"
        try {
            fixtureOutput = execute(["bash", "-c",
                    "${docker} exec ${shellQuote(container)} bash -lc ${shellQuote(command)}"])
        } finally {
            execute(["bash", "-c", "${docker} exec ${shellQuote(container)} rm -rf ${shellQuote(directory)}"])
        }
    }
    def cases = ["nan_middle", "nan_last", "nan_first", "all_nan", "nullable_nan", "finite", "all_null",
            "positive_inf", "negative_inf", "opposite_inf", "double_overflow", "float_nan_only", "double_nan_only", "empty"]
    def authored = fixtureOutput.readLines().findAll { it.startsWith("ORC_STATS_FIXTURE ") }
            .collect { it.split(" ")[1] }
    assertEquals(cases.toSet(), authored.toSet(), "The SDK must write and read back every fixture")
    assertEquals(cases.size(), authored.size())

    String originalSqlCache = (sql "SHOW VARIABLES LIKE 'enable_sql_cache'")[0][1].toString()
    String originalQueryCache = (sql "SHOW VARIABLES LIKE 'enable_query_cache'")[0][1].toString()
    try {
        sql "SET enable_sql_cache=false"
        sql "SET enable_query_cache=false"
        sql "DROP CATALOG IF EXISTS orc_floating_minmax_catalog"
        if (local) {
            sql """CREATE CATALOG orc_floating_minmax_catalog PROPERTIES (
                    'type'='iceberg', 'iceberg.catalog.type'='hadoop',
                    'warehouse'='${warehouse.replace("'", "''")}')"""
        } else {
            String host = context.config.otherConfigs.get("externalEnvIp")
            String rest = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio = context.config.otherConfigs.get("iceberg_minio_port")
            sql """CREATE CATALOG orc_floating_minmax_catalog PROPERTIES (
                    'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${rest}',
                    's3.access_key'='admin', 's3.secret_key'='password',
                    's3.endpoint'='http://${host}:${minio}', 's3.region'='us-east-1')"""
        }
        for (String name : cases) {
            String table = "orc_floating_minmax_catalog.orc_floating_minmax.${name}"
            // Compare textual values and NaN flags: fuzzy floating comparison can hide a NaN mismatch.
            String expressions = "CAST(min(f) AS STRING), CAST(max(f) AS STRING), isnan(max(f)), " +
                    "CAST(min(d) AS STRING), CAST(max(d) AS STRING), isnan(max(d))"
            String pushed = "SELECT /*+ SET_VAR(enable_push_down_no_group_agg=true,enable_file_scanner_v2=true) */ ${expressions} FROM ${table}"
            String scanned = "SELECT /*+ SET_VAR(enable_push_down_no_group_agg=false,enable_file_scanner_v2=true) */ ${expressions} FROM ${table}"
            if (name != "empty") {
                explain {
                    sql pushed
                    contains "pushdown agg=MINMAX"
                }
            }
            quickTest("${name}_matches_scan", pushed, scanned)
            "qt_${name}" pushed
            "qt_${name}_counts" "SELECT count(*), count(f), count(d) FROM ${table}"
        }
    } finally {
        sql "SET enable_sql_cache=${originalSqlCache}"
        sql "SET enable_query_cache=${originalQueryCache}"
    }
}
