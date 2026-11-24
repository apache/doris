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

import org.apache.doris.regression.action.ProfileAction

suite("test_iceberg_nested_predicate_cache", "p0,external,iceberg,external_docker,external_docker_iceberg") {
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
        // List<String> can still contain GStrings; ProcessBuilder copies its elements into String[].
        def process = new ProcessBuilder(command.collect { it.toString() }).start()
        Thread outputReader = process.consumeProcessOutputStream(stdout)
        Thread errorReader = process.consumeProcessErrorStream(stderr)
        process.waitForOrKill(300000)
        // Process exit can precede output-consumer completion; read every SDK marker before checking.
        outputReader.join()
        errorReader.join()
        assertEquals(0, process.exitValue(), "SDK command failed: ${stderr}\n${stdout}")
        return stdout.toString()
    }
    def shellQuote = { String value -> "'" + value.replace("'", "'\"'\"'") + "'" }
    File source = new File(context.file.parentFile, "cache_data/CreateNestedPredicateCacheFixture.java")
    File classes
    String container
    String directory = "/tmp/doris_nested_cache_${UUID.randomUUID()}"
    String docker = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def sdkRun = { String phase ->
        if (local) {
            String bin = System.getProperty("java.home") + "/bin/"
            return execute([bin + "java", "-cp", classes.absolutePath + ":" + sdk + "/*",
                    "CreateNestedPredicateCacheFixture", phase, "nested_predicate_cache", warehouse])
        }
        String command = "java -cp \"${directory}:/opt/spark/jars/*\" " +
                "CreateNestedPredicateCacheFixture ${phase} nested_predicate_cache"
        return execute(["bash", "-c", "${docker} exec ${shellQuote(container)} bash -lc ${shellQuote(command)}"])
    }
    def settings = ["enable_sql_cache", "enable_query_cache", "enable_condition_cache", "enable_profile", "profile_level", "enable_file_scanner_v2"]
            .collectEntries { name -> [(name): (sql "SELECT @@${name}")[0][0].toString()] }
    try {
        if (local) {
            assertTrue(warehouse.startsWith("file:"), "Local SDK mode requires a shared file warehouse")
            classes = new File(context.file.parentFile, ".nested_cache_${UUID.randomUUID()}")
            assertTrue(classes.mkdirs())
            execute([System.getProperty("java.home") + "/bin/javac", "-cp", sdk + "/*", "-d",
                    classes.absolutePath, source.absolutePath])
        } else {
            container = context.config.otherConfigs.get("icebergSparkContainer")
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
            String encoded = source.getText("UTF-8").getBytes("UTF-8").encodeBase64().toString()
            String command = "mkdir -p ${directory} && echo ${encoded} | base64 -d > ${directory}/${source.name} && " +
                    "javac -cp \"/opt/spark/jars/*\" -d ${directory} ${directory}/${source.name}"
            execute(["bash", "-c", "${docker} exec ${shellQuote(container)} bash -lc ${shellQuote(command)}"])
        }
        assertTrue(sdkRun("create").contains("CACHE_FIXTURE rows=8192 x=0"))
        sql "DROP CATALOG IF EXISTS nested_predicate_cache_catalog"
        if (local) {
            sql """CREATE CATALOG nested_predicate_cache_catalog PROPERTIES (
                    'type'='iceberg', 'iceberg.catalog.type'='hadoop',
                    'warehouse'='${warehouse.replace("'", "''")}')"""
        } else {
            String host = context.config.otherConfigs.get("externalEnvIp")
            String rest = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio = context.config.otherConfigs.get("iceberg_minio_port")
            sql """CREATE CATALOG nested_predicate_cache_catalog PROPERTIES (
                    'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${rest}',
                    's3.access_key'='admin', 's3.secret_key'='password',
                    's3.endpoint'='http://${host}:${minio}', 's3.region'='us-east-1')"""
        }
        sql "SET enable_sql_cache=false"
        sql "SET enable_query_cache=false"
        sql "SET enable_condition_cache=true"
        sql "SET enable_file_scanner_v2=true"
        sql "SET profile_level=2"
        def profileAction = new ProfileAction(context)
        def assertCacheHit = { String token, boolean expectPruning ->
            String profile = profileAction.getProfileBySql(token, ["ConditionCacheHit", "ConditionCacheFilteredRows"])
            def hits = profile =~ /ConditionCacheHit:\s*([0-9,]+)/
            assertTrue(hits.findAll().any { Long.parseLong(it[1].replace(",", "")) > 0 },
                    "No positive condition-cache hit; the regression must exercise cache reuse: ${profile}")
            if (expectPruning) {
                def filtered = profile =~ /ConditionCacheFilteredRows:\s*([0-9,]+)/
                assertTrue(filtered.findAll().any { Long.parseLong(it[1].replace(",", "")) > 0 },
                        "The Q-only bitmap must still prune nonmatching granules: ${profile}")
            }
        }
        String table = "nested_predicate_cache_catalog.nested_predicate_cache.nested_default"
        String predicate = "mod(element_at(s,'x'),2)=1 AND mod(k,3)=1"
        String warmToken = "nested_cache_warm_${UUID.randomUUID()}"
        // Project the whole parent: its field ID must stay stable while the child's ID changes.
        String warm = "SELECT /* ${warmToken} */ s,k FROM ${table} WHERE ${predicate}"
        sql "SET enable_profile=false"
        5.times { sql warm }
        sql "SET enable_profile=true"
        sql warm
        assertCacheHit(warmToken, true)
        qt_original_rows "SELECT count(*) FROM ${table} WHERE ${predicate}"

        String evolved = sdkRun("evolve")
        assertTrue(evolved.contains("CACHE_FIXTURE rows=8192 x=9"))
        assertTrue(evolved.contains("CACHE_EVOLVED root=1 old=3 new=5"))
        sql "REFRESH TABLE ${table}"
        String evolvedToken = "nested_cache_evolved_${UUID.randomUUID()}"
        String query = "SELECT /* ${evolvedToken} */ s,k FROM ${table} WHERE ${predicate}"
        sql "SET enable_profile=false"
        sql "SET enable_condition_cache=false"
        def expected = sql(query)
        qt_evolved_defaults """SELECT element_at(s,'x'),element_at(s,'keep'),k,count(*)
                FROM ${table} WHERE ${predicate} GROUP BY 1,2,3 ORDER BY 1,2,3"""
        qt_null_parents "SELECT count(*) FROM ${table} WHERE s IS NULL"
        sql "SET enable_condition_cache=true"
        def first = sql(query)
        assertTrue(expected == first, "A Q-only request reused a P AND Q bitmap: " +
                "expected ${expected.size()} rows, got ${first.size()}; sample ${first.take(3)}")
        sql "SET enable_profile=true"
        def repeated = sql(query)
        assertTrue(expected == repeated, "Cached rows differ from the full scan: " +
                "expected ${expected.size()} rows, got ${repeated.size()}; sample ${repeated.take(3)}")
        assertCacheHit(evolvedToken, true)
    } finally {
        settings.each { name, value -> sql "SET ${name}=${value}" }
        if (local && classes != null) {
            assertTrue(classes.deleteDir())
        } else if (container != null) {
            execute(["bash", "-c", "${docker} exec ${shellQuote(container)} rm -rf ${shellQuote(directory)}"])
        }
    }
}
