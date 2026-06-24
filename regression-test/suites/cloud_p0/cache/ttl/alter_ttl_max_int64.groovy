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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_ttl_max_int64") {
    sql """ use @regression_cluster_name1 """
    long currentUnixSeconds = Math.floorDiv(System.currentTimeMillis(), 1000L)
    long overflowGuardSeconds = 86400L
    long safeLargeTtlSeconds = Long.MAX_VALUE - currentUnixSeconds - overflowGuardSeconds
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="${safeLargeTtlSeconds}") """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)

    backendId = backendIdToBackendIP.keySet()[0]
    def url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/file_cache?op=clear&sync=true"""
    logger.info(url)
    def clearFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri url
            op "get"
            body ""
            check check_func
        }
    }

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()


    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
        def loadLabel = table + "_" + uniqueID
        sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    def getTabletIds = { String table ->
        def tablets = sql "show tablets from ${table}"
        assertTrue(tablets.size() > 0, "No tablets found for table ${table}")
        tablets.collect { it[0] as Long }
    }

    def waitForFileCacheType = { List<Long> tabletIds, String expectedType, long timeoutMs = 60000L, long intervalMs = 2000L ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            boolean allMatch = true
            for (Long tabletId in tabletIds) {
                def rows = sql "select type from information_schema.file_cache_info where tablet_id = ${tabletId}"
                if (rows.isEmpty()) {
                    logger.warn("file_cache_info is empty for tablet ${tabletId} while waiting for ${expectedType}")
                    allMatch = false
                    break
                }
                def mismatch = rows.find { row -> !row[0]?.toString()?.equalsIgnoreCase(expectedType) }
                if (mismatch) {
                    logger.info("tablet ${tabletId} has cache types ${rows.collect { it[0] }} while waiting for ${expectedType}")
                    allMatch = false
                    break
                }
            }
            if (allMatch) {
                logger.info("All file cache entries for tablets ${tabletIds} are ${expectedType}")
                return
            }
            sleep(intervalMs)
        }
        assertTrue(false, "Timeout waiting for file_cache_info type ${expectedType} for tablets ${tabletIds}")
    }

    clearFileCache.call() {
        respCode, body -> {}
    }
    sleep(30000)

    load_customer_once("customer_ttl")
    def tabletIds = getTabletIds.call("customer_ttl")
    waitForFileCacheType.call(tabletIds, "ttl")
    sleep(30000) // 30s
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            for (String line in strs) {
                if (flag1) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    assertTrue(line.substring(i).trim().toLong() > 838860800)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }
}
