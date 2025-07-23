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

suite("test_warm_up_same_table_multi_times") {
    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(custoBeConfig) {
    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    sql """use @${validCluster};""";

    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="12000") """
    def getJobState = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0]
    }

    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("${validCluster}")) {
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

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
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



    sql "use @${validCluster}"

    def table = "customer"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    // create table if not exists
    sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
    sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction

    sleep(10000)

    def load_customer_once =  {
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
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

    clearFileCache.call();
    sleep(30000)

    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()

    def jobId = sql "warm up cluster ${validCluster} with table customer;"
    try {
        sql "warm up cluster ${validCluster} with table customer;"
        assertTrue(true) // dup warm up command can be send to fe queue now
    } catch (Exception e) {
        assertTrue(false)
    }
    int retryTime = 120
    int j = 0
    for (; j < retryTime; j++) {
        sleep(1000)
        def statuses = getJobState(jobId[0][0])
        if (statuses.any { it != null && it.equals("CANCELLED") }) {
            assertTrue(false);
        }
        if (statuses.any { it != null && it.equals("FINISHED") }) {
            break;
        }
    }
    if (j == retryTime) {
        sql "cancel warm up job where id = ${jobId[0][0]}"
        assertTrue(false);
    }
    sleep(30000)
    long ttl_cache_size = 0
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    long skip_io_bytes_start = 0;
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("cached_remote_reader_skip_local_cache_io_sum_bytes")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    skip_io_bytes_start = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    // AGAIN!
    jobId = sql "warm up cluster ${validCluster} with table customer;"

    retryTime = 120
    j = 0
    for (; j < retryTime; j++) {
        sleep(1000)
        def statuses = getJobState(jobId[0][0])
        if (statuses.any { it != null && it.equals("CANCELLED") }) {
            assertTrue(false);
        }
        if (statuses.any { it != null && it.equals("FINISHED") }) {
            break;
        }
    }
    if (j == retryTime) {
        sql "cancel warm up job where id = ${jobId[0][0]}"
        assertTrue(false);
    }
    sleep(30000)
    ttl_cache_size = 0
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    assertEquals(ttl_cache_size, line.substring(i).toLong())
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    long skip_io_bytes_end = 0;
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("cached_remote_reader_skip_local_cache_io_sum_bytes")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    skip_io_bytes_end = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
    long diff = skip_io_bytes_end - skip_io_bytes_start;
    println("skip_io_bytes diff: " + diff);
    assertTrue(diff > 1000);
    }
}
