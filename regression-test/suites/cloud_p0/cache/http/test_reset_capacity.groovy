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

// 1. clear file cache
// 2. load 19.5G ttl table data into cache (cache capacity is 20G)
// 3. check ttl size and total size
// 4. load 1.3G normal table data into cache (just little datas will be cached)
// 5. select some data from normal table, and it will read from s3
// 6. select some data from ttl table, and it will not read from s3
// 7. wait for ttl data timeout
// 8. drop the normal table and load again. All normal table datas will be cached this time.
// 9. select some data from normal table to check whether all datas are cached
suite("test_reset_capacity") {
    sql """ use @regression_cluster_name1 """
    sql """ set global enable_auto_analyze = false; """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="4200") """
    //doris show backends: BackendId  Host  HeartbeatPort  BePort  HttpPort  BrpcPort  ArrowFlightSqlPort  LastStartTime  LastHeartbeat  Alive  SystemDecommissioned  TabletNum  DataUsedCapacity  TrashUsedCapcacity  AvailCapacity  TotalCapacity  UsedPct  MaxDiskUsedPct  RemoteUsedCapacity  Tag  ErrMsg  Version  Status  HeartbeatFailureCounter  NodeRole
    def backends = sql_return_maparray "show backends;"
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    String host = ''
    for (def backend in backends) {
        if (backend.keySet().contains('Host')) {
            host = backend.Host
        } else {
            host = backend.IP
        }
        def cloud_tag = parseJson(backend.Tag)
        if (backend.Alive.equals("true") && cloud_tag.cloud_cluster_name.contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend.BackendId, host)
            backendIdToBackendHttpPort.put(backend.BackendId, backend.HttpPort)
            backendIdToBackendBrpcPort.put(backend.BackendId, backend.BrpcPort)
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

    def resetUrl = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/file_cache?op=reset&capacity="""
    def capacity = "0"
    logger.info(resetUrl)
    def resetFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri resetUrl + capacity
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
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()


    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/customer_delete.sql""").text
    def load_customer_ttl_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        // def table = "customer"
        // create table if not exists
        sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
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

    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        // def table = "customer"
        // create table if not exists
        sql new File("""${context.file.parent}/../ddl/${table}.sql""").text
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

    clearFileCache.call() {
        respCode, body -> {}
    }

    // one customer table would take about 1.3GB, the total cache size is 20GB
    // the following would take 19.5G all
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")

    // The max ttl cache size is 90% cache capacity
    long ttl_cache_size = 0
    sleep(30000)
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
                    ttl_cache_size = line.substring(i).toLong()
                    logger.info("current ttl_cache_size " + ttl_cache_size);
                    assertTrue(ttl_cache_size <= 19327352832)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }
    capacity = "-1"
    resetFileCache.call() {
        respCode, body -> {
            assertFalse("${respCode}".toString().equals("200"))
        }
    }

    capacity = "-faf13r1r"
    resetFileCache.call() {
        respCode, body -> {
            assertFalse("${respCode}".toString().equals("200"))
        }
    }

    capacity = "0"
    resetFileCache.call() {
        respCode, body -> {
            assertFalse("${respCode}".toString().equals("200"))
        }
    }

    capacity = "1073741824&path=/xxxxxx" // 1GB
    resetFileCache.call() {
        respCode, body -> {
            assertEquals("${respCode}".toString(), "200")
        }
    }

    capacity = "1073741824" // 1GB
    resetFileCache.call() {
        respCode, body -> {
            assertEquals("${respCode}".toString(), "200")
        }
    }

    sleep(60000)
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
                    ttl_cache_size = line.substring(i).toLong()
                    logger.info("current ttl_cache_size " + ttl_cache_size);
                    assertTrue(ttl_cache_size <= 1073741824)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }

    capacity = "1099511627776" // 1TB
    resetFileCache.call() {
        respCode, body -> {
            assertEquals("${respCode}".toString(), "200")
        }
    }
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")
    load_customer_ttl_once("customer_ttl")

    sleep(30000)
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
                    ttl_cache_size = line.substring(i).toLong()
                    logger.info("current ttl_cache_size " + ttl_cache_size);
                    assertTrue(ttl_cache_size > 1073741824)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }

    capacity = "21474836480" // 20GB
    resetFileCache.call() {
        respCode, body -> {
            assertEquals("${respCode}".toString(), "200")
        }
    }

    sql new File("""${context.file.parent}/../ddl/customer_delete.sql""").text
}