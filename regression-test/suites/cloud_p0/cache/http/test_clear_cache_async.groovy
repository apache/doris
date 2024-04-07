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

suite("test_clear_cache_async") {
    sql """ use @regression_cluster_name1 """
    sql """ set global enable_auto_analyze = false; """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="180") """
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
    def url = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + """/api/clear_file_cache"""
    url = url + "?sync=false"
    logger.info("clear file cache URL:" + url)
    def clearFileCache = { check_func ->
        httpTest {
            endpoint ""
            uri url
            op "post"
            body ""
            check check_func
        }
    }

    def tables = [customer_ttl: 15000000]
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    

    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
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

    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    clearFileCache.call() {
        respCode, body -> {}
    }
    // sleep(30000) // 30s
    // load_customer_once("customer_ttl")
    // load_customer_once("customer")

    // clearFileCache.call() {
    //     respCode, body -> {}
    // }
    // sleep(30000)
    // getMetricsMethod.call() {
    //     respCode, body ->
    //         assertEquals("${respCode}".toString(), "200")
    //         String out = "${body}".toString()
    //         def strs = out.split('\n')
    //         Boolean flag = false;
    //         long total_cache_size = 0;
    //         for (String line in strs) {
    //             if (line.contains("file_cache_cache_size")) {
    //                 if (line.startsWith("#")) {
    //                     continue
    //                 }
    //                 def i = line.indexOf(' ')
    //                 total_cache_size = line.substring(i).toLong()
    //                 assertEquals(0, total_cache_size)
    //                 flag = true
    //                 break
    //             }
    //         }
    //         assertTrue(flag)
    // }

    // sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    // sql new File("""${context.file.parent}/../ddl/customer_delete.sql""").text
}
