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

suite("test_ttl_random") {
    sql """ use @regression_cluster_name1 """
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

    def tables = [customer_ttl: 15000000]
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
    sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
    def load_customer_once =  { String table ->
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/customer_ttl_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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
    sleep(30000)

    load_customer_once("customer_ttl")

    long maxFileCacheTtlSeconds = Long.MAX_VALUE / 2L
    long randomSeed = 20260902L
    Random random = new Random(randomSeed)
    long lastAlterTtlSeconds = 180L
    logger.info("Use deterministic random seed ${randomSeed} for TTL ALTER values")
    for (int j = 0; j < 40; j++) {
        int number = j % 10
        if (number < 5) {
            sql """ select * from customer_ttl limit 10"""
        } else if (number >= 5 && number < 8) {
            load_customer_once("customer_ttl")
        } else {
            lastAlterTtlSeconds = Math.floorMod(
                    random.nextLong(), maxFileCacheTtlSeconds + 1L)
            sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="${lastAlterTtlSeconds}") """
            sleep(40000)
        }
    }

    def showCreateTable = sql "show create table customer_ttl"
    assertTrue(showCreateTable[0][1].toString().contains(
            "\"file_cache_ttl_seconds\" = \"${lastAlterTtlSeconds}\""),
            "file_cache_ttl_seconds should converge to the last ALTER value ${lastAlterTtlSeconds}")
}
