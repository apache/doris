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

suite("alter_ttl_3") {
    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(custoBeConfig) {
    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    sql """ use @${validCluster} """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="0") """
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

    def getTabletIds = { String tableName ->
        def tablets = sql "show tablets from ${tableName}"
        assertTrue(tablets.size() > 0, "No tablets found for table ${tableName}")
        tablets.collect { it[0] as Long }
    }

    def waitForFileCacheType = { List<Long> tabletIds, String expectedType, long timeoutMs = 120000L, long intervalMs = 2000L ->
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

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    def load_customer_ttl_once =  { String table ->
        sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
        sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction
        def totalRows = 200
        def batchSize = 100
        def commentSuffix = ' ' + ('X' * 50)
        for (int offset = 0; offset < totalRows; offset += batchSize) {
            def sb = new StringBuilder()
            int batchEnd = Math.min(totalRows, offset + batchSize)
            for (int idx = offset; idx < batchEnd; idx++) {
                def customerId = 10001 + idx
                def customerName = String.format('Customer#%09d', customerId)
                sb.append("""INSERT INTO ${table} VALUES (
                    ${customerId},
                    '${customerName}',
                    'Address Line 1',
                    15,
                    '123-456-7890',
                    12345.67,
                    'AUTOMOBILE',
                    'This is a test comment for the customer.${commentSuffix}'
                    );
                    """)
            }
            sql sb.toString()
        }
    }

    clearFileCache.call() {
        respCode, body -> {}
    }
    sleep(30000)

    load_customer_ttl_once("customer_ttl")
    def tabletIds = getTabletIds.call("customer_ttl")
    // ttl=0 means data stays in normal cache until altered to a positive ttl
    waitForFileCacheType.call(tabletIds, "normal", 60000L)
    sql """ select count(*) from customer_ttl """
    sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="3600") """
    sleep(160000)
    waitForFileCacheType.call(tabletIds, "ttl", 60000L)
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            Boolean flag2 = false;
            long ttl_cache_size = 0;
            for (String line in strs) {
                if (flag1 && flag2) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    logger.info("ttl_cache_size line after manual load: " + line)
                    assertTrue(line.substring(i).toLong() > 0)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }
    }
}
