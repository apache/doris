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

suite("alter_ttl_1") {
    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(custoBeConfig) {
    sql "set global enable_auto_analyze = false"
    sql "set global enable_audit_plugin = false"
    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    sql """use @${validCluster};""";
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="90") """
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
        // def table = "customer"
        // create table if not exists
        sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
        sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction
        // insert rows until the dataset reaches ~100MB
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
    waitForFileCacheType.call(tabletIds, "ttl", 60000L)
    sleep(30000)
    long ttl_cache_size = 0
    long normal_cache_size = 0
    getMetricsMethod.call() {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag1 = false;
            Boolean flag2 = false;
            for (String line in strs) {
                if (flag1) break;
                if (line.contains("ttl_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    ttl_cache_size = line.substring(i).toLong()
                    flag1 = true
                }
                if (line.contains("normal_queue_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    normal_cache_size = line.substring(i).toLong()
                    flag2 = true
                }
            }
            assertTrue(flag1 && flag2)
    }
    sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="100") """
    sleep(80000)
    // after 110s, the first load has translate to normal
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
                    logger.info("ttl_cache_size line: " + line)
                    assertEquals(line.substring(i).toLong(), 0)

                }

                if (line.contains("normal_queue_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    logger.info("ttl_cache_size: " + ttl_cache_size)
                    logger.info("normal_cache_size: " + normal_cache_size)
                    logger.info("new normal cache_size: " + line)
                    assertEquals(line.substring(i).toLong(), ttl_cache_size + normal_cache_size)
                    flag1 = true
                }
            }
            assertTrue(flag1)
    }

    waitForFileCacheType.call(tabletIds, "normal", 180000L)
    }
}
