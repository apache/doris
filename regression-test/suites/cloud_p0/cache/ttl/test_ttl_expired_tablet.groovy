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

// The file cache TTL deadline is the tablet creation time plus file_cache_ttl_seconds.
// Once a tablet is past that deadline, the load, query and warm up paths must all stamp the
// blocks they create as NORMAL right away. Before the deadline was defined in one place,
// those paths passed the raw ttl_seconds instead, so every block went into the TTL queue and
// the background sweep pulled it straight back out, over and over, for the rest of the
// tablet's life. This test pins that down: past the deadline, nothing reaches the TTL queue.
suite("test_ttl_expired_tablet") {
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

    def ttlSeconds = 30
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="${ttlSeconds}") """
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

    def getTtlCacheSize = {
        long ttlCacheSize = -1
        getMetricsMethod.call() {
            respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                String out = "${body}".toString()
                for (String line in out.split('\n')) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    if (line.contains("ttl_cache_size")) {
                        def i = line.indexOf(' ')
                        ttlCacheSize = line.substring(i).toLong()
                        break
                    }
                }
        }
        assertTrue(ttlCacheSize >= 0, "ttl_cache_size metric not found")
        return ttlCacheSize
    }

    def getTabletIds = { String tableName ->
        def tablets = sql "show tablets from ${tableName}"
        assertTrue(tablets.size() > 0, "No tablets found for table ${tableName}")
        tablets.collect { it[0] as Long }
    }

    def waitForFileCacheType = { List<Long> tabletIds, String expectedType, long timeoutMs = 60000L, long intervalMs = 1000L ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            boolean allMatch = true
            for (Long tabletId in tabletIds) {
                def rows = sql "select type from information_schema.file_cache_info where tablet_id = ${tabletId}"
                if (rows.isEmpty()) {
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
                return
            }
            sleep(intervalMs)
        }
        assertTrue(false, "Timeout waiting for file_cache_info type ${expectedType} for tablets ${tabletIds}")
    }

    def loadCustomerRows = { String table ->
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

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    clearFileCache.call() {
        respCode, body -> {}
    }
    sleep(10000)
    assertEquals(0L, getTtlCacheSize.call())

    // Create the table, then let its TTL deadline pass before writing a single row.
    sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
    sql """ alter table customer_ttl set ("disable_auto_compaction" = "true") """
    sleep((ttlSeconds + 15) * 1000L)

    loadCustomerRows("customer_ttl")
    def tabletIds = getTabletIds.call("customer_ttl")

    // The tablet is past its deadline, so the load path must have written every block into
    // the normal queue directly.
    waitForFileCacheType.call(tabletIds, "normal", 60000L)

    // And it must stay there. Sample repeatedly: with the deadline stamped per block, the
    // background sweep has nothing to convert, so the TTL queue never grows. When the write
    // path passed a raw ttl_seconds instead, this is where the promote/demote churn showed up.
    for (int i = 0; i < 10; i++) {
        assertEquals(0L, getTtlCacheSize.call())
        sleep(1000)
    }

    // Reading the data back must not promote it either.
    sql """ select count(*) from customer_ttl """
    sleep(5000)
    waitForFileCacheType.call(tabletIds, "normal", 30000L)
    assertEquals(0L, getTtlCacheSize.call())

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text
    }
}
