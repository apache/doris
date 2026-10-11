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

// The file cache TTL deadline is the tablet creation time plus file_cache_ttl_seconds. Once a
// tablet is past that deadline, the load and query paths must stamp the blocks they create as
// non-TTL right away, instead of creating TTL blocks for the background sweep to demote again.
//
// This test deliberately runs with the TTL background threads turned down to a 10 minute
// interval. That is what gives it teeth: the sweep would otherwise repair the cache type within
// a second or two and the assertions would hold no matter what the write and read paths did.
// With the sweep out of the way, the cache type observed here is purely the one chosen at
// admission, so the expired-tablet cases below fail if the deadline is not applied at the source.
suite("test_ttl_expired_tablet") {
    def ttlSeconds = 30
    def sweepOffMs = 600000

    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        // Long enough that neither TTL background thread reconciles anything while the test runs.
        file_cache_background_ttl_gc_interval_ms : sweepOffMs,
        file_cache_background_ttl_info_update_interval_ms : sweepOffMs,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(custoBeConfig) {
    sql "set global enable_auto_analyze = false"
    sql "set global enable_audit_plugin = false"
    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]
    sql """use @${validCluster};""";

    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("${validCluster}")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
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

    def getTabletIds = { String tableName ->
        def tablets = sql "show tablets from ${tableName}"
        assertTrue(tablets.size() > 0, "No tablets found for table ${tableName}")
        tablets.collect { it[0] as Long }
    }

    // Counts of each cache type across the given tablets, e.g. [normal: 40, index: 8].
    def cacheTypeCounts = { List<Long> tabletIds ->
        def counts = [:].withDefault { 0 }
        for (Long tabletId in tabletIds) {
            def rows = sql "select type from information_schema.file_cache_info where tablet_id = ${tabletId}"
            for (row in rows) {
                counts[row[0].toString().toLowerCase()] += 1
            }
        }
        return counts
    }

    def totalBlocks = { Map counts -> counts.values().sum() ?: 0 }

    // Cache writes are asynchronous, so wait for the blocks to show up before judging their
    // type. Only their existence is waited on; the type assertions stay strict.
    def waitForAnyBlock = { List<Long> tabletIds, long timeoutMs ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (totalBlocks(cacheTypeCounts.call(tabletIds)) > 0) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "timed out waiting for cached blocks of tablets ${tabletIds}")
    }

    def waitForEmptyCache = { List<Long> tabletIds, long timeoutMs ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (totalBlocks(cacheTypeCounts.call(tabletIds)) == 0) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "timed out waiting for an empty cache for tablets ${tabletIds}")
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

    def createCustomerTable = { String table, long ttl ->
        def ddl = new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text
        sql (ddl.replace("customer_ttl", table)
             + """ PROPERTIES("file_cache_ttl_seconds"="${ttl}") """)
        sql """ alter table ${table} set ("disable_auto_compaction" = "true") """
    }

    sql """ DROP TABLE IF EXISTS customer_ttl_expired """
    sql """ DROP TABLE IF EXISTS customer_ttl_live """
    clearFileCache.call() { respCode, body -> {} }
    sleep(5000)

    // ---------------------------------------------------------------------------------------
    // Case 1: load into a tablet that is already past its deadline.
    // ---------------------------------------------------------------------------------------
    createCustomerTable.call("customer_ttl_expired", ttlSeconds)
    // Let the deadline pass before a single row is written.
    sleep((ttlSeconds + 15) * 1000L)

    loadCustomerRows("customer_ttl_expired")
    def expiredTablets = getTabletIds.call("customer_ttl_expired")

    // Guard against a vacuous pass: the load must actually have cached something.
    waitForAnyBlock.call(expiredTablets, 60000L)
    def afterLoad = cacheTypeCounts.call(expiredTablets)
    logger.info("cache types after loading an expired tablet: ${afterLoad}")
    assertEquals(0, afterLoad['ttl'],
                 "load path put blocks of an expired tablet into the TTL queue: ${afterLoad}")

    // ---------------------------------------------------------------------------------------
    // Case 2: read from that tablet on a cold cache, so the query path admits the blocks.
    // ---------------------------------------------------------------------------------------
    clearFileCache.call() { respCode, body -> {} }
    // Without an empty cache the query below would be a hit and would admit nothing, leaving
    // the read path untested.
    waitForEmptyCache.call(expiredTablets, 60000L)

    // sum() has to read the column data, unlike count(*) which can be answered from metadata.
    sql """ select sum(C_ACCTBAL), count(C_COMMENT) from customer_ttl_expired """

    waitForAnyBlock.call(expiredTablets, 60000L)
    def afterRead = cacheTypeCounts.call(expiredTablets)
    logger.info("cache types after reading an expired tablet: ${afterRead}")
    assertEquals(0, afterRead['ttl'],
                 "read path put blocks of an expired tablet into the TTL queue: ${afterRead}")

    // ---------------------------------------------------------------------------------------
    // Case 3: a tablet still inside its window must keep using the TTL queue. Without this the
    // two cases above would also pass if the deadline were simply always reported as expired.
    // ---------------------------------------------------------------------------------------
    createCustomerTable.call("customer_ttl_live", 3600)
    loadCustomerRows("customer_ttl_live")
    def liveTablets = getTabletIds.call("customer_ttl_live")

    waitForAnyBlock.call(liveTablets, 60000L)
    def liveCounts = cacheTypeCounts.call(liveTablets)
    logger.info("cache types for a tablet inside its TTL window: ${liveCounts}")
    assertTrue(liveCounts['ttl'] > 0,
               "tablet inside its TTL window got no TTL blocks: ${liveCounts}")

    sql """ DROP TABLE IF EXISTS customer_ttl_expired """
    sql """ DROP TABLE IF EXISTS customer_ttl_live """
    }
}
