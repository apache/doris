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

import org.apache.doris.regression.util.WarmupMetricsUtils

suite("alter_ttl_2", "nonConcurrent") {
    def originalQueryCache = sql("select @@enable_sql_cache, @@enable_query_cache")[0]
            .collect { it.toString().toLowerCase(Locale.ROOT) }
    assertTrue(originalQueryCache.every { it in ["true", "false", "1", "0"] })
    onFinish {
        sql "set enable_sql_cache = ${originalQueryCache[0]}"
        sql "set enable_query_cache = ${originalQueryCache[1]}"
    }
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
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
    sql """use @${validCluster};""";
    long initialTtlSeconds = 3600L
    long expiredTtlSeconds = 1L
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="${initialTtlSeconds}") """
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
    def cacheBackend = [
        ip: backendIdToBackendIP.get(backendId),
        httpPort: backendIdToBackendHttpPort.get(backendId),
        brpcPort: backendIdToBackendBrpcPort.get(backendId)
    ]

    def getTabletIds = { String tableName ->
        def tablets = sql "show tablets from ${tableName}"
        assertTrue(tablets.size() > 0, "No tablets found for table ${tableName}")
        tablets.collect { it[0] as Long }
    }

    def getCache = { List<Long> tabletIds ->
        sql "use @${validCluster}"
        def rows = sql """select be_id, cache_path, tablet_id, `hash`, `offset`, size, lower(type)
            from information_schema.file_cache_info
            where tablet_id in (${tabletIds.join(',')}) and be_id = ${backendId}"""
        def blocks = [:]
        rows.each { row ->
            def key = [row[0] as Long, row[1].toString(), row[2] as Long,
                       row[3].toString(), row[4] as Long]
            assertTrue(!blocks.containsKey(key), "Duplicate cache block: ${key}")
            assertTrue((row[5] as Long) > 0L, "Invalid block size: ${row}")
            blocks[key] = [size: row[5] as Long, type: row[6].toString()]
        }
        blocks
    }
    def sizes = { Map blocks -> blocks.collectEntries { key, value -> [(key): value.size] } }
    def waitForFileCacheType = { List<Long> tabletIds, String expectedType, Map expectedBlocks = null ->
        long deadline = System.currentTimeMillis() + 600000L
        long stableSince = 0L
        def previous = null
        def lastState = [:]
        while (System.currentTimeMillis() < deadline) {
            def blocks = getCache(tabletIds)
            def missing = tabletIds.findAll { id -> !blocks.keySet().any { it[2] == id } }
            boolean ready = missing.isEmpty() && blocks.values().every { it.type == expectedType } &&
                    (expectedBlocks == null || sizes(blocks) == sizes(expectedBlocks))
            lastState = [expected_type: expectedType, missing_tablets: missing,
                         by_type: blocks.values().groupBy { it.type }.collectEntries { type, values ->
                             [(type): [blocks: values.size(), bytes: values.sum(0L) { it.size }]]
                         }, missing_blocks: expectedBlocks == null ? [] : (expectedBlocks.keySet() - blocks.keySet()).take(10)]
            if (blocks != previous) { logger.info("Scoped TTL transition: ${lastState}") }
            if (!ready || blocks != previous) { stableSince = System.currentTimeMillis() }
            previous = blocks
            if (ready && System.currentTimeMillis() - stableSince >= 3000L) {
                logger.info("Verified scoped cache: tablets=${tabletIds}, state=${lastState}")
                return blocks
            }
            sleep(1000)
        }
        assertTrue(false, "Timeout waiting for scoped cache conversion on ${validCluster}/BE ${backendId}: ${lastState}")
    }
    def getGlobalMetrics = {
        ["ttl_cache_size", "normal_queue_cache_size"].collectEntries { name ->
            [(name): WarmupMetricsUtils.getBrpcMetric(cacheBackend.ip.toString(), cacheBackend.brpcPort.toString(), name)]
        }
    }
    def scanTable = {
        // Force a BE column scan and check fixture data before and after the cache transition.
        def result = sql "select count(*), sum(C_CUSTKEY) from customer_ttl"
        assertEquals(200L, result[0][0] as Long)
        assertEquals(2020100L, result[0][1] as Long)
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

    try {
        def metricsBeforeClear = getGlobalMetrics()
        WarmupMetricsUtils.clearFileCache(cacheBackend.ip.toString(), cacheBackend.httpPort.toString())
        def metricsAfterClear = getGlobalMetrics()
        // sync=true may leave held blocks from other tables. Global counters are diagnostic only.
        logger.info("TTL clear metrics: before=${metricsBeforeClear}, after=${metricsAfterClear}")

        load_customer_ttl_once("customer_ttl")
        def tabletIds = getTabletIds.call("customer_ttl")
        scanTable()
        def baseline = waitForFileCacheType(tabletIds, "ttl")
        def metricsBeforeAlter = getGlobalMetrics()
        logger.info("TTL before shortening: tablets=${tabletIds}, blocks=${baseline}, metrics=${metricsBeforeAlter}")

        // The stable TTL baseline above is already more than one second after tablet creation.
        // Retain the positive -> positive transition; the original blocks must change type in place.
        sql """ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="${expiredTtlSeconds}")"""
        waitForFileCacheType(tabletIds, "normal", baseline)
        def metricsAfterAlter = getGlobalMetrics()
        def delta = metricsAfterAlter.collectEntries { name, value -> [(name): value - metricsBeforeAlter[name]] }
        logger.info("TTL after shortening: before=${metricsBeforeAlter}, after=${metricsAfterAlter}, delta=${delta}")
        scanTable()
    } finally {
        sql "DROP TABLE IF EXISTS customer_ttl FORCE"
    }
    }
}
