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

import groovy.json.JsonSlurper

suite("test_clear_file_cache_on_load_failure", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    // Clear any existing debug points
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    // Helper function to clear file cache on all backends
    def clearFileCache = { ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
        }
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """SHOW BACKENDS"""
        for (be in backends) {
            def ip = be[1]
            def port = be[4]
            clearFileCache(ip, port)
        }
        // Wait for async clear to complete
        sleep(5000)
    }

    // Helper function to get brpc metrics
    def getBrpcMetrics = { ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        try {
            def metrics = new URL(url).text
            def matcher = metrics =~ ~"${name}\\s+(\\d+)"
            if (matcher.find()) {
                return matcher[0][1] as long
            }
        } catch (Exception e) {
            logger.warn("Failed to get brpc metrics from ${ip}:${port}: ${e.message}")
        }
        return 0L
    }

    // Helper function to get index queue cache size
    def getIndexQueueSize = { ip, port ->
        return getBrpcMetrics(ip, port, "file_cache_index_queue_cache_size")
    }

    // Helper function to get total index queue cache size across all backends
    def getTotalIndexQueueSize = {
        def backends = sql """SHOW BACKENDS"""
        long totalSize = 0
        for (be in backends) {
            def ip = be[1]
            def brpcPort = be[5]
            def size = getIndexQueueSize(ip, brpcPort)
            totalSize += size
            logger.info("BE ${ip}:${brpcPort} index_queue_size = ${size}")
        }
        return totalSize
    }

    // Create test table with file cache enabled
    def tableName = "test_load_failure_cache"
    sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE ${tableName} (
            k1 INT NOT NULL,
            v1 VARCHAR(100),
            v2 INT
        ) UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    try {
        // Disable auto analyze to avoid internal loads affecting cache size
        sql """SET GLOBAL enable_auto_analyze = false"""

        // Clear file cache and wait for it to complete
        clearFileCacheOnAllBackends()
        sleep(3000)

        // Get initial cache size
        def initialCacheSize = getTotalIndexQueueSize()
        logger.info("Initial file cache size: ${initialCacheSize}")

        // First, do a successful load to establish baseline
        sql """INSERT INTO ${tableName} VALUES (1, 'test1', 100)"""
        // Wait for cache metrics to update
        sleep(5000)

        def afterSuccessfulLoadSize = getTotalIndexQueueSize()
        logger.info("Cache size after successful load: ${afterSuccessfulLoadSize}")
        assertTrue(afterSuccessfulLoadSize > initialCacheSize,
            "Cache should increase after successful load. Initial: ${initialCacheSize}, After: ${afterSuccessfulLoadSize}")

        // Clear cache again to reset
        clearFileCacheOnAllBackends()
        sleep(3000)

        def afterClearSize = getTotalIndexQueueSize()
        logger.info("Cache size after clear: ${afterClearSize}")

        // Enable debug point to make commit_rowset return error
        GetDebugPoint().enableDebugPointForAllBEs("LoadChannel.add_batch.failed")

        // Try to insert data - this should fail due to injection point
        try {
            sql """INSERT INTO ${tableName} VALUES (2, 'test2', 200)"""
        } catch (Exception e) {
            logger.info("Expected load failure occurred: ${e.message}")
        }

        // Wait for cleanup to complete and cache metrics to update
        sleep(5000)

        // Get cache size after failed load
        def afterFailedLoadSize = getTotalIndexQueueSize()
        logger.info("Cache size after failed load: ${afterFailedLoadSize}")

        // Verify cache size has not increased
        assertTrue(afterFailedLoadSize == afterClearSize,
            "Cache should not increase after failed load. " +
            "Before: ${afterClearSize}, After: ${afterFailedLoadSize}, " +
            "Difference: ${afterFailedLoadSize - afterClearSize}")

        logger.info("Test passed: File cache was properly cleared after load failure")
    } finally {
        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
