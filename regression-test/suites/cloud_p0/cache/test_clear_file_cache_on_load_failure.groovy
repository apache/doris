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

suite("test_clear_file_cache_on_load_failure", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    String originalAutoAnalyze = sql("SHOW GLOBAL VARIABLES LIKE 'enable_auto_analyze'")[0][1].toString()
    assertTrue(originalAutoAnalyze.toLowerCase() in ["true", "false", "1", "0"],
            "Unexpected auto-analyze setting: ${originalAutoAnalyze}")
    onFinish {
        sql "SET GLOBAL enable_auto_analyze = ${originalAutoAnalyze}"
        logger.info("Restored enable_auto_analyze=${originalAutoAnalyze}")
    }

    // Clear any existing debug points
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def getCacheBackends = {
        return (sql """SHOW BACKENDS""").collect { be ->
            [ip: be[1], httpPort: be[4], brpcPort: be[5]]
        }
    }

    def requestFileCacheClearOnAllBackends = {
        getCacheBackends().each { be ->
            WarmupMetricsUtils.clearFileCache(be.ip.toString(), be.httpPort.toString())
        }
    }

    def cacheSizeMetric = "file_cache_cache_size"

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

        requestFileCacheClearOnAllBackends()

        // sync=true synchronously removes releasable blocks, but it does not guarantee that
        // blocks still held by readers or writers have all been recycled. Use a stable metric
        // snapshot as the baseline and verify cache-size increments instead of requiring zero.
        def initialCacheSize = WarmupMetricsUtils.waitForBackendMetricSumStable(
                getCacheBackends(),
                cacheSizeMetric,
                10000,
                60000)
        logger.info("Initial file cache size: ${initialCacheSize}")

        // First, do a successful load to establish baseline
        sql """INSERT INTO ${tableName} VALUES (1, 'test1', 100)"""

        def afterSuccessfulLoadSize = WarmupMetricsUtils.waitForBackendMetricSum(
                getCacheBackends(),
                cacheSizeMetric,
                { value -> value > initialCacheSize },
                120000,
                "Cache should increase after successful load. Initial: ${initialCacheSize}")
        logger.info("Cache size after successful load: ${afterSuccessfulLoadSize}")

        // The successful load may populate the cache asynchronously. Wait until it is stable,
        // then use that value as the baseline for the failed-load cache-size increment.
        afterSuccessfulLoadSize = WarmupMetricsUtils.waitForBackendMetricSumStable(
                getCacheBackends(),
                cacheSizeMetric,
                10000,
                60000)
        logger.info("Stable cache size before failed load: ${afterSuccessfulLoadSize}")

        // Enable debug point to make commit_rowset return error
        GetDebugPoint().enableDebugPointForAllBEs("LoadChannel.add_batch.failed")

        // Try to insert data - this should fail due to injection point
        try {
            sql """INSERT INTO ${tableName} VALUES (2, 'test2', 200)"""
        } catch (Exception e) {
            logger.info("Expected load failure occurred: ${e.message}")
        }

        // Wait for cleanup to complete and cache metrics to update
        def afterFailedLoadSize = WarmupMetricsUtils.waitForBackendMetricSumStable(
                getCacheBackends(),
                cacheSizeMetric,
                10000,
                60000)

        // Get cache size after failed load
        logger.info("Cache size after failed load: ${afterFailedLoadSize}")

        // Pre-existing deleting blocks may be recycled during this interval, so a negative
        // delta is valid. A failed load must not add cache bytes over the stable baseline.
        def failedLoadCacheSizeDelta = afterFailedLoadSize - afterSuccessfulLoadSize
        assertTrue(failedLoadCacheSizeDelta <= 0,
            "Cache should not increase after failed load. " +
            "Before: ${afterSuccessfulLoadSize}, After: ${afterFailedLoadSize}, " +
            "Difference: ${failedLoadCacheSizeDelta}")

        logger.info("Test passed: File cache was properly cleared after load failure")
    } finally {
        sql """DROP TABLE IF EXISTS ${tableName} FORCE"""
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
