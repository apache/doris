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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper

/*
Test Description:

1. When disable_file_cache = true and enable_file_cache = true, it is expected that the S3 TVF load (import phase) will NOT enter the cache, while the query 
   phase will enter the Disposable queue.
   Specifically: Normal queue size should be 0, Disposable queue size should be 91163 bytes.
2. When disable_file_cache = false and enable_file_cache = true, it is expected that the S3 TVF load (import phase) will enter the Normal queue, and the query 
   phase will still enter the Disposable queue.
   Specifically: Normal queue size should be 236988 bytes, Disposable queue size should still be 91163 bytes.

Explanation: The query phase caches the compressed file, so the Disposable queue size is checked for an exact value; for the import phase cache, since future 
changes to statistics are possible, only a reasonable range is required.
*/

suite('test_load_cache', 'docker') {
    // Randomly enable or disable packed_file to test both scenarios
    def enablePackedFile = new Random().nextBoolean()
    logger.info("Running test with enable_packed_file=${enablePackedFile}")

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        "enable_packed_file=${enablePackedFile}",
    ]
    options.cloudMode = true
    options.beNum = 1

    def clearFileCache = {ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)

        // Check the status
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

        // clear file cache is async, wait it done
        sleep(5000)
    }

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            return 0
        }
    }

    def getAllCacheMetrics = {ip, port ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def cacheMetrics = [:]
        metrics.eachLine { line ->
            if (line.contains("file_cache") && !line.startsWith("#")) {
                def parts = line.split()
                if (parts.size() >= 2) {
                    cacheMetrics[parts[0]] = parts[1]
                }
            }
        }
        return cacheMetrics
    }

    def getCacheFileList = {ip, port ->
        try {
            def url = "http://${ip}:${port}/api/file_cache?op=list"
            def response = new URL(url).text
            return response
        } catch (Exception e) {
            logger.warn("Failed to get cache file list: ${e.message}")
            return "Error: ${e.message}"
        }
    }

    def getNormalQueueSize = { ip, port ->
        return getBrpcMetrics(ip, port, "file_cache_normal_queue_cache_size")
    }

    def getDisposableQueueSize = { ip, port ->
        return getBrpcMetrics(ip, port, "file_cache_disposable_queue_cache_size")
    }

    def getDisposableQueueElementCount = { ip, port ->
        return getBrpcMetrics(ip, port, "file_cache_disposable_queue_element_count")
    }

    def getTotalNormalQueueSize = {
        def backends = sql """SHOW BACKENDS"""
        long sum = 0
        for (be in backends) {
            def ip = be[1]
            def port = be[5]
            def size = getNormalQueueSize(ip, port)
            sum += size
            logger.info("BE ${ip}:${port} normal_queue_size = ${size}")
        }
        return sum
    }

    def getTotalDisposableQueueSize = {
        def backends = sql """SHOW BACKENDS"""
        long sum = 0
        for (be in backends) {
            def ip = be[1]
            def port = be[5]
            def size = getDisposableQueueSize(ip, port)
            sum += size
            logger.info("BE ${ip}:${port} disposable_queue_size = ${size}")
        }
        return sum
    }

    def getTotalDisposableQueueElementCount = {
        def backends = sql """SHOW BACKENDS"""
        long sum = 0
        for (be in backends) {
            def ip = be[1]
            def port = be[5]
            def count = getDisposableQueueElementCount(ip, port)
            sum += count
            logger.info("BE ${ip}:${port} disposable_queue_element_count = ${count}")
        }
        return sum
    }

    docker(options) {
        def ak = getS3AK()
        def sk = getS3SK()
        def s3_endpoint = getS3Endpoint()
        def s3_region = getS3Region()
        def s3_bucket = getS3BucketName()
        def s3_provider = getS3Provider()

        def s3_tvf_uri = "s3://${s3_bucket}/regression/tpch/sf0.01/customer.csv.gz"

        // ============================================================================
        // SCENARIO 1: disable_file_cache = true
        // ============================================================================

        // Clear file cache before test
        clearFileCacheOnAllBackends()

        // Set session variables for Scenario 1
        sql "set disable_file_cache = true;"
        sql "set enable_file_cache = true;"

        // Create test table
        sql """DROP TABLE IF EXISTS load_test_table"""
        sql """
            CREATE TABLE load_test_table (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        VARCHAR(25) NOT NULL,
                C_ADDRESS     VARCHAR(40) NOT NULL,
                C_NATIONKEY   INTEGER NOT NULL,
                C_PHONE       CHAR(15) NOT NULL,
                C_ACCTBAL     DECIMAL(15,2) NOT NULL,
                C_MKTSEGMENT  CHAR(10) NOT NULL,
                C_COMMENT     VARCHAR(117) NOT NULL
            ) DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Record initial cache sizes
        long normalQueueSizeBefore1 = getTotalNormalQueueSize()
        long disposableQueueSizeBefore1 = getTotalDisposableQueueSize()
        long disposableQueueElementCountBefore1 = getTotalDisposableQueueElementCount()

        // Execute S3 TVF Load
        sql """
            INSERT INTO load_test_table
            SELECT
                CAST(c1 AS INT) AS C_CUSTKEY,
                CAST(c2 AS VARCHAR(25)) AS C_NAME,
                CAST(c3 AS VARCHAR(40)) AS C_ADDRESS,
                CAST(c4 AS INT) AS C_NATIONKEY,
                CAST(c5 AS CHAR(15)) AS C_PHONE,
                CAST(c6 AS DECIMAL(15,2)) AS C_ACCTBAL,
                CAST(c7 AS CHAR(10)) AS C_MKTSEGMENT,
                CAST(c8 AS VARCHAR(117)) AS C_COMMENT
            FROM S3(
                "uri" = "${s3_tvf_uri}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${s3_region}",
                "format" = "csv",
                "column_separator" = "|"
            )
        """

        def rowCount1 = sql """SELECT COUNT(*) FROM load_test_table"""
        logger.info("Loaded ${rowCount1[0][0]} rows")

        // Wait for cache metrics to update
        sleep(5000)

        // Record cache sizes after load
        long normalQueueSizeAfter1 = getTotalNormalQueueSize()
        long disposableQueueSizeAfter1 = getTotalDisposableQueueSize()
        long disposableQueueElementCountAfter1 = getTotalDisposableQueueElementCount()

        long normalQueueIncrease1 = normalQueueSizeAfter1 - normalQueueSizeBefore1
        long disposableQueueIncrease1 = disposableQueueSizeAfter1 - disposableQueueSizeBefore1
        long disposableElementIncrease1 = disposableQueueElementCountAfter1 - disposableQueueElementCountBefore1

        logger.info("Expected: Normal=0 bytes, Disposable=91163 bytes")
        logger.info("Actual:   Normal=${normalQueueIncrease1} bytes, Disposable=${disposableQueueIncrease1} bytes")

        // Verify Scenario 1
        def expectedDisposableSize = 91163
        assertTrue(disposableQueueIncrease1 == expectedDisposableSize,
            "Scenario 1: Disposable queue should be exactly ${expectedDisposableSize} bytes, but got ${disposableQueueIncrease1} bytes")
        assertTrue(disposableElementIncrease1 > 0,
            "Scenario 1: Disposable queue elements should increase, but got ${disposableElementIncrease1}")
        assertTrue(normalQueueIncrease1 == 0,
            "Scenario 1: Normal queue should be 0 bytes, but got ${normalQueueIncrease1} bytes")

        // Clean up
        sql """DROP TABLE IF EXISTS load_test_table"""

        // Wait between tests
        sleep(3000)

        // ============================================================================
        // SCENARIO 2: disable_file_cache = false
        // ============================================================================

        // Clear file cache before test
        clearFileCacheOnAllBackends()

        // Set session variables for Scenario 2
        sql "set disable_file_cache = false;"
        sql "set enable_file_cache = true;"

        // Create test table
        sql """DROP TABLE IF EXISTS load_test_table"""
        sql """
            CREATE TABLE load_test_table (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        VARCHAR(25) NOT NULL,
                C_ADDRESS     VARCHAR(40) NOT NULL,
                C_NATIONKEY   INTEGER NOT NULL,
                C_PHONE       CHAR(15) NOT NULL,
                C_ACCTBAL     DECIMAL(15,2) NOT NULL,
                C_MKTSEGMENT  CHAR(10) NOT NULL,
                C_COMMENT     VARCHAR(117) NOT NULL
            ) DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Record initial cache sizes
        long normalQueueSizeBefore2 = getTotalNormalQueueSize()
        long disposableQueueSizeBefore2 = getTotalDisposableQueueSize()
        long disposableQueueElementCountBefore2 = getTotalDisposableQueueElementCount()

        // Execute S3 TVF Load
        sql """
            INSERT INTO load_test_table
            SELECT
                CAST(c1 AS INT) AS C_CUSTKEY,
                CAST(c2 AS VARCHAR(25)) AS C_NAME,
                CAST(c3 AS VARCHAR(40)) AS C_ADDRESS,
                CAST(c4 AS INT) AS C_NATIONKEY,
                CAST(c5 AS CHAR(15)) AS C_PHONE,
                CAST(c6 AS DECIMAL(15,2)) AS C_ACCTBAL,
                CAST(c7 AS CHAR(10)) AS C_MKTSEGMENT,
                CAST(c8 AS VARCHAR(117)) AS C_COMMENT
            FROM S3(
                "uri" = "${s3_tvf_uri}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${s3_region}",
                "format" = "csv",
                "column_separator" = "|"
            )
        """

        def rowCount2 = sql """SELECT COUNT(*) FROM load_test_table"""
        logger.info("Loaded ${rowCount2[0][0]} rows")

        // Wait for cache metrics to update
        sleep(5000)

        // Record cache sizes after load
        long normalQueueSizeAfter2 = getTotalNormalQueueSize()
        long disposableQueueSizeAfter2 = getTotalDisposableQueueSize()
        long disposableQueueElementCountAfter2 = getTotalDisposableQueueElementCount()

        long normalQueueIncrease2 = normalQueueSizeAfter2 - normalQueueSizeBefore2
        long disposableQueueIncrease2 = disposableQueueSizeAfter2 - disposableQueueSizeBefore2
        long disposableElementIncrease2 = disposableQueueElementCountAfter2 - disposableQueueElementCountBefore2

        logger.info("Expected: Normal=~237KB (range), Disposable=91163 bytes (exact)")
        logger.info("Actual:   Normal=${normalQueueIncrease2} bytes (${String.format("%.2f", normalQueueIncrease2 / 1024.0)} KB), Disposable=${disposableQueueIncrease2} bytes")

        // Verify Scenario 2
        assertTrue(disposableQueueIncrease2 == expectedDisposableSize,
            "Scenario 2: Disposable queue should be exactly ${expectedDisposableSize} bytes, but got ${disposableQueueIncrease2} bytes")
        assertTrue(disposableElementIncrease2 > 0,
            "Scenario 2: Disposable queue elements should increase, but got ${disposableElementIncrease2}")
        def normalMinThreshold = 200 * 1024
        def normalMaxThreshold = 280 * 1024
        assertTrue(normalQueueIncrease2 >= normalMinThreshold && normalQueueIncrease2 <= normalMaxThreshold,
            "Scenario 2: Normal queue should be in range [${normalMinThreshold}, ${normalMaxThreshold}] bytes, but got ${normalQueueIncrease2} bytes")

        // Clean up
        sql """DROP TABLE IF EXISTS load_test_table"""
    }
}