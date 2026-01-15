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

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

// Constants for backend configuration check
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String ENABLE_FILE_CACHE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is not set to true"
final String FILE_CACHE_PATH_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_path is empty or not configured"

// Constants for hit ratio check
final String HIT_RATIO_CHECK_FAILED_PREFIX = "Hit ratio check failed: "
final String HIT_RATIO_METRIC_FALSE_MSG = HIT_RATIO_CHECK_FAILED_PREFIX + "hits_ratio metric is false"
final String HIT_RATIO_1H_METRIC_FALSE_MSG = HIT_RATIO_CHECK_FAILED_PREFIX + "hits_ratio_1h metric is false"
final String HIT_RATIO_5M_METRIC_FALSE_MSG = HIT_RATIO_CHECK_FAILED_PREFIX + "hits_ratio_5m metric is false"

// Constants for normal queue check
final String NORMAL_QUEUE_CHECK_FAILED_PREFIX = "Normal queue check failed: "
final String NORMAL_QUEUE_SIZE_VALIDATION_FAILED_MSG = NORMAL_QUEUE_CHECK_FAILED_PREFIX + "size validation failed (curr_size should be > 0 and < max_size)"
final String NORMAL_QUEUE_ELEMENTS_VALIDATION_FAILED_MSG = NORMAL_QUEUE_CHECK_FAILED_PREFIX + "elements validation failed (curr_elements should be > 0 and < max_elements)"

// Constants for hit and read counts check
final String HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX = "Hit and read counts check failed: "
final String INITIAL_TOTAL_HIT_COUNTS_NOT_GREATER_THAN_0_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "initial total_hit_counts is not greater than 0"
final String INITIAL_TOTAL_READ_COUNTS_NOT_GREATER_THAN_0_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "initial total_read_counts is not greater than 0"
final String TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "total_hit_counts did not increase after cache operation"
final String TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "total_read_counts did not increase after cache operation"

suite("test_file_cache_statistics", "external_docker,hive,external_docker_hive,p0,external,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    // Check backend configuration prerequisites
    // Note: This test case assumes a single backend scenario. Testing with single backend is logically equivalent 
    // to testing with multiple backends having identical configurations, but simpler in logic.
    def enableFileCacheResult = sql """show backend config like 'enable_file_cache';"""
    logger.info("enable_file_cache configuration: " + enableFileCacheResult)
    
    if (enableFileCacheResult.size() == 0 || !enableFileCacheResult[0][3].equalsIgnoreCase("true")) {
        logger.info(ENABLE_FILE_CACHE_CHECK_FAILED_MSG)
        assertTrue(false, ENABLE_FILE_CACHE_CHECK_FAILED_MSG)
    }
    
    def fileCachePathResult = sql """show backend config like 'file_cache_path';"""
    logger.info("file_cache_path configuration: " + fileCachePathResult)
    
    if (fileCachePathResult.size() == 0 || fileCachePathResult[0][3] == null || fileCachePathResult[0][3].trim().isEmpty()) {
        logger.info(FILE_CACHE_PATH_CHECK_FAILED_MSG)
        assertTrue(false, FILE_CACHE_PATH_CHECK_FAILED_MSG)
    }

    String catalog_name = "test_file_cache_statistics"
    String ex_db_name = "`default`"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

    sql """set global enable_file_cache=true"""
    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'hadoop.username' = 'hive'
    );"""

    sql """switch ${catalog_name}"""

    // load the table into file cache
    sql """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table where l_orderkey=1 and l_partkey=1534 limit 1;"""
    // do it twice to make sure the table block could hit the cache
    order_qt_1 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table where l_orderkey=1 and l_partkey=1534 limit 1;"""

    def fileCacheBackgroundMonitorIntervalMsResult = sql """show backend config like 'file_cache_background_monitor_interval_ms';"""
    logger.info("file_cache_background_monitor_interval_ms configuration: " + fileCacheBackgroundMonitorIntervalMsResult)
    assertFalse(fileCacheBackgroundMonitorIntervalMsResult.size() == 0 || fileCacheBackgroundMonitorIntervalMsResult[0][3] == null ||
            fileCacheBackgroundMonitorIntervalMsResult[0][3].trim().isEmpty(), "file_cache_background_monitor_interval_ms is empty or not set to true")

    // brpc metrics will be updated at most 5 seconds
    def totalWaitTime = (fileCacheBackgroundMonitorIntervalMsResult[0][3].toInteger() / 1000) as int
    def interval = 1
    def iterations = totalWaitTime / interval

    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // ===== Hit Ratio Metrics Check =====
    // Check overall hit ratio hits_ratio
    def hitsRatioResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics where METRIC_NAME = 'hits_ratio' limit 1;"""
    logger.info("hits_ratio result: " + hitsRatioResult)

    // Check 1-hour hit ratio hits_ratio_1h
    def hitsRatio1hResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics where METRIC_NAME = 'hits_ratio_1h' limit 1;"""
    logger.info("hits_ratio_1h result: " + hitsRatio1hResult)

    // Check 5-minute hit ratio hits_ratio_5m
    def hitsRatio5mResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics where METRIC_NAME = 'hits_ratio_5m' limit 1;"""
    logger.info("hits_ratio_5m result: " + hitsRatio5mResult)

    // Check if all three metrics exist and are greater than 0
    boolean hasHitsRatio = hitsRatioResult.size() > 0 && Double.valueOf(hitsRatioResult[0][0]) > 0
    boolean hasHitsRatio1h = hitsRatio1hResult.size() > 0 && Double.valueOf(hitsRatio1hResult[0][0]) > 0
    boolean hasHitsRatio5m = hitsRatio5mResult.size() > 0 && Double.valueOf(hitsRatio5mResult[0][0]) > 0

    logger.info("Hit ratio metrics check result - hits_ratio: ${hasHitsRatio}, hits_ratio_1h: ${hasHitsRatio1h}, hits_ratio_5m: ${hasHitsRatio5m}")

    // Return false if any metric is false, otherwise return true
    if (!hasHitsRatio) {
        logger.info(HIT_RATIO_METRIC_FALSE_MSG)
        assertTrue(false, HIT_RATIO_METRIC_FALSE_MSG)
    }
    if (!hasHitsRatio1h) {
        logger.info(HIT_RATIO_1H_METRIC_FALSE_MSG)
        assertTrue(false, HIT_RATIO_1H_METRIC_FALSE_MSG)
    }
    if (!hasHitsRatio5m) {
        logger.info(HIT_RATIO_5M_METRIC_FALSE_MSG)
        assertTrue(false, HIT_RATIO_5M_METRIC_FALSE_MSG)
    }
    // ===== End Hit Ratio Metrics Check =====

    // ===== Normal Queue Metrics Check =====
    // Check normal queue current size and max size
    def normalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + normalQueueCurrSizeResult)

    def normalQueueMaxSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'normal_queue_max_size' limit 1;"""
    logger.info("normal_queue_max_size result: " + normalQueueMaxSizeResult)

    // Check normal queue current elements and max elements
    def normalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
    logger.info("normal_queue_curr_elements result: " + normalQueueCurrElementsResult)

    def normalQueueMaxElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'normal_queue_max_elements' limit 1;"""
    logger.info("normal_queue_max_elements result: " + normalQueueMaxElementsResult)

    // Check normal queue size metrics
    boolean hasNormalQueueCurrSize = normalQueueCurrSizeResult.size() > 0 &&
        Double.valueOf(normalQueueCurrSizeResult[0][0]) > 0
    boolean hasNormalQueueMaxSize = normalQueueMaxSizeResult.size() > 0 &&
        Double.valueOf(normalQueueMaxSizeResult[0][0]) > 0
    boolean hasNormalQueueCurrElements = normalQueueCurrElementsResult.size() > 0 &&
        Double.valueOf(normalQueueCurrElementsResult[0][0]) > 0
    boolean hasNormalQueueMaxElements = normalQueueMaxElementsResult.size() > 0 &&
        Double.valueOf(normalQueueMaxElementsResult[0][0]) > 0

    // Check if current size is less than max size and current elements is less than max elements
    boolean normalQueueSizeValid = hasNormalQueueCurrSize && hasNormalQueueMaxSize &&
        Double.valueOf(normalQueueCurrSizeResult[0][0]) < Double.valueOf(normalQueueMaxSizeResult[0][0])
    boolean normalQueueElementsValid = hasNormalQueueCurrElements && hasNormalQueueMaxElements &&
        Double.valueOf(normalQueueCurrElementsResult[0][0]) < Double.valueOf(normalQueueMaxElementsResult[0][0])

    logger.info("Normal queue metrics check result - size valid: ${normalQueueSizeValid}, " +
        "elements valid: ${normalQueueElementsValid}")

    if (!normalQueueSizeValid) {
        logger.info(NORMAL_QUEUE_SIZE_VALIDATION_FAILED_MSG)
        assertTrue(false, NORMAL_QUEUE_SIZE_VALIDATION_FAILED_MSG)
    }
    if (!normalQueueElementsValid) {
        logger.info(NORMAL_QUEUE_ELEMENTS_VALIDATION_FAILED_MSG)
        assertTrue(false, NORMAL_QUEUE_ELEMENTS_VALIDATION_FAILED_MSG)
    }
    // ===== End Normal Queue Metrics Check =====

    // ===== Hit and Read Counts Metrics Check =====
    // Get initial values for hit and read counts
    def initialHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'total_hit_counts' limit 1;"""
    logger.info("Initial total_hit_counts result: " + initialHitCountsResult)

    def initialReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'total_read_counts' limit 1;"""
    logger.info("Initial total_read_counts result: " + initialReadCountsResult)

    // Check if initial values exist and are greater than 0
    if (initialHitCountsResult.size() == 0 || Double.valueOf(initialHitCountsResult[0][0]) <= 0) {
        logger.info(INITIAL_TOTAL_HIT_COUNTS_NOT_GREATER_THAN_0_MSG)
        assertTrue(false, INITIAL_TOTAL_HIT_COUNTS_NOT_GREATER_THAN_0_MSG)
    }
    if (initialReadCountsResult.size() == 0 || Double.valueOf(initialReadCountsResult[0][0]) <= 0) {
        logger.info(INITIAL_TOTAL_READ_COUNTS_NOT_GREATER_THAN_0_MSG)
        assertTrue(false, INITIAL_TOTAL_READ_COUNTS_NOT_GREATER_THAN_0_MSG)
    }

    // Store initial values
    double initialHitCounts = Double.valueOf(initialHitCountsResult[0][0])
    double initialReadCounts = Double.valueOf(initialReadCountsResult[0][0])

    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // Execute the same query to trigger cache operations
    order_qt_2 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table
        where l_orderkey=1 and l_partkey=1534 limit 1;"""

    // Get updated values after cache operations
    def updatedHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'total_hit_counts' limit 1;"""
    logger.info("Updated total_hit_counts result: " + updatedHitCountsResult)

    def updatedReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'total_read_counts' limit 1;"""
    logger.info("Updated total_read_counts result: " + updatedReadCountsResult)

    // Check if updated values are greater than initial values
    double updatedHitCounts = Double.valueOf(updatedHitCountsResult[0][0])
    double updatedReadCounts = Double.valueOf(updatedReadCountsResult[0][0])

    boolean hitCountsIncreased = updatedHitCounts > initialHitCounts
    boolean readCountsIncreased = updatedReadCounts > initialReadCounts

    logger.info("Hit and read counts comparison - hit_counts: ${initialHitCounts} -> " +
        "${updatedHitCounts} (increased: ${hitCountsIncreased}), read_counts: ${initialReadCounts} -> " +
        "${updatedReadCounts} (increased: ${readCountsIncreased})")

    if (!hitCountsIncreased) {
        logger.info(TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(false, TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
    }
    if (!readCountsIncreased) {
        logger.info(TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(false, TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
    }
    // ===== End Hit and Read Counts Metrics Check =====
    sql """set global enable_file_cache=false"""
    return true
}

