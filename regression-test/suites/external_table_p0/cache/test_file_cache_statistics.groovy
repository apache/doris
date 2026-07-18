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
final String TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "total_hit_counts did not increase after cache operation"
final String TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG = HIT_AND_READ_COUNTS_CHECK_FAILED_PREFIX + "total_read_counts did not increase after cache operation"

suite("test_file_cache_statistics", "p0,external,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    sql """set enable_file_cache=true"""
    sql """set disable_file_cache=false"""
    // This case validates BlockFileCache counters. Keep upper-layer caches and scan scheduling
    // from serving or cancelling the repeated read before it reaches BlockFileCache.
    sql """set enable_sql_cache=false"""
    sql """set enable_hive_sql_cache=false"""
    sql """set enable_parquet_file_page_cache=false"""
    sql """set parallel_pipeline_task_num=1"""

    // Check backend configuration prerequisites
    // Note: This test case assumes a single backend scenario. Testing with single backend is logically equivalent 
    // to testing with multiple backends having identical configurations, but simpler in logic.
    def enableFileCacheResult = sql """show backend config like 'enable_file_cache';"""
    logger.info("enable_file_cache configuration: " + enableFileCacheResult)
    assertFalse(enableFileCacheResult.size() == 0 || !enableFileCacheResult[0][3].equalsIgnoreCase("true"),
            ENABLE_FILE_CACHE_CHECK_FAILED_MSG)
    
    def fileCachePathResult = sql """show backend config like 'file_cache_path';"""
    logger.info("file_cache_path configuration: " + fileCachePathResult)
    assertFalse(fileCachePathResult.size() == 0 || fileCachePathResult[0][3] == null || fileCachePathResult[0][3].trim().isEmpty(),
            FILE_CACHE_PATH_CHECK_FAILED_MSG)

    String catalog_name = "test_file_cache_statistics"
    String ex_db_name = "`default`"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

    def cacheMetric = { String metricName, String aggregateFunc ->
        def r = sql """select ${aggregateFunc}(cast(METRIC_VALUE as double)) from information_schema.file_cache_statistics
                where METRIC_NAME = '${metricName}';"""
        return (r.size() == 0 || r[0][0] == null) ? null : Double.valueOf(r[0][0].toString())
    }
    def cacheMetricSum = { String metricName -> cacheMetric(metricName, "sum") }
    def cacheMetricMax = { String metricName -> cacheMetric(metricName, "max") }

    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'hadoop.username' = 'hive'
    );"""

    sql """switch ${catalog_name}"""

    // Pin the query to one partition file instead of racing all six file scan ranges under LIMIT 1.
    String querySql = """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table
            where nation='cn' and city='beijing'
            and l_orderkey=1 and l_partkey=1534 limit 1;"""

    // load the table into file cache
    sql querySql
    // do it twice to make sure the table block could hit the cache
    order_qt_1 querySql

    def fileCacheBackgroundMonitorIntervalMsResult = sql """show backend config like 'file_cache_background_monitor_interval_ms';"""
    logger.info("file_cache_background_monitor_interval_ms configuration: " + fileCacheBackgroundMonitorIntervalMsResult)
    assertFalse(fileCacheBackgroundMonitorIntervalMsResult.size() == 0 || fileCacheBackgroundMonitorIntervalMsResult[0][3] == null ||
            fileCacheBackgroundMonitorIntervalMsResult[0][3].trim().isEmpty(), "file_cache_background_monitor_interval_ms is empty or not set to true")

    def totalWaitTime = (fileCacheBackgroundMonitorIntervalMsResult[0][3].toInteger() / 1000) as int
    int pollTimeoutSeconds = Math.max(30, totalWaitTime * 6)

    awaitUntil(pollTimeoutSeconds, 1) {
        return cacheMetricMax('hits_ratio') != null && cacheMetricMax('hits_ratio') > 0.0 &&
                cacheMetricMax('hits_ratio_1h') != null && cacheMetricMax('hits_ratio_1h') > 0.0 &&
                cacheMetricMax('hits_ratio_5m') != null && cacheMetricMax('hits_ratio_5m') > 0.0 &&
                cacheMetricSum('normal_queue_curr_size') != null && cacheMetricSum('normal_queue_curr_size') > 0.0 &&
                cacheMetricSum('normal_queue_max_size') != null && cacheMetricSum('normal_queue_max_size') > 0.0 &&
                cacheMetricSum('normal_queue_curr_elements') != null && cacheMetricSum('normal_queue_curr_elements') > 0.0 &&
                cacheMetricSum('normal_queue_max_elements') != null && cacheMetricSum('normal_queue_max_elements') > 0.0 &&
                cacheMetricSum('total_hit_counts') != null && cacheMetricSum('total_hit_counts') > 0.0 &&
                cacheMetricSum('total_read_counts') != null && cacheMetricSum('total_read_counts') > 0.0
    }

    Double hitsRatio = cacheMetricMax('hits_ratio')
    Double hitsRatio1h = cacheMetricMax('hits_ratio_1h')
    Double hitsRatio5m = cacheMetricMax('hits_ratio_5m')
    logger.info("Hit ratio metrics - hits_ratio: ${hitsRatio}, hits_ratio_1h: ${hitsRatio1h}, hits_ratio_5m: ${hitsRatio5m}")
    assertTrue(hitsRatio > 0.0, HIT_RATIO_METRIC_FALSE_MSG)
    assertTrue(hitsRatio1h > 0.0, HIT_RATIO_1H_METRIC_FALSE_MSG)
    assertTrue(hitsRatio5m > 0.0, HIT_RATIO_5M_METRIC_FALSE_MSG)

    Double normalQueueCurrSize = cacheMetricSum('normal_queue_curr_size')
    Double normalQueueMaxSize = cacheMetricSum('normal_queue_max_size')
    Double normalQueueCurrElements = cacheMetricSum('normal_queue_curr_elements')
    Double normalQueueMaxElements = cacheMetricSum('normal_queue_max_elements')
    logger.info("Normal queue metrics - curr_size: ${normalQueueCurrSize}, max_size: ${normalQueueMaxSize}, " +
            "curr_elements: ${normalQueueCurrElements}, max_elements: ${normalQueueMaxElements}")
    assertTrue(normalQueueCurrSize > 0.0 && normalQueueCurrSize < normalQueueMaxSize,
            NORMAL_QUEUE_SIZE_VALIDATION_FAILED_MSG)
    assertTrue(normalQueueCurrElements > 0.0 && normalQueueCurrElements < normalQueueMaxElements,
            NORMAL_QUEUE_ELEMENTS_VALIDATION_FAILED_MSG)

    Double initialHitCounts = cacheMetricSum('total_hit_counts')
    Double initialReadCounts = cacheMetricSum('total_read_counts')
    logger.info("Initial hit/read counts - hit_counts: ${initialHitCounts}, read_counts: ${initialReadCounts}")

    // Execute the same query to trigger cache operations
    order_qt_2 querySql

    awaitUntil(pollTimeoutSeconds, 1) {
        Double updatedHitCounts = cacheMetricSum('total_hit_counts')
        Double updatedReadCounts = cacheMetricSum('total_read_counts')
        return updatedHitCounts != null && updatedHitCounts > initialHitCounts &&
                updatedReadCounts != null && updatedReadCounts > initialReadCounts
    }

    Double updatedHitCounts = cacheMetricSum('total_hit_counts')
    Double updatedReadCounts = cacheMetricSum('total_read_counts')

    logger.info("Hit and read counts comparison - hit_counts: ${initialHitCounts} -> " +
            "${updatedHitCounts}, read_counts: ${initialReadCounts} -> ${updatedReadCounts}")

    assertTrue(updatedHitCounts > initialHitCounts, TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
    assertTrue(updatedReadCounts > initialReadCounts, TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
    return true
}
