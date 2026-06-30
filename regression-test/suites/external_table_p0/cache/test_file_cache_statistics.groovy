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

    // information_schema.file_cache_statistics emits ONE ROW PER (cache_path, metric_name):
    // BE iterates every cache instance in FileCacheFactory::_caches, and a given data file is
    // routed to exactly ONE instance by hash(basename) % num_caches. A bare "... limit 1"
    // therefore inspects an arbitrary single path's counter, which need not be the path the
    // query's data file routed to -- that made the previous total_hit_counts/total_read_counts
    // assertions flaky (the inspected path never moved while the routed path did, a coin flip
    // with >1 cache path). Aggregate across ALL paths with SUM so every metric is path-count
    // agnostic and always includes the routed instance. METRIC_VALUE is a numeric string
    // (std::to_string(double)) so CAST(... AS DOUBLE) is safe.
    def cacheMetricSum = { String metricName ->
        def r = sql """select sum(cast(METRIC_VALUE as double)) from information_schema.file_cache_statistics
                where METRIC_NAME = '${metricName}';"""
        if (r.size() == 0 || r[0][0] == null) {
            return null
        }
        return Double.valueOf(r[0][0].toString())
    }

    // Poll a monitor-published metric until the predicate holds, or until timeout.
    // hits_ratio* and the *_queue_curr_* metrics are refreshed by the BE background monitor on
    // its own cadence (file_cache_background_monitor_interval_ms), so reading them a single fixed
    // interval after the query races the refresh. Awaitility polling waits only as long as needed
    // and avoids reading too soon. On timeout we swallow the exception so the caller's own
    // metric-specific assert below can surface the precise failure message.
    def pollMetric = { String metricName, Closure predicate, long timeoutSeconds ->
        try {
            Awaitility.await()
                    .atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until {
                        def v = cacheMetricSum(metricName)
                        return v != null && predicate(v)
                    }
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
            // fall through; the caller's assert will surface the precise failure
        }
    }

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

    // hits_ratio* and queue-curr metrics are published by the background monitor at most once per
    // monitor interval, so allow polling for a couple of intervals before giving up.
    def monitorIntervalSeconds = Math.max(1, (fileCacheBackgroundMonitorIntervalMsResult[0][3].toInteger() / 1000) as int)
    def metricPollTimeoutSeconds = (monitorIntervalSeconds * 2 + 5) as long

    // ===== Hit Ratio Metrics Check =====
    // hits_ratio / hits_ratio_1h / hits_ratio_5m are monitor-published: poll until each is > 0.
    // SUM across paths is still > 0 when any path reports a positive ratio (each path's ratio is
    // in (0, 1], so the cross-path SUM is strictly positive once published).
    pollMetric('hits_ratio', { it > 0 }, metricPollTimeoutSeconds)
    pollMetric('hits_ratio_1h', { it > 0 }, metricPollTimeoutSeconds)
    pollMetric('hits_ratio_5m', { it > 0 }, metricPollTimeoutSeconds)

    def hitsRatioSum = cacheMetricSum('hits_ratio')
    def hitsRatio1hSum = cacheMetricSum('hits_ratio_1h')
    def hitsRatio5mSum = cacheMetricSum('hits_ratio_5m')
    logger.info("hits_ratio sum: ${hitsRatioSum}, hits_ratio_1h sum: ${hitsRatio1hSum}, hits_ratio_5m sum: ${hitsRatio5mSum}")

    boolean hasHitsRatio = hitsRatioSum != null && hitsRatioSum > 0
    boolean hasHitsRatio1h = hitsRatio1hSum != null && hitsRatio1hSum > 0
    boolean hasHitsRatio5m = hitsRatio5mSum != null && hitsRatio5mSum > 0

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
    // curr_size / curr_elements are monitor-published; poll until populated (> 0) across paths.
    // max_size / max_elements come from the queue's static capacity (not monitor-published), so
    // they are read once without polling. SUM across paths preserves the curr < max inequality
    // (sum of per-path curr < sum of per-path max, since each curr < max).
    pollMetric('normal_queue_curr_size', { it > 0 }, metricPollTimeoutSeconds)
    pollMetric('normal_queue_curr_elements', { it > 0 }, metricPollTimeoutSeconds)

    def normalQueueCurrSizeSum = cacheMetricSum('normal_queue_curr_size')
    logger.info("normal_queue_curr_size sum: " + normalQueueCurrSizeSum)
    def normalQueueMaxSizeSum = cacheMetricSum('normal_queue_max_size')
    logger.info("normal_queue_max_size sum: " + normalQueueMaxSizeSum)
    def normalQueueCurrElementsSum = cacheMetricSum('normal_queue_curr_elements')
    logger.info("normal_queue_curr_elements sum: " + normalQueueCurrElementsSum)
    def normalQueueMaxElementsSum = cacheMetricSum('normal_queue_max_elements')
    logger.info("normal_queue_max_elements sum: " + normalQueueMaxElementsSum)

    boolean hasNormalQueueCurrSize = normalQueueCurrSizeSum != null && normalQueueCurrSizeSum > 0
    boolean hasNormalQueueMaxSize = normalQueueMaxSizeSum != null && normalQueueMaxSizeSum > 0
    boolean hasNormalQueueCurrElements = normalQueueCurrElementsSum != null && normalQueueCurrElementsSum > 0
    boolean hasNormalQueueMaxElements = normalQueueMaxElementsSum != null && normalQueueMaxElementsSum > 0

    // Check if current size is less than max size and current elements is less than max elements
    boolean normalQueueSizeValid = hasNormalQueueCurrSize && hasNormalQueueMaxSize &&
        normalQueueCurrSizeSum < normalQueueMaxSizeSum
    boolean normalQueueElementsValid = hasNormalQueueCurrElements && hasNormalQueueMaxElements &&
        normalQueueCurrElementsSum < normalQueueMaxElementsSum

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
    // total_hit_counts / total_read_counts are LIVE bvar adders (read directly in get_stats(),
    // NOT monitor-published), so no monitor-interval wait is needed here. They are summed across
    // all cache paths above, so the cluster-wide totals are guaranteed to move on any read
    // regardless of which path the data file routes to. Read count increments on every get_or_set
    // (always, even on a miss); hit count increments per already-DOWNLOADED block (cache hit). For
    // external tables the read-cache-file-directly shortcut is not taken, so a re-query always
    // flows through get_or_set and advances both counters when the block is cached.
    Double initialHitCountsBox = cacheMetricSum('total_hit_counts')
    Double initialReadCountsBox = cacheMetricSum('total_read_counts')
    logger.info("Initial total_hit_counts (sum): ${initialHitCountsBox}, total_read_counts (sum): ${initialReadCountsBox}")

    // Check if initial values exist and are greater than 0
    if (initialHitCountsBox == null || initialHitCountsBox <= 0) {
        logger.info(INITIAL_TOTAL_HIT_COUNTS_NOT_GREATER_THAN_0_MSG)
        assertTrue(false, INITIAL_TOTAL_HIT_COUNTS_NOT_GREATER_THAN_0_MSG)
    }
    if (initialReadCountsBox == null || initialReadCountsBox <= 0) {
        logger.info(INITIAL_TOTAL_READ_COUNTS_NOT_GREATER_THAN_0_MSG)
        assertTrue(false, INITIAL_TOTAL_READ_COUNTS_NOT_GREATER_THAN_0_MSG)
    }

    // Store initial values
    double initialHitCounts = initialHitCountsBox
    double initialReadCounts = initialReadCountsBox

    // Execute the same query to trigger cache operations, then poll the live aggregated counters
    // until BOTH increase. The block was just cached by the warm-up queries above and is re-queried
    // promptly here, so it is a cache hit (hit count increases) and is also re-read (read count
    // increases). Re-running the query INSIDE the poll guards against transient bvar visibility lag
    // and against the rare case where the just-cached block was evicted (re-querying re-caches and
    // re-hits it). The inner re-query is a plain sql (not an order_qt), so the golden .out is
    // unaffected. On a working build this typically succeeds on the first re-query.
    order_qt_2 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table
        where l_orderkey=1 and l_partkey=1534 limit 1;"""

    double updatedHitCounts = initialHitCounts
    double updatedReadCounts = initialReadCounts
    try {
        Awaitility.await()
                .atMost(metricPollTimeoutSeconds, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until {
                    // re-run the query each poll so a read+hit is regenerated even if the block was evicted
                    sql """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table
                        where l_orderkey=1 and l_partkey=1534 limit 1;"""
                    Double h = cacheMetricSum('total_hit_counts')
                    Double r = cacheMetricSum('total_read_counts')
                    if (h != null) { updatedHitCounts = h }
                    if (r != null) { updatedReadCounts = r }
                    return h != null && r != null && h > initialHitCounts && r > initialReadCounts
                }
    } catch (org.awaitility.core.ConditionTimeoutException ignored) {
        // fall through; the asserts below surface the precise failure message
    }

    boolean hitCountsIncreased = updatedHitCounts > initialHitCounts
    boolean readCountsIncreased = updatedReadCounts > initialReadCounts

    logger.info("Hit and read counts comparison - hit_counts: ${initialHitCounts} -> " +
        "${updatedHitCounts} (increased: ${hitCountsIncreased}), read_counts: ${initialReadCounts} -> " +
        "${updatedReadCounts} (increased: ${readCountsIncreased})")

    // read count is the robust floor (always increments on get_or_set), so surface it first
    if (!readCountsIncreased) {
        logger.info(TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(false, TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
    }
    if (!hitCountsIncreased) {
        logger.info(TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(false, TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
    }
    // ===== End Hit and Read Counts Metrics Check =====
    sql """set global enable_file_cache=false"""
    return true
}