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

// Constants for file cache configuration
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX = "File cache features check failed: "

final String ENABLE_FILE_CACHE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is not set to true"
final String FILE_CACHE_PATH_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_path is empty or not configured"
final String INITIAL_DISK_RESOURCE_LIMIT_MODE_CHECK_FAILED_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial disk_resource_limit_mode does not exist"
final String INITIAL_NEED_EVICT_CACHE_IN_ADVANCE_CHECK_FAILED_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial need_evict_cache_in_advance does not exist"

suite("test_file_cache_features", "p0,external,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    sql """set enable_file_cache=true"""
    sql """set disable_file_cache=false"""

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

    String catalog_name = "test_file_cache_features"
    String ex_db_name = "`tpch1_parquet`"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

    def cacheMetricMax = { String metricName ->
        def r = sql """select max(cast(METRIC_VALUE as double)) from information_schema.file_cache_statistics
                where METRIC_NAME = '${metricName}';"""
        return (r.size() == 0 || r[0][0] == null) ? null : Double.valueOf(r[0][0].toString())
    }

    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'hadoop.username' = 'hive'
    );"""

    sql """switch ${catalog_name}"""
    sql """select l_returnflag, l_linestatus, 
        sum(l_quantity) as sum_qty, 
        sum(l_extendedprice) as sum_base_price, 
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, 
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, 
        avg(l_quantity) as avg_qty, 
        avg(l_extendedprice) as avg_price, 
        avg(l_discount) as avg_disc, 
        count(*) as count_order 
    from ${catalog_name}.${ex_db_name}.lineitem 
    where l_shipdate <= date '1998-12-01' - interval '90' day 
    group by l_returnflag, l_linestatus 
    order by l_returnflag, l_linestatus;"""
    
    def fileCacheBackgroundMonitorIntervalMsResult = sql """show backend config like 'file_cache_background_monitor_interval_ms';"""
    logger.info("file_cache_background_monitor_interval_ms configuration: " + fileCacheBackgroundMonitorIntervalMsResult)
    assertFalse(fileCacheBackgroundMonitorIntervalMsResult.size() == 0 || fileCacheBackgroundMonitorIntervalMsResult[0][3] == null ||
            fileCacheBackgroundMonitorIntervalMsResult[0][3].trim().isEmpty(), "file_cache_background_monitor_interval_ms is empty or not set to true")

    def totalWaitTime = (fileCacheBackgroundMonitorIntervalMsResult[0][3].toInteger() / 1000) as int
    int pollTimeoutSeconds = Math.max(30, totalWaitTime * 6)

    awaitUntil(pollTimeoutSeconds, 1) {
        return cacheMetricMax('disk_resource_limit_mode') != null &&
                cacheMetricMax('need_evict_cache_in_advance') != null &&
                cacheMetricMax('disk_resource_limit_mode') == 0.0 &&
                cacheMetricMax('need_evict_cache_in_advance') == 0.0
    }

    double initialDiskResourceLimitMode = cacheMetricMax('disk_resource_limit_mode')
    double initialNeedEvictCacheInAdvance = cacheMetricMax('need_evict_cache_in_advance')
    logger.info("Initial file cache features values - disk_resource_limit_mode: ${initialDiskResourceLimitMode}, " +
            "need_evict_cache_in_advance: ${initialNeedEvictCacheInAdvance}")
    assertTrue(initialDiskResourceLimitMode == 0.0, INITIAL_DISK_RESOURCE_LIMIT_MODE_CHECK_FAILED_MSG)
    assertTrue(initialNeedEvictCacheInAdvance == 0.0, INITIAL_NEED_EVICT_CACHE_IN_ADVANCE_CHECK_FAILED_MSG)

    // Set backend configuration parameters for testing
    setBeConfigTemporary([
        "file_cache_enter_disk_resource_limit_mode_percent": "2",
        "file_cache_exit_disk_resource_limit_mode_percent": "1"
    ]) {
        // Execute test logic with modified configuration
        logger.info("Backend configuration set - file_cache_enter_disk_resource_limit_mode_percent: 2, " +
            "file_cache_exit_disk_resource_limit_mode_percent: 1")

        awaitUntil(pollTimeoutSeconds, 1) {
            return cacheMetricMax('disk_resource_limit_mode') != null &&
                    cacheMetricMax('disk_resource_limit_mode') == 1.0
        }
        logger.info("Disk resource limit mode is now active (value = ${cacheMetricMax('disk_resource_limit_mode')})")
    }

    awaitUntil(pollTimeoutSeconds, 1) {
        return cacheMetricMax('disk_resource_limit_mode') != null &&
                cacheMetricMax('disk_resource_limit_mode') == 0.0
    }
    
    // Set backend configuration parameters for need_evict_cache_in_advance testing
    setBeConfigTemporary([
        "enable_evict_file_cache_in_advance": "true",
        "file_cache_enter_need_evict_cache_in_advance_percent": "2",
        "file_cache_exit_need_evict_cache_in_advance_percent": "1"
    ]) {
        // Execute test logic with modified configuration for need_evict_cache_in_advance
        logger.info("Backend configuration set for need_evict_cache_in_advance - " +
            "enable_evict_file_cache_in_advance: true, " +
            "file_cache_enter_need_evict_cache_in_advance_percent: 2, " +
            "file_cache_exit_need_evict_cache_in_advance_percent: 1")

        awaitUntil(pollTimeoutSeconds, 1) {
            return cacheMetricMax('need_evict_cache_in_advance') != null &&
                    cacheMetricMax('need_evict_cache_in_advance') == 1.0
        }
        logger.info("Need evict cache in advance mode is now active (value = ${cacheMetricMax('need_evict_cache_in_advance')})")
    }

    awaitUntil(pollTimeoutSeconds, 1) {
        return cacheMetricMax('need_evict_cache_in_advance') != null &&
                cacheMetricMax('need_evict_cache_in_advance') == 0.0
    }

    return true
}
