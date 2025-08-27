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

// Constants for file cache configuration
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX = "File cache features check failed: "

final String ENABLE_FILE_CACHE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is not set to true"
final String FILE_CACHE_PATH_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_path is empty or not configured"
final String INITIAL_DISK_RESOURCE_LIMIT_MODE_CHECK_FAILED_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial disk_resource_limit_mode does not exist"
final String INITIAL_NEED_EVICT_CACHE_IN_ADVANCE_CHECK_FAILED_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial need_evict_cache_in_advance does not exist"
final String INITIAL_VALUES_NOT_ZERO_CHECK_FAILED_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial values are not both 0 - "
final String DISK_RESOURCE_LIMIT_MODE_TEST_FAILED_MSG = "Disk resource limit mode test failed"
final String NEED_EVICT_CACHE_IN_ADVANCE_TEST_FAILED_MSG = "Need evict cache in advance test failed"

suite("test_file_cache_features", "external_docker,hive,external_docker_hive,p0,external") {
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

    String catalog_name = "test_file_cache_features"
    String ex_db_name = "`tpch1_parquet`"
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
    
    // Check file cache features
    // ===== File Cache Features Metrics Check =====
    // Get initial values for disk resource limit mode and cache eviction advance
    def initialDiskResourceLimitModeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics 
        where METRIC_NAME = 'disk_resource_limit_mode' limit 1;"""
    logger.info("Initial disk_resource_limit_mode result: " + initialDiskResourceLimitModeResult)
    
    def initialNeedEvictCacheInAdvanceResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics 
        where METRIC_NAME = 'need_evict_cache_in_advance' limit 1;"""
    logger.info("Initial need_evict_cache_in_advance result: " + initialNeedEvictCacheInAdvanceResult)
    
    // Check if initial values exist
    if (initialDiskResourceLimitModeResult.size() == 0) {
        logger.info(INITIAL_DISK_RESOURCE_LIMIT_MODE_CHECK_FAILED_MSG)
        assertTrue(false, INITIAL_DISK_RESOURCE_LIMIT_MODE_CHECK_FAILED_MSG)
    }
    if (initialNeedEvictCacheInAdvanceResult.size() == 0) {
        logger.info(INITIAL_NEED_EVICT_CACHE_IN_ADVANCE_CHECK_FAILED_MSG)
        assertTrue(false, INITIAL_NEED_EVICT_CACHE_IN_ADVANCE_CHECK_FAILED_MSG)
    }
    
    // Store initial values
    double initialDiskResourceLimitMode = Double.valueOf(initialDiskResourceLimitModeResult[0][0])
    double initialNeedEvictCacheInAdvance = Double.valueOf(initialNeedEvictCacheInAdvanceResult[0][0])
    
    logger.info("Initial file cache features values - disk_resource_limit_mode: ${initialDiskResourceLimitMode}, " +
        "need_evict_cache_in_advance: ${initialNeedEvictCacheInAdvance}")
    
    // Check if initial values are both 0
    if (initialDiskResourceLimitMode != 0.0 || initialNeedEvictCacheInAdvance != 0.0) {
        logger.info(INITIAL_VALUES_NOT_ZERO_CHECK_FAILED_MSG +
            "disk_resource_limit_mode: ${initialDiskResourceLimitMode}, need_evict_cache_in_advance: ${initialNeedEvictCacheInAdvance}")
        assertTrue(false, INITIAL_VALUES_NOT_ZERO_CHECK_FAILED_MSG +
            "disk_resource_limit_mode: ${initialDiskResourceLimitMode}, need_evict_cache_in_advance: ${initialNeedEvictCacheInAdvance}")
    }
    
    // Set backend configuration parameters for testing
    boolean diskResourceLimitModeTestPassed = true
    setBeConfigTemporary([
        "file_cache_enter_disk_resource_limit_mode_percent": "2",
        "file_cache_exit_disk_resource_limit_mode_percent": "1"
    ]) {
        // Execute test logic with modified configuration
        logger.info("Backend configuration set - file_cache_enter_disk_resource_limit_mode_percent: 2, " +
            "file_cache_exit_disk_resource_limit_mode_percent: 1")
        
        // Wait for disk_resource_limit_mode metric to change to 1
        try {
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until {
                def updatedDiskResourceLimitModeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics 
                    where METRIC_NAME = 'disk_resource_limit_mode' limit 1;"""
                logger.info("Checking disk_resource_limit_mode result: " + updatedDiskResourceLimitModeResult)
                
                if (updatedDiskResourceLimitModeResult.size() > 0) {
                    double updatedDiskResourceLimitMode = Double.valueOf(updatedDiskResourceLimitModeResult[0][0])
                    logger.info("Current disk_resource_limit_mode value: ${updatedDiskResourceLimitMode}")
                    
                    if (updatedDiskResourceLimitMode == 1.0) {
                        logger.info("Disk resource limit mode is now active (value = 1)")
                        return true
                    } else {
                        logger.info("Disk resource limit mode is not yet active (value = ${updatedDiskResourceLimitMode}), waiting...")
                        return false
                    }
                } else {
                    logger.info("Failed to get disk_resource_limit_mode metric, waiting...")
                    return false
                }
            }
        } catch (Exception e) {
            logger.info(DISK_RESOURCE_LIMIT_MODE_TEST_FAILED_MSG + e.getMessage())
            diskResourceLimitModeTestPassed = false
        }
    }
    
    // Check disk resource limit mode test result
    if (!diskResourceLimitModeTestPassed) {
        logger.info(DISK_RESOURCE_LIMIT_MODE_TEST_FAILED_MSG)
        assertTrue(false, DISK_RESOURCE_LIMIT_MODE_TEST_FAILED_MSG)
    }
    
    // Set backend configuration parameters for need_evict_cache_in_advance testing
    boolean needEvictCacheInAdvanceTestPassed = true
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
        
        // Wait for need_evict_cache_in_advance metric to change to 1
        try {
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until {
                def updatedNeedEvictCacheInAdvanceResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics 
                    where METRIC_NAME = 'need_evict_cache_in_advance' limit 1;"""
                logger.info("Checking need_evict_cache_in_advance result: " + updatedNeedEvictCacheInAdvanceResult)
                
                if (updatedNeedEvictCacheInAdvanceResult.size() > 0) {
                    double updatedNeedEvictCacheInAdvance = Double.valueOf(updatedNeedEvictCacheInAdvanceResult[0][0])
                    logger.info("Current need_evict_cache_in_advance value: ${updatedNeedEvictCacheInAdvance}")
                    
                    if (updatedNeedEvictCacheInAdvance == 1.0) {
                        logger.info("Need evict cache in advance mode is now active (value = 1)")
                        return true
                    } else {
                        logger.info("Need evict cache in advance mode is not yet active (value = ${updatedNeedEvictCacheInAdvance}), waiting...")
                        return false
                    }
                } else {
                    logger.info("Failed to get need_evict_cache_in_advance metric, waiting...")
                    return false
                }
            }
        } catch (Exception e) {
            logger.info(NEED_EVICT_CACHE_IN_ADVANCE_TEST_FAILED_MSG + e.getMessage())
            needEvictCacheInAdvanceTestPassed = false
        } 
    }
    
    // Check need evict cache in advance test result
    if (!needEvictCacheInAdvanceTestPassed) {
        logger.info(NEED_EVICT_CACHE_IN_ADVANCE_TEST_FAILED_MSG)
        assertTrue(false, NEED_EVICT_CACHE_IN_ADVANCE_TEST_FAILED_MSG)
    }
    // ===== End File Cache Features Metrics Check =====

    sql """set global enable_file_cache=false"""
    return true
}

