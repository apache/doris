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

import org.codehaus.groovy.runtime.dgmimpl.arrays.IntegerArrayGetAtMetaMethod

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

// Constants for backend configuration check
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String ENABLE_FILE_CACHE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is empty or not set to true"
final String FILE_CACHE_PATH_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_path is empty or not configured"
final String WEB_SERVER_PORT_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "webserver_port is empty or not configured"
final String NORMAL_QUEUE_MAX_SIZE_LESS_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "normal_queue_max_size is less than file_cache_query_limit_bytes"
final String ENABLE_FILE_CACHE_QUERY_LIMIT_CHECK_FALSE_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache_query_limit is empty or not set to false"
final String ENABLE_FILE_CACHE_QUERY_LIMIT_CHECK_TRUE_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache_query_limit is empty or not set to true"
final String FILE_CACHE_QUERY_LIMIT_BYTES_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_query_limit_bytes is empty or not configured"
final String FILE_CACHE_QUERY_LIMIT_ENABLE_EVICT_FROM_OTHER_QUEUE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_query_limit_enable_evict_from_other_queue is empty or not set to false"

// Constants for cache query features check
final String FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX = "File cache features check failed: "
final String BASE_NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "base normal_queue_curr_size is 0"
final String BASE_NORMAL_QUEUE_CURR_ELEMENTS_iS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "base normal_queue_curr_elements is 0"
final String TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "total_hit_counts did not increase after cache operation"
final String TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "total_read_counts did not increase after cache operation"
final String INITIAL_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_size is not 0"
final String INITIAL_NORMAL_QUEUE_CURR_ELEMENTS_NOT_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_elements is not 0"
final String INITIAL_NORMAL_QUEUE_MAX_SIZE_IS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_max_size is 0"
final String INITIAL_NORMAL_QUEUE_MAX_ELEMENTS_IS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_max_elements is 0"
final String NORMAL_QUEUE_CURR_SIZE_NOT_GREATER_THAN_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is not greater than 0 after cache operation"
final String NORMAL_QUEUE_CURR_ELEMENTS_NOT_GREATER_THAN_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_elements is not greater than 0 after cache operation"
final String NORMAL_QUEUE_CURR_SIZE_GREATER_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is greater than file_cache_query_limit_bytes"
final String NORMAL_QUEUE_CURR_SIZE_LESS_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is less than file_cache_query_limit_bytes"
final String NORMAL_QUEUE_CURR_SIZE_GREATER_THAN_QUERY_CACHE_CAPACITY_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is greater than query cache capacity"

suite("test_file_cache_query_limit", "external_docker,hive,external_docker_hive,p0,external") {
    String enableHiveTest = context.config.otherConfigs.get("enableHiveTest")
    if (enableHiveTest == null || !enableHiveTest.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return
    }

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

    String catalog_name = "test_file_cache_query_limit"
    String ex_db_name = "tpch1_parquet"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String file_cache_query_limit_bytes
    int divisor = 4
    int queryCacheCapacity

    sql """set enable_file_cache=true"""
    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'hadoop.username' = 'hive'
    );"""

    String query_sql =
            """select sum(l_quantity) as sum_qty,
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

    def webserverPortResult = sql """SHOW BACKEND CONFIG LIKE 'webserver_port';"""
    logger.info("webserver_port configuration: " + webserverPortResult)
    assertFalse(webserverPortResult.size() == 0 || webserverPortResult[0][3] == null || webserverPortResult[0][3].trim().isEmpty(),
            WEB_SERVER_PORT_CHECK_FAILED_MSG)

    String webserver_port = webserverPortResult[0][3]

    // Run file cache base test for setting the parameter file_cache_query_limit_bytes
    logger.info("========================= Start running file cache base test ========================")

    // Clear file cache
    def command = ["curl", "-X", "POST", "${externalEnvIp}:${webserver_port}/api/file_cache?op=clear&sync=true"]
    def stringCommand = command.collect{it.toString()}
    def process = new ProcessBuilder(stringCommand as String[]).redirectErrorStream(true).start()

    def output = new StringBuilder()
    def errorOutput = new StringBuilder()
    process.inputStream.eachLine{line -> output.append(line).append("\n")}
    process.errorStream.eachLine{line -> errorOutput.append(line).append("\n")}

    // Wait for process completion and check exit status
    def exitCode = process.waitFor()
    logger.info("File cache clear command output: ${output.toString()}")
    assertTrue(exitCode == 0, "File cache clear failed with exit code ${exitCode}. Error: ${errorOutput.toString()}")

    // brpc metrics will be updated at most 20 seconds
    def totalWaitTime = 20
    def interval = 5
    def iterations = totalWaitTime / interval

    // Waiting for file cache clearing
    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache clearing ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    def initialNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + initialNormalQueueCurrSizeResult)
    assertFalse(initialNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(initialNormalQueueCurrSizeResult[0][0]) != 0.0,
            INITIAL_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG)

    // Check normal queue current elements
    def initialNormalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
    logger.info("normal_queue_curr_elements result: " + initialNormalQueueCurrElementsResult)
    assertFalse(initialNormalQueueCurrElementsResult.size() == 0 || Double.valueOf(initialNormalQueueCurrElementsResult[0][0]) != 0.0,
            INITIAL_NORMAL_QUEUE_CURR_ELEMENTS_NOT_ZERO_MSG)

    double initialNormalQueueCurrSize = Double.valueOf(initialNormalQueueCurrSizeResult[0][0])
    double initialNormalQueueCurrElements = Double.valueOf(initialNormalQueueCurrElementsResult[0][0])

    logger.info("Initial normal queue curr size and elements - size: ${initialNormalQueueCurrSize} , " +
            "elements: ${initialNormalQueueCurrElements}")

    setBeConfigTemporary([
            "enable_file_cache_query_limit": "false",
            "file_cache_enter_disk_resource_limit_mode_percent": "99",
            "file_cache_exit_disk_resource_limit_mode_percent": "99"
    ]) {
        // Execute test logic with modified configuration for file_cache_query_limit
        logger.info("Backend configuration set - enable_file_cache_query_limit: false")

        // Waiting for backend configuration update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for backend configuration update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        // Check if the configuration is modified
        def enableFileCacheQueryLimitResult = sql """SHOW BACKEND CONFIG LIKE 'enable_file_cache_query_limit';"""
        logger.info("enable_file_cache_query_limit configuration: " + enableFileCacheQueryLimitResult)
        assertFalse(enableFileCacheQueryLimitResult.size() == 0 || enableFileCacheQueryLimitResult[0][3] == null || enableFileCacheQueryLimitResult[0][3] != "false",
                ENABLE_FILE_CACHE_QUERY_LIMIT_CHECK_FALSE_FAILED_MSG)

        sql """switch ${catalog_name}"""
        // load the table into file cache
        sql query_sql

        // Waiting for file cache statistics update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        def baseNormalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
        logger.info("normal_queue_curr_elements result: " + baseNormalQueueCurrElementsResult)
        assertFalse(baseNormalQueueCurrElementsResult.size() == 0 || Double.valueOf(baseNormalQueueCurrElementsResult[0][0]) == 0.0,
                BASE_NORMAL_QUEUE_CURR_ELEMENTS_iS_ZERO_MSG)

        def baseNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
        logger.info("normal_queue_curr_size result: " + baseNormalQueueCurrSizeResult)
        assertFalse(baseNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(baseNormalQueueCurrSizeResult[0][0]) == 0.0,
                BASE_NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG)

        int baseNormalQueueCurrElements = Double.valueOf(baseNormalQueueCurrElementsResult[0][0]) as Integer
        queryCacheCapacity = Double.valueOf(baseNormalQueueCurrSizeResult[0][0]) as Integer

        // The parameter file_cache_query_limit_bytes must be set smaller than the cache capacity required by the query
        file_cache_query_limit_bytes = (((baseNormalQueueCurrElements / divisor) as Integer) * (1024 * 1024)).toString()
        logger.info("file_cache_query_limit_bytes: " + file_cache_query_limit_bytes)
    }

    logger.info("========================== End running file cache base test =========================")

    logger.info("==================== Start running file cache query limit test 1 ====================")

    // Clear file cache
    process = new ProcessBuilder(stringCommand as String[]).redirectErrorStream(true).start()

    output = new StringBuilder()
    errorOutput = new StringBuilder()
    process.inputStream.eachLine{line -> output.append(line).append("\n")}
    process.errorStream.eachLine{line -> errorOutput.append(line).append("\n")}

    // Wait for process completion and check exit status
    exitCode = process.waitFor()
    logger.info("File cache clear command output: ${output.toString()}")
    assertTrue(exitCode == 0, "File cache clear failed with exit code ${exitCode}. Error: ${errorOutput.toString()}")

    // Waiting for file cache clearing
    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache clearing ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // ===== Normal Queue Metrics Check =====
    // Check normal queue current size
    initialNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + initialNormalQueueCurrSizeResult)
    assertFalse(initialNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(initialNormalQueueCurrSizeResult[0][0]) != 0.0,
            INITIAL_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG)

    // Check normal queue current elements
    initialNormalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
    logger.info("normal_queue_curr_elements result: " + initialNormalQueueCurrElementsResult)
    assertFalse(initialNormalQueueCurrElementsResult.size() == 0 || Double.valueOf(initialNormalQueueCurrElementsResult[0][0]) != 0.0,
            INITIAL_NORMAL_QUEUE_CURR_ELEMENTS_NOT_ZERO_MSG)

    // Check normal queue max size
    def initialNormalQueueMaxSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_max_size' limit 1;"""
    logger.info("normal_queue_max_size result: " + initialNormalQueueMaxSizeResult)
    assertFalse(initialNormalQueueMaxSizeResult.size() == 0 || Double.valueOf(initialNormalQueueMaxSizeResult[0][0]) == 0.0,
            INITIAL_NORMAL_QUEUE_MAX_SIZE_IS_ZERO_MSG)

    // Check normal queue max elements
    def initialNormalQueueMaxElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_max_elements' limit 1;"""
    logger.info("normal_queue_max_elements result: " + initialNormalQueueMaxElementsResult)
    assertFalse(initialNormalQueueMaxElementsResult.size() == 0 || Double.valueOf(initialNormalQueueMaxElementsResult[0][0]) == 0.0,
            INITIAL_NORMAL_QUEUE_MAX_ELEMENTS_IS_ZERO_MSG)

    initialNormalQueueCurrSize = Double.valueOf(initialNormalQueueCurrSizeResult[0][0])
    initialNormalQueueCurrElements = Double.valueOf(initialNormalQueueCurrElementsResult[0][0])
    double initialNormalQueueMaxSize = Double.valueOf(initialNormalQueueMaxSizeResult[0][0])
    double initialNormalQueueMaxElements = Double.valueOf(initialNormalQueueMaxElementsResult[0][0])

    logger.info("Initial normal queue curr size and elements - size: ${initialNormalQueueCurrSize} , " +
                "elements: ${initialNormalQueueCurrElements}")

    logger.info("Initial normal queue max size and elements - size: ${initialNormalQueueMaxSize} , " +
                "elements: ${initialNormalQueueMaxElements}")

    // Check file_cache_query_limit_bytes is less than normal_queue_max_size
    assertTrue((initialNormalQueueMaxSize as Integer) > file_cache_query_limit_bytes.toInteger(),
            NORMAL_QUEUE_MAX_SIZE_LESS_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG)

    // ===== Hit And Read Counts Metrics Check =====
    // Get initial values for hit and read counts
    def initialTotalHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'total_hit_counts' limit 1;"""
    logger.info("Initial total_hit_counts result: " + initialTotalHitCountsResult)

    def initialTotalReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'total_read_counts' limit 1;"""
    logger.info("Initial total_read_counts result: " + initialTotalReadCountsResult)

    // Store initial values
    double initialTotalHitCounts = Double.valueOf(initialTotalHitCountsResult[0][0])
    double initialTotalReadCounts = Double.valueOf(initialTotalReadCountsResult[0][0])

    // Set backend configuration parameters for file_cache_query_limit test 1
    setBeConfigTemporary([
            "enable_file_cache_query_limit": "true",
            "file_cache_query_limit_bytes": file_cache_query_limit_bytes,
            "file_cache_query_limit_enable_evict_from_other_queue": "false",
            "file_cache_enter_disk_resource_limit_mode_percent": "99",
            "file_cache_exit_disk_resource_limit_mode_percent": "99"
    ]) {
        // Execute test logic with modified configuration for file_cache_query_limit
        logger.info("Backend configuration set - enable_file_cache_query_limit: true , " +
                "file_cache_query_limit_bytes: ${file_cache_query_limit_bytes} , " +
                "file_cache_query_limit_enable_evict_from_other_queue: false")

        // Waiting for backend configuration update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for backend configuration update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        // Check if the configuration is modified
        def enableFileCacheQueryLimitResult = sql """SHOW BACKEND CONFIG LIKE 'enable_file_cache_query_limit';"""
        logger.info("enable_file_cache_query_limit configuration: " + enableFileCacheQueryLimitResult)
        assertFalse(enableFileCacheQueryLimitResult.size() == 0 || enableFileCacheQueryLimitResult[0][3] == null || enableFileCacheQueryLimitResult[0][3] != "true",
                ENABLE_FILE_CACHE_QUERY_LIMIT_CHECK_TRUE_FAILED_MSG)

        def fileCacheQueryLimitBytesResult = sql """SHOW BACKEND CONFIG LIKE 'file_cache_query_limit_bytes';"""
        logger.info("file_cache_query_limit_bytes configuration: " + fileCacheQueryLimitBytesResult)
        assertFalse(fileCacheQueryLimitBytesResult.size() == 0 || fileCacheQueryLimitBytesResult[0][3] == null || fileCacheQueryLimitBytesResult[0][3]  != file_cache_query_limit_bytes,
                FILE_CACHE_QUERY_LIMIT_BYTES_CHECK_FAILED_MSG)

        def fileCacheQueryLimitEnableEvictFromOtherQueueResult = sql """SHOW BACKEND CONFIG LIKE 'file_cache_query_limit_enable_evict_from_other_queue';"""
        logger.info("file_cache_query_limit_enable_evict_from_other_queue configuration: " + fileCacheQueryLimitEnableEvictFromOtherQueueResult)
        assertFalse(fileCacheQueryLimitEnableEvictFromOtherQueueResult.size() == 0 || fileCacheQueryLimitEnableEvictFromOtherQueueResult[0][3] == null || fileCacheQueryLimitEnableEvictFromOtherQueueResult[0][3] != "false",
                FILE_CACHE_QUERY_LIMIT_ENABLE_EVICT_FROM_OTHER_QUEUE_CHECK_FAILED_MSG)

        sql """switch ${catalog_name}"""

        // load the table into file cache
        sql query_sql

        // Waiting for file cache statistics update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        // Get updated value of normal queue current elements and max elements after cache operations
        def updatedNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
        logger.info("normal_queue_curr_size result: " + updatedNormalQueueCurrSizeResult)

        def updatedNormalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
        logger.info("normal_queue_curr_elements result: " + updatedNormalQueueCurrElementsResult)

        // Check if updated values are greater than initial values
        double updatedNormalQueueCurrSize = Double.valueOf(updatedNormalQueueCurrSizeResult[0][0])
        double updatedNormalQueueCurrElements = Double.valueOf(updatedNormalQueueCurrElementsResult[0][0])

        logger.info("Updated normal queue curr size and elements - size: ${updatedNormalQueueCurrSize} , " +
                "elements: ${updatedNormalQueueCurrElements}")

        assertTrue(updatedNormalQueueCurrSize > 0.0, NORMAL_QUEUE_CURR_SIZE_NOT_GREATER_THAN_ZERO_MSG)
        assertTrue(updatedNormalQueueCurrElements > 0.0, NORMAL_QUEUE_CURR_ELEMENTS_NOT_GREATER_THAN_ZERO_MSG)

        logger.info("Normal queue curr size and file cache query limit bytes comparison - normal queue curr size: ${(updatedNormalQueueCurrSize as Integer)} , " +
                "file cache query limit bytes: ${file_cache_query_limit_bytes.toInteger()}")

        assertTrue(((updatedNormalQueueCurrSize as Integer) <= file_cache_query_limit_bytes.toInteger()),
                NORMAL_QUEUE_CURR_SIZE_GREATER_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG)

        // Get updated values for hit and read counts after cache operations
        def updatedTotalHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'total_hit_counts' limit 1;"""
        logger.info("Initial total_hit_counts result: " + updatedTotalHitCountsResult)

        def updatedTotalReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'total_read_counts' limit 1;"""
        logger.info("Initial total_read_counts result: " + updatedTotalReadCountsResult)

        // Check if updated values are greater than initial values
        double updatedTotalHitCounts = Double.valueOf(updatedTotalHitCountsResult[0][0])
        double updatedTotalReadCounts = Double.valueOf(updatedTotalReadCountsResult[0][0])

        logger.info("Total hit and read counts comparison - hit counts: ${initialTotalHitCounts} -> " +
                "${updatedTotalHitCounts} , read counts: ${initialTotalReadCounts} -> ${updatedTotalReadCounts}")

        assertTrue(updatedTotalHitCounts > initialTotalHitCounts, TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(updatedTotalReadCounts > initialTotalReadCounts, TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
    }

    logger.info("===================== End running file cache query limit test 1 =====================")


    logger.info("==================== Start running file cache query limit test 2 ====================")

    // Clear file cache
    process = new ProcessBuilder(stringCommand as String[]).redirectErrorStream(true).start()

    output = new StringBuilder()
    errorOutput = new StringBuilder()
    process.inputStream.eachLine{line -> output.append(line).append("\n")}
    process.errorStream.eachLine{line -> errorOutput.append(line).append("\n")}

    // Wait for process completion and check exit status
    exitCode = process.waitFor()
    logger.info("File cache clear command output: ${output.toString()}")
    assertTrue(exitCode == 0, "File cache clear failed with exit code ${exitCode}. Error: ${errorOutput.toString()}")

    // Waiting for file cache clearing
    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache clearing ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // ===== Hit And Read Counts Metrics Check =====
    // Get initial values for hit and read counts
    initialTotalHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'total_hit_counts' limit 1;"""
    logger.info("Initial total_hit_counts result: " + initialTotalHitCountsResult)

    initialTotalReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'total_read_counts' limit 1;"""
    logger.info("Initial total_read_counts result: " + initialTotalReadCountsResult)

    // Store initial values
    initialTotalHitCounts = Double.valueOf(initialTotalHitCountsResult[0][0])
    initialTotalReadCounts = Double.valueOf(initialTotalReadCountsResult[0][0])

    // Set backend configuration parameters for file_cache_query_limit test 2
    setBeConfigTemporary([
            "enable_file_cache_query_limit": "true",
            "file_cache_query_limit_bytes": file_cache_query_limit_bytes,
            "file_cache_query_limit_enable_evict_from_other_queue": "true",
            "file_cache_enter_disk_resource_limit_mode_percent": "99",
            "file_cache_exit_disk_resource_limit_mode_percent": "99"
    ]) {
        // Execute test logic with modified configuration for file_cache_query_limit
        logger.info("Backend configuration set - enable_file_cache_query_limit: true , " +
                "file_cache_query_limit_bytes: ${file_cache_query_limit_bytes} , " +
                "file_cache_query_limit_enable_evict_from_other_queue: true")

        // Waiting for backend configuration update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for backend configuration update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        // Check if the configuration is modified
        def enableFileCacheQueryLimitResult = sql """SHOW BACKEND CONFIG LIKE 'enable_file_cache_query_limit';"""
        logger.info("enable_file_cache_query_limit configuration: " + enableFileCacheQueryLimitResult)
        assertFalse(enableFileCacheQueryLimitResult.size() == 0 || enableFileCacheQueryLimitResult[0][3] == null || enableFileCacheQueryLimitResult[0][3] != "true",
                ENABLE_FILE_CACHE_QUERY_LIMIT_CHECK_TRUE_FAILED_MSG)

        def fileCacheQueryLimitBytesResult = sql """SHOW BACKEND CONFIG LIKE 'file_cache_query_limit_bytes';"""
        logger.info("file_cache_query_limit_bytes configuration: " + fileCacheQueryLimitBytesResult)
        assertFalse(fileCacheQueryLimitBytesResult.size() == 0 || fileCacheQueryLimitBytesResult[0][3] == null || fileCacheQueryLimitBytesResult[0][3]  != file_cache_query_limit_bytes,
                FILE_CACHE_QUERY_LIMIT_BYTES_CHECK_FAILED_MSG)

        def fileCacheQueryLimitEnableEvictFromOtherQueueResult = sql """SHOW BACKEND CONFIG LIKE 'file_cache_query_limit_enable_evict_from_other_queue';"""
        logger.info("file_cache_query_limit_enable_evict_from_other_queue configuration: " + fileCacheQueryLimitEnableEvictFromOtherQueueResult)
        assertFalse(fileCacheQueryLimitEnableEvictFromOtherQueueResult.size() == 0 || fileCacheQueryLimitEnableEvictFromOtherQueueResult[0][3] == null || fileCacheQueryLimitEnableEvictFromOtherQueueResult[0][3] != "true",
                FILE_CACHE_QUERY_LIMIT_ENABLE_EVICT_FROM_OTHER_QUEUE_CHECK_FAILED_MSG)

        sql """switch ${catalog_name}"""

        // load the table into file cache
        sql query_sql

        // Waiting for file cache statistics update
        (1..iterations).each { count ->
            Thread.sleep(interval * 1000)
            def elapsedSeconds = count * interval
            def remainingSeconds = totalWaitTime - elapsedSeconds
            logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
        }

        // Get updated value of normal queue current elements and max elements after cache operations
        def updatedNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
        logger.info("normal_queue_curr_size result: " + updatedNormalQueueCurrSizeResult)

        def updatedNormalQueueCurrElementsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'normal_queue_curr_elements' limit 1;"""
        logger.info("normal_queue_curr_elements result: " + updatedNormalQueueCurrElementsResult)

        // Check if updated values are greater than initial values
        double updatedNormalQueueCurrSize = Double.valueOf(updatedNormalQueueCurrSizeResult[0][0])
        double updatedNormalQueueCurrElements = Double.valueOf(updatedNormalQueueCurrElementsResult[0][0])

        logger.info("Updated normal queue curr size and elements - size: ${updatedNormalQueueCurrSize} , " +
                "elements: ${updatedNormalQueueCurrElements}")

        assertTrue(updatedNormalQueueCurrSize > 0.0, NORMAL_QUEUE_CURR_SIZE_NOT_GREATER_THAN_ZERO_MSG)
        assertTrue(updatedNormalQueueCurrElements > 0.0, NORMAL_QUEUE_CURR_ELEMENTS_NOT_GREATER_THAN_ZERO_MSG)

        logger.info("Normal queue curr size and file cache query limit bytes comparison - normal queue curr size: ${(updatedNormalQueueCurrSize as Integer)} , " +
                "file cache query limit bytes: ${file_cache_query_limit_bytes.toInteger()}")
        assertTrue(((updatedNormalQueueCurrSize as Integer) >= file_cache_query_limit_bytes.toInteger()),
                NORMAL_QUEUE_CURR_SIZE_LESS_THAN_FILE_CACHE_QUERY_LIMIT_BYTES_MSG)

        logger.info("Normal queue curr size and query cache capacity comparison - normal queue curr size: ${(updatedNormalQueueCurrSize as Integer)} , " +
                "query cache capacity: ${queryCacheCapacity}")
        assertTrue(((updatedNormalQueueCurrSize as Integer) <= queryCacheCapacity),
                NORMAL_QUEUE_CURR_SIZE_GREATER_THAN_QUERY_CACHE_CAPACITY_MSG)

        // Get updated values for hit and read counts after cache operations
        def updatedTotalHitCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'total_hit_counts' limit 1;"""
        logger.info("Initial total_hit_counts result: " + updatedTotalHitCountsResult)

        def updatedTotalReadCountsResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
                where METRIC_NAME = 'total_read_counts' limit 1;"""
        logger.info("Initial total_read_counts result: " + updatedTotalReadCountsResult)

        // Check if updated values are greater than initial values
        double updatedTotalHitCounts = Double.valueOf(updatedTotalHitCountsResult[0][0])
        double updatedTotalReadCounts = Double.valueOf(updatedTotalReadCountsResult[0][0])

        assertTrue(updatedTotalHitCounts > initialTotalHitCounts, TOTAL_HIT_COUNTS_DID_NOT_INCREASE_MSG)
        assertTrue(updatedTotalReadCounts > initialTotalReadCounts, TOTAL_READ_COUNTS_DID_NOT_INCREASE_MSG)
    }

    logger.info("===================== End running file cache query limit test 2 =====================")

    return true;
}
