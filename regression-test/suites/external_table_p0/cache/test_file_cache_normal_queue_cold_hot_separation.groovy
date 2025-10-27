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

import org.codehaus.groovy.runtime.dgmimpl.arrays.LongArrayGetAtMetaMethod

import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

// Constants for backend configuration check
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String ENABLE_FILE_CACHE_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is empty or not set to true"
final String FILE_CACHE_BACKGROUND_MONITOR_INTERVAL_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_background_monitor_interval_ms is empty or not set to true"
final String ENABLE_NORMAL_QUEUE_COLD_HOT_SEPARATION_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_normal_queue_cold_hot_separation is empty or not set to true"
final String WEB_SERVER_PORT_CHECK_FAILED_MSG = BACKEND_CONFIG_CHECK_FAILED_PREFIX + "webserver_port is empty or not configured"
// Constants for cache query features check
final String FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX = "File cache features check failed: "
final String NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is not 0"
final String COLD_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "cold_normal_queue_curr_size is not 0"
final String NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is 0"
final String COLD_NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "cold_normal_queue_curr_size is 0"
final String TOTAL_NORMAL_QUEUE_SIZE_NOT_MATCH_MSG = FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "total normal queue size do not match"

suite("test_file_cache_query_limit", "external_docker,hive,external_docker_hive,p0,external,nonConcurrent") {
    String enableHiveTest = context.config.otherConfigs.get("enableHiveTest")
    if (enableHiveTest == null || !enableHiveTest.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return
    }

    sql """set enable_file_cache=true"""

    // Check backend configuration prerequisites
    // Note: This test case assumes a single backend scenario. Testing with single backend is logically equivalent
    // to testing with multiple backends having identical configurations, but simpler in logic.
    def enableFileCacheResult = sql """show backend config like 'enable_file_cache';"""
    logger.info("enable_file_cache configuration: " + enableFileCacheResult)
    assertFalse(enableFileCacheResult.size() == 0 || !enableFileCacheResult[0][3].equalsIgnoreCase("true"),
            ENABLE_FILE_CACHE_CHECK_FAILED_MSG)

    def fileCacheBackgroundMonitorIntervalMsResult = sql """show backend config like 'file_cache_background_monitor_interval_ms';"""
    logger.info("file_cache_background_monitor_interval_ms configuration: " + fileCacheBackgroundMonitorIntervalMsResult)
    assertFalse(fileCacheBackgroundMonitorIntervalMsResult.size() == 0 || fileCacheBackgroundMonitorIntervalMsResult[0][3] == null ||
            fileCacheBackgroundMonitorIntervalMsResult[0][3].trim().isEmpty(), FILE_CACHE_BACKGROUND_MONITOR_INTERVAL_CHECK_FAILED_MSG)

    def enableNormalQueueColdHotSeparationResult = sql """show backend config like 'enable_normal_queue_cold_hot_separation';"""
    logger.info("enable_normal_queue_cold_hot_separation configuration: " + enableNormalQueueColdHotSeparationResult)
    assertFalse(enableNormalQueueColdHotSeparationResult.size() == 0 || enableNormalQueueColdHotSeparationResult[0][3] == null ||
            enableNormalQueueColdHotSeparationResult[0][3].trim().isEmpty(), ENABLE_NORMAL_QUEUE_COLD_HOT_SEPARATION_CHECK_FAILED_MSG)

    if (enableNormalQueueColdHotSeparationResult.size() == 0 || enableNormalQueueColdHotSeparationResult[0][3] == null ||
                    enableNormalQueueColdHotSeparationResult[0][3].trim().isEmpty()) {
        logger.info(ENABLE_NORMAL_QUEUE_COLD_HOT_SEPARATION_CHECK_FAILED_MSG)
        return
    }

    String catalog_name = "test_file_cache_normal_queue_cold_hot_separation"
    String ex_db_name = "tpch1_parquet"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    int queryCacheCapacity

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
    logger.info("========================= Start executing query sql (1 / 2) ========================")

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

    // brpc metrics will be updated at most 5 seconds
    def totalWaitTime = (fileCacheBackgroundMonitorIntervalMsResult[0][3].toLong() / 1000) as int
    def interval = 1
    def iterations = totalWaitTime / interval

    // Waiting for file cache clearing
    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache clearing ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // Check normal queue and cold normal queue current size
    def normalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + normalQueueCurrSizeResult)
    assertFalse(normalQueueCurrSizeResult.size() == 0 || Double.valueOf(normalQueueCurrSizeResult[0][0]) != 0.0,
            NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG)

    def coldNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'cold_normal_queue_curr_size' limit 1;"""
    logger.info("cold_normal_queue_curr_size result: " + coldNormalQueueCurrSizeResult)
    assertFalse(coldNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(coldNormalQueueCurrSizeResult[0][0]) != 0.0,
            COLD_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG)

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

    normalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + normalQueueCurrSizeResult)
    assertFalse(normalQueueCurrSizeResult.size() == 0 || Double.valueOf(normalQueueCurrSizeResult[0][0]) == 0.0,
            NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG)

    coldNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
        where METRIC_NAME = 'cold_normal_queue_curr_size' limit 1;"""
    logger.info("cold_normal_queue_curr_size result: " + coldNormalQueueCurrSizeResult)
    assertFalse(coldNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(coldNormalQueueCurrSizeResult[0][0]) == 0.0,
            COLD_NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG)

    def totalNormalQueueSize = Double.valueOf(normalQueueCurrSizeResult[0][0]) as Long + Double.valueOf(coldNormalQueueCurrSizeResult[0][0]) as Long

    logger.info("========================== End executing query sql (1 / 2) =========================")

    logger.info("========================= Start executing query sql (2 / 2) ========================")

    // load the table into file cache
    sql query_sql

    // Waiting for file cache statistics update
    (1..iterations).each { count ->
        Thread.sleep(interval * 1000)
        def elapsedSeconds = count * interval
        def remainingSeconds = totalWaitTime - elapsedSeconds
        logger.info("Waited for file cache statistics update ${elapsedSeconds} seconds, ${remainingSeconds} seconds remaining")
    }

    // Check normal queue and cold normal queue current size
    normalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'normal_queue_curr_size' limit 1;"""
    logger.info("normal_queue_curr_size result: " + normalQueueCurrSizeResult)
    assertFalse(normalQueueCurrSizeResult.size() == 0 || Double.valueOf(normalQueueCurrSizeResult[0][0]) == 0.0,
            NORMAL_QUEUE_CURR_SIZE_IS_ZERO_MSG)

    coldNormalQueueCurrSizeResult = sql """select METRIC_VALUE from information_schema.file_cache_statistics
            where METRIC_NAME = 'cold_normal_queue_curr_size' limit 1;"""
    logger.info("cold_normal_queue_curr_size result: " + coldNormalQueueCurrSizeResult)
    assertFalse(coldNormalQueueCurrSizeResult.size() == 0 || Double.valueOf(coldNormalQueueCurrSizeResult[0][0]) != 0.0,
            COLD_NORMAL_QUEUE_CURR_SIZE_NOT_ZERO_MSG)

    assertTrue(totalNormalQueueSize == (Double.valueOf(normalQueueCurrSizeResult[0][0]) as Long), TOTAL_NORMAL_QUEUE_SIZE_NOT_MATCH_MSG)

    logger.info("========================== End executing query sql (2 / 2) =========================")

    return true;
}