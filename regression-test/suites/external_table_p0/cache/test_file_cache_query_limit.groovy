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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

// Keep only reusable prefixes and inline one-off failure messages near assertions.
final String BACKEND_CONFIG_CHECK_FAILED_PREFIX = "Backend configuration check failed: "
final String FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX = "File cache features check failed: "

suite("test_file_cache_query_limit", "p0,external") {
    String enableHiveTest = context.config.otherConfigs.get("enableHiveTest")
    if (enableHiveTest == null || !enableHiveTest.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return
    }

    sql """set enable_file_cache=true"""

    def enableFileCacheResult = sql """show backend config like 'enable_file_cache';"""
    logger.info("enable_file_cache configuration: " + enableFileCacheResult)
    assertFalse(enableFileCacheResult.size() == 0 || !enableFileCacheResult[0][3].equalsIgnoreCase("true"),
            BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache is empty or not set to true")

    def fileCacheBackgroundMonitorIntervalMsResult = sql """show backend config like 'file_cache_background_monitor_interval_ms';"""
    logger.info("file_cache_background_monitor_interval_ms configuration: " + fileCacheBackgroundMonitorIntervalMsResult)
    assertFalse(fileCacheBackgroundMonitorIntervalMsResult.size() == 0 || fileCacheBackgroundMonitorIntervalMsResult[0][3] == null ||
            fileCacheBackgroundMonitorIntervalMsResult[0][3].trim().isEmpty(),
            BACKEND_CONFIG_CHECK_FAILED_PREFIX + "file_cache_background_monitor_interval_ms is empty or not configured")

    long backgroundMonitorIntervalMs = fileCacheBackgroundMonitorIntervalMsResult[0][3].toLong()
    long waitTimeoutSeconds = Math.max(10L, backgroundMonitorIntervalMs.intdiv(1000L) + 5L)

    String catalogName = "test_file_cache_query_limit"
    String externalDbName = "tpch1_parquet"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    long baseQueryCacheUsageBytes = 0L

    // Discover the real Doris backend endpoints
    def aliveBackends = sql_return_maparray("show backends").findAll { backend ->
        backend.Alive.toString().equalsIgnoreCase("true")
    }
    logger.info("alive backends: " + aliveBackends)
    assertTrue(aliveBackends.size() == 1,
            BACKEND_CONFIG_CHECK_FAILED_PREFIX + "query limit test requires exactly one alive backend: " + aliveBackends)
    def targetBackend = aliveBackends[0]
    String targetBeIp = targetBackend.Host.toString()
    String targetHttpPort = targetBackend.HttpPort.toString()
    String targetBrpcPort = targetBackend.BrpcPort.toString()

    // Reuse the same command execution path for every backend HTTP request.
    def runCommandAndCollectOutput = { List<String> command ->
        def process = new ProcessBuilder(command as String[]).redirectErrorStream(true).start()
        def output = new StringBuilder()
        process.inputStream.eachLine { line -> output.append(line).append("\n") }
        int exitCode = process.waitFor()
        return [exitCode: exitCode, output: output.toString()]
    }

    // Aggregate metrics across all cache paths on the target backend to avoid flaky single-row reads.
    def getMetricSum = { String metricName ->
        def result = sql """select coalesce(sum(cast(METRIC_VALUE as double)), 0)
                from information_schema.file_cache_statistics
                where BE_IP = '${targetBeIp}' and METRIC_NAME = '${metricName}';"""
        double metricValue = Double.valueOf(result[0][0].toString())
        logger.info("metric ${metricName} on backend ${targetBeIp}: ${metricValue}")
        return metricValue
    }

    // Poll until asynchronous backend state converges to the expected condition.
    def waitForCondition = { String description, Closure<Boolean> condition ->
        Awaitility.await()
                .atMost(waitTimeoutSeconds, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until {
                    boolean matched = condition()
                    if (!matched) {
                        logger.info("Waiting for ${description}")
                    }
                    return matched
                }
    }

    // Read backend config through SQL and return the raw value for later assertions.
    def getBackendConfigValue = { String configKey ->
        def result = sql """SHOW BACKEND CONFIG LIKE '${configKey}';"""
        logger.info("${configKey} configuration: " + result)
        assertFalse(result.size() == 0 || result[0][3] == null || result[0][3].toString().trim().isEmpty(),
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "${configKey} is empty or not configured")
        return result[0][3].toString().trim()
    }

    // Clear file cache synchronously and wait until the observable queue metrics return to zero.
    def clearFileCacheAndWait = {
        def commandResult = runCommandAndCollectOutput([
                "curl", "-X", "POST", "http://${targetBeIp}:${targetHttpPort}/api/file_cache?op=clear&sync=true"
        ])
        logger.info("File cache clear command output: ${commandResult.output}")
        assertTrue(commandResult.exitCode == 0,
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "HTTP command failed: clear file cache failed with exit code ${commandResult.exitCode}")
        waitForCondition("file cache clear on ${targetBeIp}") {
            getMetricSum("normal_queue_curr_size") == 0.0 &&
                    getMetricSum("normal_queue_curr_elements") == 0.0
        }
    }

    // Fetch the total cache capacity from the target backend brpc vars endpoint.
    def getFileCacheCapacity = {
        def commandResult = runCommandAndCollectOutput([
                "curl", "-X", "POST", "http://${targetBeIp}:${targetBrpcPort}/vars"
        ])
        logger.info("file cache vars output: ${commandResult.output}")
        assertTrue(commandResult.exitCode == 0,
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "HTTP command failed: fetch brpc vars failed with exit code ${commandResult.exitCode}")
        def fileCacheCapacityResult = commandResult.output.split("\n")
                .find { it.contains("file_cache_capacity") }
                ?.split(":")
                ?.last()
                ?.trim()
        assertTrue(fileCacheCapacityResult != null,
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "failed to find file_cache_capacity in brpc metrics")
        return Long.valueOf(fileCacheCapacityResult)
    }

    sql """drop catalog if exists ${catalogName} """

    sql """CREATE CATALOG ${catalogName} PROPERTIES (
        'type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
        'hadoop.username' = 'hive'
    );"""

    String querySql =
            """select sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
            from ${catalogName}.${externalDbName}.lineitem
            where l_shipdate <= date '1998-12-01' - interval '90' day
            group by l_returnflag, l_linestatus
            order by l_returnflag, l_linestatus;"""

    long fileCacheCapacity = getFileCacheCapacity()
    logger.info("File cache capacity: ${fileCacheCapacity}")

    // Build a baseline without query limit so the next stage can derive a meaningful limit percent.
    logger.info("Start file cache base test")

    clearFileCacheAndWait()

    double initialNormalQueueCurrSize = getMetricSum("normal_queue_curr_size")
    assertTrue(initialNormalQueueCurrSize == 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_size is not 0")

    double initialNormalQueueCurrElements = getMetricSum("normal_queue_curr_elements")
    assertTrue(initialNormalQueueCurrElements == 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_elements is not 0")

    logger.info("Initial normal queue curr size and elements - size: ${initialNormalQueueCurrSize} , elements: ${initialNormalQueueCurrElements}")

    setBeConfigTemporary([
            "enable_file_cache_query_limit": "false"
    ]) {
        logger.info("Backend configuration set - enable_file_cache_query_limit: false")

        waitForCondition("backend config enable_file_cache_query_limit=false") {
            getBackendConfigValue("enable_file_cache_query_limit") == "false"
        }

        assertEquals("false", getBackendConfigValue("enable_file_cache_query_limit"),
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache_query_limit is empty or not set to false")

        sql """switch ${catalogName}"""
        sql querySql

        waitForCondition("base file cache statistics update") {
            getMetricSum("normal_queue_curr_size") > 0.0 &&
                    getMetricSum("normal_queue_curr_elements") > 0.0
        }

        double baseNormalQueueCurrElements = getMetricSum("normal_queue_curr_elements")
        assertTrue(baseNormalQueueCurrElements > 0.0,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "base normal_queue_curr_elements is 0")

        double baseNormalQueueCurrSize = getMetricSum("normal_queue_curr_size")
        assertTrue(baseNormalQueueCurrSize > 0.0,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "base normal_queue_curr_size is 0")

        baseQueryCacheUsageBytes = baseNormalQueueCurrSize as Long
    }

    double fileCacheQueryLimitPercent = baseQueryCacheUsageBytes * 100.0d / fileCacheCapacity
    logger.info("file_cache_query_limit_percent: " + fileCacheQueryLimitPercent)

    // Re-run the same query with a lower per-query cache limit and verify the observed cache usage stays bounded.
    logger.info("Start file cache query limit test")

    long fileCacheQueryLimitPercentCandidate = Math.floor(fileCacheQueryLimitPercent / 2.0d).longValue()
    long fileCacheQueryLimitPercentTest1 = Math.max(1L, fileCacheQueryLimitPercentCandidate)
    logger.info("file_cache_query_limit_percent_test1: " + fileCacheQueryLimitPercentTest1)

    clearFileCacheAndWait()

    initialNormalQueueCurrSize = getMetricSum("normal_queue_curr_size")
    assertTrue(initialNormalQueueCurrSize == 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_size is not 0")

    initialNormalQueueCurrElements = getMetricSum("normal_queue_curr_elements")
    assertTrue(initialNormalQueueCurrElements == 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_curr_elements is not 0")

    double initialNormalQueueMaxSize = getMetricSum("normal_queue_max_size")
    assertTrue(initialNormalQueueMaxSize > 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_max_size is 0")

    double initialNormalQueueMaxElements = getMetricSum("normal_queue_max_elements")
    assertTrue(initialNormalQueueMaxElements > 0.0,
            FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "initial normal_queue_max_elements is 0")

    logger.info("Initial normal queue curr size and elements - size: ${initialNormalQueueCurrSize} , elements: ${initialNormalQueueCurrElements}")
    logger.info("Initial normal queue max size and elements - size: ${initialNormalQueueMaxSize} , elements: ${initialNormalQueueMaxElements}")

    double initialTotalHitCounts = getMetricSum("total_hit_counts")
    double initialTotalReadCounts = getMetricSum("total_read_counts")

    setBeConfigTemporary([
            "enable_file_cache_query_limit": "true"
    ]) {
        logger.info("Backend configuration set - enable_file_cache_query_limit: true")

        sql """set file_cache_query_limit_percent = ${fileCacheQueryLimitPercentTest1}"""

        waitForCondition("backend config enable_file_cache_query_limit=true") {
            getBackendConfigValue("enable_file_cache_query_limit") == "true"
        }

        assertEquals("true", getBackendConfigValue("enable_file_cache_query_limit"),
                BACKEND_CONFIG_CHECK_FAILED_PREFIX + "enable_file_cache_query_limit is empty or not set to true")

        sql """switch ${catalogName}"""
        sql querySql

        waitForCondition("query limit file cache statistics update") {
            getMetricSum("normal_queue_curr_size") > 0.0 &&
                    getMetricSum("normal_queue_curr_elements") > 0.0 &&
                    getMetricSum("total_read_counts") > initialTotalReadCounts
        }

        double updatedNormalQueueCurrSize = getMetricSum("normal_queue_curr_size")
        double updatedNormalQueueCurrElements = getMetricSum("normal_queue_curr_elements")

        logger.info("Updated normal queue curr size and elements - size: ${updatedNormalQueueCurrSize} , elements: ${updatedNormalQueueCurrElements}")

        assertTrue(updatedNormalQueueCurrSize > 0.0,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is not greater than 0 after cache operation")
        assertTrue(updatedNormalQueueCurrElements > 0.0,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_elements is not greater than 0 after cache operation")

        long expectedQueryLimitBytes = fileCacheCapacity * fileCacheQueryLimitPercentTest1 / 100
        logger.info("Normal queue curr size and expected query limit comparison - normal queue curr size: ${updatedNormalQueueCurrSize as Long} , expected query limit: ${expectedQueryLimitBytes}")

        assertTrue((updatedNormalQueueCurrSize as Long) <= expectedQueryLimitBytes,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "normal_queue_curr_size is greater than expected query limit")

        double updatedTotalHitCounts = getMetricSum("total_hit_counts")
        double updatedTotalReadCounts = getMetricSum("total_read_counts")

        logger.info("Total hit and read counts comparison - hit counts: ${initialTotalHitCounts} -> ${updatedTotalHitCounts} , read counts: ${initialTotalReadCounts} -> ${updatedTotalReadCounts}")

        assertTrue(updatedTotalReadCounts > initialTotalReadCounts,
                FILE_CACHE_FEATURES_CHECK_FAILED_PREFIX + "total_read_counts did not increase after cache operation")
    }

    return true
}
