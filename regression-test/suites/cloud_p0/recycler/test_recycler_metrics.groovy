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

suite("test_recycler_recycle_metrics") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = "test_recycler_recycle_metrics"

    def parseMetricLine = { String line ->
        def matcher = (line =~ /\{(.*?)\}\s+(\d+\.?\d*)/)
        if (matcher.find()) {
            String labelsString = matcher.group(1)
            String metricValue = matcher.group(2)

            def labels = [:]
            def labelMatcher = (labelsString =~ /(\w+?)="([^"]*?)"/)
            labelMatcher.each { match ->
                labels[match[1]] = match[2]
            }
            return [labels: labels, value: metricValue]
        }
        return null
    }
    // g_bvar_recycler_instance_recycle_last_success_ts

    def getMetricsMethod = { String ipWithPort, String metricsName, String resourceType = null ->
        def parsedMetricData = null

        httpTest {
            endpoint ipWithPort
            uri "/brpc_metrics"
            op "get"
            check { respCode, body ->
                assertEquals("${respCode}".toString(), "200")

                String out = "${body}".toString()
                def lines = out.split('\n')

                for (String line in lines) {
                    if (line.contains(metricsName) && !line.startsWith("#")) {
                        def data = parseMetricLine(line)
                        if (data) {
                            if (resourceType != null && data.labels?.resource_type == resourceType) {
                                parsedMetricData = data
                                break
                            } else if (resourceType == null) {
                                parsedMetricData = data
                                break
                            }
                        }
                    }
                }
            }
        }
        return parsedMetricData
    }

    def waitForMetricUpdate = { String metricName, String port, Closure<Boolean> validationCheck, int maxRetries = 15, int sleepMillis = 10000 ->
        int retry = maxRetries
        boolean success = false
        do {
            Long timestampBefore = System.currentTimeMillis() / 1000
            triggerRecycle(token, instanceId)
            Thread.sleep(sleepMillis)
            
            def metricData = getMetricsMethod.call(port, metricName)
            
            if (validationCheck(metricData, timestampBefore)) {
                success = true
                logger.info("--- Parsed Metric Data ---")
                logger.info("Metric Name: ${metricName}")
                logger.info("Value: ${metricData.value}")
                logger.info("Labels: ${metricData.labels}")
                logger.info("Instance ID: ${metricData.labels?.instance_id}")
                logger.info("Resource Type: ${metricData.labels?.resource_type}")
                logger.info("--------------------------------------")
                break
            } else {
                logger.warn("Metric '${metricName}' validation failed, retrying...")
            }
        } while (retry--)
        return success
    }

    def recyclerHttpPort = context.config.recycleServiceHttpAddress

    println "recyclerHttpPort: ${recyclerHttpPort}"

    logger.info("=== Test 1: recycler_instance_last_round_recycled_num of recycle_rowsets job ===")

    sql "drop table if exists ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id int,
            `k` int,
            `k1` int,
            `k2` int
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    for (int i = 1; i <= 50; i++) {
        sql """INSERT INTO ${tableName} VALUES (${i}, ${i}, ${i}, ${i})"""
    }

    String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList:${tabletInfoList}")
    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tabletInfoList) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    def success = waitForMetricUpdate("recycler_instance_recycle_last_success_ts", recyclerHttpPort, 
        { metricData, timestampBefore ->
            Long lastSuccessRecycleTs = Long.parseLong(metricData.value) / 1000
            logger.info("lastSuccessRecycleTs: ${lastSuccessRecycleTs}, currentTime: ${timestampBefore}")
            return lastSuccessRecycleTs > timestampBefore
        }
    )
    assertTrue(success)

    int retryCount = 0
    while (true) {
        def metricDataBeforeRecycle = getMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_bytes",
            "recycle_rowsets"
        )

        def metricDataAftereRecycle = getMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_bytes",
            "recycle_rowsets"
        )

        if (metricDataBeforeRecycle && metricDataAftereRecycle && metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
            logger.info("--- Parsed Metric Data (Example 1) ---")
            logger.info("Metric Name: recycler_instance_last_round_recycled_num")
            logger.info("Value: ${metricDataBeforeRecycle.value}")
            logger.info("Labels: ${metricDataBeforeRecycle.labels}")
            logger.info("Instance ID: ${metricDataBeforeRecycle.labels?.instance_id}")
            logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
            logger.info("--------------------------------------")

            assertNotNull(metricDataBeforeRecycle, "Metric 'recycler_instance_last_round_recycled_num' with resource_type 'recycle_rowsets' should be found and parsed.")
            break
        }
        retryCount++
        if (retryCount > 10) {
            logger.error("Failed to get metric 'recycler_instance_last_round_to_recycle_bytes' after 10 retries.")
            return;
        }
        sleep(2000)
    }

    retryCount = 0
    while (true) {
        def metricDataBeforeRecycle = getMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_num",
            "recycle_rowsets"
        )

        def metricDataAftereRecycle = getMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_num",
            "recycle_rowsets"
        )

        if (metricDataBeforeRecycle && metricDataAftereRecycle) {
            if (metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
                logger.info("--- Recycle Success ---")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Labels: ${metricDataBeforeRecycle.labels}")
                logger.info("Instance ID: ${metricDataBeforeRecycle.labels?.instance_id}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                break
            } else {
                logger.info("--- Recycle failed ---")
                logger.info("Metric Name: recycler_instance_last_round_to_recycle_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Labels: ${metricDataBeforeRecycle.labels}")
                logger.info("Instance ID: ${metricDataBeforeRecycle.labels?.instance_id}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataAftereRecycle.value}")
                logger.info("Labels: ${metricDataAftereRecycle.labels}")
                logger.info("Instance ID: ${metricDataAftereRecycle.labels?.instance_id}")
                logger.info("Resource Type: ${metricDataAftereRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
            }
        }
        retryCount++
        if (retryCount > 10) {
            logger.error("Failed to get metric 'recycler_instance_last_round_to_recycle_num' after 10 retries.")
            return;
        }
        sleep(2000)
    }
}

