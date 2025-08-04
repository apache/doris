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
import groovy.json.JsonOutput

import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.util.*

Suite.metaClass.triggerRecycle = { String token, String instanceId /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite
    // function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, token: ${token}, instance:${instanceId}".toString())

    def triggerRecycleBody = [instance_ids: ["${instanceId}"]]
    def jsonOutput = new JsonOutput()
    def triggerRecycleJson = jsonOutput.toJson(triggerRecycleBody)
    def triggerRecycleResult = null;
    def triggerRecycleApi = { requestBody, checkFunc ->
        httpTest {
            endpoint suite.context.config.recycleServiceHttpAddress
            uri "/RecyclerService/http/recycle_instance?token=$token"
            body requestBody
            check checkFunc
        }
    }

    triggerRecycleApi.call(triggerRecycleJson) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            triggerRecycleResult = body
            suite.getLogger().info("triggerRecycleResult:${triggerRecycleResult}".toString())
            assertTrue(triggerRecycleResult.trim().equalsIgnoreCase("OK"))
    }
    return;
}

logger.info("Added 'triggerRecycle' function to Suite")

//cloud mode recycler plugin
Suite.metaClass.checkRecycleTable = { String token, String instanceId, String cloudUniqueId, String tableName, 
        Collection<String> tabletIdList /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, tableName: ${tableName}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleTable(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
    String provider = getObjStoreInfoApiResult.result.obj_info[0].provider
    suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}, provider:${provider}".toString())

    ListObjectsFileNames client = ListObjectsFileNamesUtil.getListObjectsFileNames(provider, ak, sk, endpoint, region, prefix, bucket, suite)

    assertTrue(tabletIdList.size() > 0)
    for (tabletId : tabletIdList) {
        suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}");
        // def objectListing = s3Client.listObjects(
        //     new ListObjectsRequest().withMaxKeys(1).withBucketName(bucket).withPrefix("${prefix}/data/${tabletId}/"))

        // suite.getLogger().info("tableName: ${tableName}, tabletId:${tabletId}, objectListing:${objectListing.getObjectSummaries()}".toString())
        if (!client.isEmpty(tableName, tabletId)) {
            return false;
        }
    }
    return true;
}

logger.info("Added 'checkRecycleTable' function to Suite")

Suite.metaClass.checkRecycleInternalStage = { String token, String instanceId, String cloudUniqueId, String fileName
    /* param */ ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleTable(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
    String provider = getObjStoreInfoApiResult.result.obj_info[0].provider
    suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}, provider:${provider}".toString())

    ListObjectsFileNames client = ListObjectsFileNamesUtil.getListObjectsFileNames(provider, ak, sk, endpoint, region, prefix, bucket, suite)

    // for root and admin, userId equal userName
    String userName = suite.context.config.jdbcUser;
    String userId = suite.context.config.jdbcUser;

    if (!client.isEmpty(userName, userId, fileName)) {
        return false;
    }

    return true;
}
logger.info("Added 'checkRecycleInternalStage' function to Suite")

Suite.metaClass.checkRecycleExpiredStageObjects = { String token, String instanceId, String cloudUniqueId, Set<String> nonExistFileNames, Set<String> existFileNames ->
    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("""Test plugin: suiteName: ${suite.name}, instanceId: ${instanceId}, token:${token}, cloudUniqueId:${cloudUniqueId}""".toString())

    def getObjStoreInfoApiResult = suite.getObjStoreInfo(token, cloudUniqueId);
    suite.getLogger().info("checkRecycleExpiredStageObjects(): getObjStoreInfoApiResult:${getObjStoreInfoApiResult}".toString())

    String ak = getObjStoreInfoApiResult.result.obj_info[0].ak
    String sk = getObjStoreInfoApiResult.result.obj_info[0].sk
    String endpoint = getObjStoreInfoApiResult.result.obj_info[0].endpoint
    String region = getObjStoreInfoApiResult.result.obj_info[0].region
    String prefix = getObjStoreInfoApiResult.result.obj_info[0].prefix
    String bucket = getObjStoreInfoApiResult.result.obj_info[0].bucket
    String provider = getObjStoreInfoApiResult.result.obj_info[0].provider
    suite.getLogger().info("ak:${ak}, sk:${sk}, endpoint:${endpoint}, prefix:${prefix}, provider:${provider}".toString())

    ListObjectsFileNames client = ListObjectsFileNamesUtil.getListObjectsFileNames(provider, ak, sk, endpoint, region, prefix, bucket, suite)

    // for root and admin, userId equal userName
    String userName = suite.context.config.jdbcUser;
    String userId = suite.context.config.jdbcUser;

    Set<String> fileNames = client.listObjects(userName, userId)
    for(def f : nonExistFileNames) {
        if (fileNames.contains(f)) {
            return false
        }
    }
    for(def f : existFileNames) {
        if (!fileNames.contains(f)) {
            return false
        }
    }
    return true
}
logger.info("Added 'checkRecycleExpiredStageObjects' function to Suite")

Suite.metaClass.checkRecycleMetrics = { String recyclerHttpPort, String recycleJobType ->

    def getRecyclerMetricsMethod = { String ipWithPort, String metricsName, String resourceType = null ->
        def parsedMetricData = null

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

        httpTest {
            endpoint ipWithPort
            uri "/brpc_metrics"
            op "get"
            printResponse false
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

    int retryCount = 0
    while (true) {
        def metricDataBeforeRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_bytes",
            recycleJobType
        )

        def metricDataAftereRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_bytes",
            recycleJobType
        )

        // not all resource types have bytes metrics
        def validResourceTypes = ["recycle_indexes", "recycle_partitions", "recycle_tmp_rowsets", "recycle_rowsets", "recycle_tablet", "recycle_segment"]
        
        boolean checkFlag1 = false
        boolean checkFlag2 = false

        if (validResourceTypes.contains(recycleJobType)) {
            checkFlag1 = true
        }

        if (metricDataBeforeRecycle && metricDataAftereRecycle && !checkFlag1) {
            if (metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
                logger.info("--- Recycle Success ---")
                logger.info("Metric Name: recycler_instance_last_round_recycled_bytes")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                checkFlag1 = true
            } else {
                logger.info("--- Recycle failed ---")
                logger.info("Metric Name: recycler_instance_last_round_to_recycle_bytes")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                logger.info("Metric Name: recycler_instance_last_round_recycled_bytes")
                logger.info("Value: ${metricDataAftereRecycle.value}")
                logger.info("Resource Type: ${metricDataAftereRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
            }
        }

        metricDataBeforeRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_to_recycle_num",
            recycleJobType
        )

        metricDataAftereRecycle = getRecyclerMetricsMethod.call(
            recyclerHttpPort,
            "recycler_instance_last_round_recycled_num",
            recycleJobType
        )

        if (metricDataBeforeRecycle && metricDataAftereRecycle && !checkFlag2) {
            if (metricDataBeforeRecycle.value == metricDataAftereRecycle.value) {
                logger.info("--- Recycle Success ---")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                checkFlag2 = true
            } else {
                logger.info("--- Recycle failed ---")
                logger.info("Metric Name: recycler_instance_last_round_to_recycle_num")
                logger.info("Value: ${metricDataBeforeRecycle.value}")
                logger.info("Resource Type: ${metricDataBeforeRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
                logger.info("Metric Name: recycler_instance_last_round_recycled_num")
                logger.info("Value: ${metricDataAftereRecycle.value}")
                logger.info("Resource Type: ${metricDataAftereRecycle.labels?.resource_type}")
                logger.info("--------------------------------------")
            }
        }

        if (checkFlag1 && checkFlag2) {
            break;
        }

        retryCount++
        if (retryCount > 10) {
            logger.error("Failed to get metric 'recycler_instance_last_round_to_recycle_bytes' after 10 retries.")
            return;
        }
        sleep(5000)
    }
}
logger.info("Added 'checkRecycleMetrics' function to Suite")
