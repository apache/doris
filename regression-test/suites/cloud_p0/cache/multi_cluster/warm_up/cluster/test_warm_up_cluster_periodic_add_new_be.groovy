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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper

suite('test_warm_up_cluster_periodic_add_new_be', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
        'rehash_tablet_after_be_dead_seconds=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true

    def clearFileCache = {ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)

        // Check the status
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
        }
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """SHOW BACKENDS"""

        for (be in backends) {
            def ip = be[1]
            def port = be[4]
            clearFileCache(ip, port)
        }

        // clear file cache is async, wait it done
        sleep(5000)
    }

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") + " --cert " + context.config.otherConfigs.get("trustCert") + " --cacert " + context.config.otherConfigs.get("trustCACert") + " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    def logFileCacheDownloadMetrics = { cluster ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def submitted = getBrpcMetrics(ip, port, "file_cache_download_submitted_num")
            def finished = getBrpcMetrics(ip, port, "file_cache_download_finished_num")
            def failed = getBrpcMetrics(ip, port, "file_cache_download_failed_num")
            logger.info("${cluster} be ${ip}:${port}, downloader submitted=${submitted}"
                    + ", finished=${finished}, failed=${failed}")
        }
    }

    def logFileCacheQueueMetrics = { cluster, phase ->
        def queueMetrics = [
                normal    : [
                        size : "file_cache_normal_queue_cache_size",
                        count: "file_cache_normal_queue_element_count",
                        evict: "file_cache_normal_queue_evict_size",
                ],
                index     : [
                        size : "file_cache_index_queue_cache_size",
                        count: "file_cache_index_queue_element_count",
                        evict: "file_cache_index_queue_evict_size",
                ],
                disposable: [
                        size : "file_cache_disposable_queue_cache_size",
                        count: "file_cache_disposable_queue_element_count",
                        evict: "file_cache_disposable_queue_evict_size",
                ],
                ttl       : [
                        size : "file_cache_ttl_cache_lru_queue_size",
                        count: "file_cache_ttl_cache_lru_queue_element_count",
                        evict: "file_cache_ttl_cache_evict_size",
                ],
        ]

        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def fileCacheSize = getBrpcMetrics(ip, port, "file_cache_cache_size")
            def queueMetricDetails = []
            queueMetrics.each { queueName, metrics ->
                def size = getBrpcMetrics(ip, port, metrics.size)
                def count = getBrpcMetrics(ip, port, metrics.count)
                def evict = getBrpcMetrics(ip, port, metrics.evict)
                queueMetricDetails.add("${queueName}_queue{size=${size}, element_count=${count}, evict_size=${evict}}")
            }
            logger.info("${phase} ${cluster} be ${ip}:${port} file_cache_size=${fileCacheSize}, "
                    + "file_cache_queue_metrics: ${queueMetricDetails.join(', ')}")
        }
    }

    def logFileCacheQueueCacheSizeMetrics = { cluster, phase ->
        def queueCacheSizeMetrics = [
                normal    : "file_cache_normal_queue_cache_size",
                index     : "file_cache_index_queue_cache_size",
                disposable: "file_cache_disposable_queue_cache_size",
                ttl       : "file_cache_ttl_cache_lru_queue_size",
        ]

        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            queueCacheSizeMetrics.each { queueName, metricName ->
                def size = getBrpcMetrics(ip, port, metricName)
                logger.info("${phase} ${cluster} be ${ip}:${port} ${queueName}_queue cache_size=${size}")
            }
        }
    }

    def getFileCacheSize = { ip, port ->
        return getBrpcMetrics(ip, port, "file_cache_cache_size")
    }

    def getClusterFileCacheSizeSum = { cluster ->
        def backends = sql """SHOW BACKENDS"""

        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }

        long sum = 0
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def size = getFileCacheSize(ip, port)
            sum += size
            logger.info("${cluster} be ${ip}:${port} file cache size ${size}")
        }

        return sum
    }

    def checkFileCacheSizeSumEqual = { cluster1, cluster2 ->
        def srcSum = getClusterFileCacheSizeSum(cluster1)
        def dstSum = getClusterFileCacheSizeSum(cluster2)

        logger.info("file_cache_cache_size: src=${srcSum} dst=${dstSum}")
        assertTrue(srcSum > 0, "file_cache_cache_size should > 0")
        assertEquals(srcSum, dstSum)
    }

    def getClusterMetricSum = { cluster, metricName ->
        def backends = sql """SHOW BACKENDS"""

        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }

        long sum = 0
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def value = getBrpcMetrics(ip, port, metricName)
            sum += value
            logger.info("${cluster} be ${ip}:${port} ${metricName} ${value}")
        }

        return sum
    }

    def getPeriodicWarmupMetrics = { cluster ->
        [
                submitted: getClusterMetricSum(cluster, "file_cache_once_or_periodic_warm_up_submitted_segment_num"),
                finished : getClusterMetricSum(cluster, "file_cache_once_or_periodic_warm_up_finished_segment_num"),
        ]
    }

    def loadCustomerData = { clusterName, tableName, rowCount ->
        sql """use @${clusterName}"""

        def dataFile = File.createTempFile("test_warm_up_cluster_periodic_add_new_be_", ".csv")
        dataFile.deleteOnExit()
        dataFile.withWriter("UTF-8") { writer ->
            for (int i = 1; i <= rowCount; i++) {
                writer.write("${i},name_${i}\n")
            }
        }

        streamLoad {
            table tableName
            set 'column_separator', ','
            set 'compute_group', clusterName
            file dataFile.getAbsolutePath()
            time 60000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                logger.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(rowCount, json.NumberTotalRows)
                assertEquals(rowCount, json.NumberLoadedRows)
            }
        }
    }

    def waitUntil = { condition, timeoutMs, description ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (condition()) {
                return
            }
            sleep(1000)
        }
        throw new RuntimeException("Timed out after ${timeoutMs}ms waiting for ${description}")
    }

    docker(options) {
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        // Add two clusters
        cluster.addBackend(3, clusterName1)
        cluster.addBackend(1, clusterName2)

        def tag1 = getCloudBeTagByName(clusterName1)
        def tag2 = getCloudBeTagByName(clusterName2)

        logger.info("Cluster tag1: {}", tag1)
        logger.info("Cluster tag2: {}", tag2)

        def jsonSlurper = new JsonSlurper()
        def clusterId1 = jsonSlurper.parseText(tag1).compute_group_id
        def clusterId2 = jsonSlurper.parseText(tag2).compute_group_id

        def getJobState = { jobId ->
            def jobStateResult = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            return jobStateResult[0][3]
        }

        // Ensure we are in source cluster
        sql """use @${clusterName1}"""

        // Simple setup to simulate data load and access
        sql """CREATE TABLE IF NOT EXISTS customer (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        loadCustomerData(clusterName1, "customer", 3000)
        logFileCacheQueueCacheSizeMetrics(clusterName1, "after load before clear cache")
        logFileCacheQueueCacheSizeMetrics(clusterName2, "after load before clear cache")

        // Start warm up job
        def jobId_ = sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """

        def jobId = jobId_[0][0]
        logger.info("Warm-up job ID: ${jobId}")

        // Add new backends to cluster 2
        cluster.addBackend(2, clusterName2);

        // Clear hotspot statistics
        sql """truncate table __internal_schema.cloud_cache_hotspot;"""
        clearFileCacheOnAllBackends()

        def beforeWarmupMetrics = getPeriodicWarmupMetrics(clusterName2)
        logFileCacheQueueMetrics(clusterName1, "after clear cache before warmup")
        logFileCacheQueueMetrics(clusterName2, "after clear cache before warmup")
        for (int i = 0; i < 500; i++) {
            sql """SELECT * FROM customer"""
        }

        waitUntil({
            def metrics = getPeriodicWarmupMetrics(clusterName2)
            metrics.submitted > beforeWarmupMetrics.submitted && metrics.finished > beforeWarmupMetrics.finished
        }, 120000, "periodic warmup metrics to advance from ${beforeWarmupMetrics}")

        waitUntil({ getClusterFileCacheSizeSum(clusterName1) > 0 },
                60000, "${clusterName1} file cache size > 0")
        waitUntil({
            logFileCacheQueueMetrics(clusterName1, "wait for warmup")
            logFileCacheQueueMetrics(clusterName2, "wait for warmup")
            getClusterFileCacheSizeSum(clusterName1) == getClusterFileCacheSizeSum(clusterName2)
        }, 60000, "${clusterName1} and ${clusterName2} file cache size to match")

        logFileCacheDownloadMetrics(clusterName2)
        logFileCacheQueueMetrics(clusterName1, "after warmup")
        logFileCacheQueueMetrics(clusterName2, "after warmup")
        def afterWarmupMetrics = getPeriodicWarmupMetrics(clusterName2)
        assertTrue(afterWarmupMetrics.submitted > beforeWarmupMetrics.submitted,
                "periodic warmup should submit segments, before=${beforeWarmupMetrics}, after=${afterWarmupMetrics}")
        assertTrue(afterWarmupMetrics.finished > beforeWarmupMetrics.finished,
                "periodic warmup should finish segments, before=${beforeWarmupMetrics}, after=${afterWarmupMetrics}")
        checkFileCacheSizeSumEqual(clusterName1, clusterName2)

        def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals(jobInfo[0][0], jobId)
        assertEquals(jobInfo[0][1], clusterName1)
        assertEquals(jobInfo[0][2], clusterName2)
        assertEquals(jobInfo[0][4], "CLUSTER")
        assertTrue(jobInfo[0][3] in ["RUNNING", "PENDING"],
            "JobState is ${jobInfo[0][3]}, expected RUNNING or PENDING")
        assertEquals(jobInfo[0][5], "PERIODIC (1s)")

        // Cancel job and confirm
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        def cancelInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals(cancelInfo[0][3], "CANCELLED")

        // Clean up
        sql """DROP TABLE IF EXISTS customer"""
    }
}
