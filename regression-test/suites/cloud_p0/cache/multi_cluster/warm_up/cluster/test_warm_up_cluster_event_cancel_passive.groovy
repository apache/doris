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

suite('test_warm_up_cluster_event_cancel_passive', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'enable_debug_points=true',
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        'enable_debug_points=true',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'warmup_tablet_replica_info_cache_ttl_sec=10',
    ]
    options.cloudMode = true

    def setDebugPoint = {ip, port, op, name ->
        def urlStr = "http://${ip}:${port}/api/debug_point/${op}/${name}"
        def url = new URL(urlStr)
        def conn = url.openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput = true

        // Send empty body (required to trigger POST)
        conn.outputStream.withWriter { it << "" }

        // Read response
        def responseText = conn.inputStream.text
        logger.info("Response from ${urlStr}: ${responseText}")
        def json = new JsonSlurper().parseText(responseText)

        return json?.msg == "OK"
    }

    def setDebugPointsForCluster = { cluster, debug_point, enable ->
        def backends = sql """SHOW BACKENDS"""

        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[4]
            if (enable) {
                assertTrue(setDebugPoint(ip, port, 'add', debug_point))
            } else {
                assertTrue(setDebugPoint(ip, port, 'remove', debug_point))
            }
        }
    }

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
        sleep(10000)
    }

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
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

    def logWarmUpRowsetMetrics = { cluster ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def submitted_segment = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_submitted_segment_num")
            def finished_segment = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_finished_segment_num")
            def failed_segment = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_failed_segment_num")
            def submitted_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_submitted_index_num")
            def finished_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_finished_index_num")
            def failed_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_failed_index_num")
            logger.info("${cluster} be ${ip}:${port}, submitted_segment=${submitted_segment}"
                    + ", finished_segment=${finished_segment}, failed_segment=${failed_segment}"
                    + ", submitted_index=${submitted_index}"
                    + ", finished_index=${finished_index}"
                    + ", failed_index=${failed_index}")
        }
    }

    def getTTLCacheSize = { ip, port ->
        return getBrpcMetrics(ip, port, "ttl_cache_size")
    }

    def getClusterTTLCacheSizeSum = { cluster ->
        def backends = sql """SHOW BACKENDS"""

        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }

        long sum = 0
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[5]
            def size = getTTLCacheSize(ip, port)
            sum += size
            logger.info("be be ${ip}:${port} ttl cache size ${size}")
        }

        return sum
    }

    def checkTTLCacheSizeSumEqual = { cluster1, cluster2 ->
        def srcSum = getClusterTTLCacheSizeSum(cluster1)
        def dstSum = getClusterTTLCacheSizeSum(cluster2)

        logger.info("ttl_cache_size: src=${srcSum} dst=${dstSum}")
        assertTrue(srcSum > 0, "ttl_cache_size should > 0")
        assertEquals(srcSum, dstSum)
    }

    docker(options) {
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        // Add two clusters
        cluster.addBackend(3, clusterName1)
        cluster.addBackend(3, clusterName2)

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

        // Start warm up job
        def jobId_ = sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "event_driven",
                "sync_event" = "load"
            )
        """

        def jobId = jobId_[0][0]
        logger.info("Warm-up job ID: ${jobId}")
        clearFileCacheOnAllBackends()

        // First make some entries in tablet location info ttl cache
        sleep(15000)
        for (int i = 0; i < 100; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)
        checkTTLCacheSizeSumEqual(clusterName1, clusterName2)
        def cacheSize0 = getClusterTTLCacheSizeSum(clusterName2)

        // Make BE ignore the cancel request
        setDebugPointsForCluster(clusterName1, 'CloudWarmUpManager.set_event.ignore_all', true)
        // Cancel job and confirm
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        def cancelInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals(cancelInfo[0][3], "CANCELLED")

        // Initially, the cache has not expired, so we expect some more syncs
        for (int i = 0; i < 10; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)
        def cacheSize1 = getClusterTTLCacheSizeSum(clusterName2);
        assertTrue(cacheSize1 >= cacheSize0, "some more syncs before cache expire is expected")

        // At this point, cache should be expired, so we expect no more syncs

        for (int i = 0; i < 100; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)
        def cacheSize2 = getClusterTTLCacheSizeSum(clusterName2);
        assertEquals(cacheSize1, cacheSize2, "no more syncs after cache expire is expected")

        logWarmUpRowsetMetrics(clusterName2)
        logFileCacheDownloadMetrics(clusterName2)

        setDebugPointsForCluster(clusterName1, 'CloudWarmUpManager.set_event.ignore_all', false)

        // Clean up
        sql """DROP TABLE IF EXISTS customer"""
    }
}
