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

suite('test_warm_up_cluster_event_rename', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'warmup_tablet_replica_info_cache_ttl_sec=1'
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
            def submitted_segment_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_submitted_segment_size")
            def finished_segment_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_finished_segment_size")
            def failed_segment_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_failed_segment_size")
            def submitted_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_submitted_index_num")
            def finished_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_finished_index_num")
            def failed_index = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_failed_index_num")
            def submitted_index_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_submitted_index_size")
            def finished_index_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_finished_index_size")
            def failed_index_size = getBrpcMetrics(ip, port, "file_cache_event_driven_warm_up_failed_index_size")
            logger.info("${cluster} be ${ip}:${port}, submitted_segment=${submitted_segment}"
                    + ", finished_segment=${finished_segment}, failed_segment=${failed_segment}"
                    + ", submitted_segment_size=${submitted_segment_size}"
                    + ", finished_segment_size=${finished_segment_size}"
                    + ", failed_segment_size=${failed_segment_size}"
                    + ", submitted_index=${submitted_index}"
                    + ", finished_index=${finished_index}"
                    + ", failed_index=${failed_index}"
                    + ", submitted_index_size=${submitted_index_size}"
                    + ", finished_index_size=${finished_index_size}"
                    + ", failed_index_size=${failed_index_size}")
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
        def backends = sql """SHOW BACKENDS"""

        def srcBes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster1}\"""") }
        def tgtBes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster2}\"""") }

        long srcSum = 0
        for (src in srcBes) {
            def ip = src[1]
            def port = src[5]
            srcSum += getTTLCacheSize(ip, port)
        }

        long tgtSum = 0
        for (tgt in tgtBes) {
            def ip = tgt[1]
            def port = tgt[5]
            tgtSum += getTTLCacheSize(ip, port)
        }

        logger.info("ttl_cache_size: src=${srcSum} dst=${tgtSum}")
        assertTrue(srcSum > 0, "ttl_cache_size should > 0")
        assertEquals(srcSum, tgtSum)
    }

    docker(options) {
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"
        def clusterName3 = "warmup_other"

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
        sql """CREATE TABLE IF NOT EXISTS customer (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("file_cache_ttl_seconds" = "3600", "disable_auto_compaction" = "true")"""

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

        sleep(15000)

        for (int i = 0; i < 100; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)
        logWarmUpRowsetMetrics(clusterName2)
        logFileCacheDownloadMetrics(clusterName2)
        checkTTLCacheSizeSumEqual(clusterName1, clusterName2)

        def srcSumOld = getClusterTTLCacheSizeSum(clusterName1)
        def dstSumOld = getClusterTTLCacheSizeSum(clusterName2)

        // rename
        sql """ALTER SYSTEM RENAME COMPUTE GROUP ${clusterName2} ${clusterName3}"""
        sleep(5000)

        assertEquals(0, getClusterTTLCacheSizeSum(clusterName2))
        assertEquals(dstSumOld, getClusterTTLCacheSizeSum(clusterName3))

        for (int i = 0; i < 100; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)

        assertEquals(0, getClusterTTLCacheSizeSum(clusterName2))
        assertEquals(dstSumOld, getClusterTTLCacheSizeSum(clusterName3))

        // rename back
        sql """ALTER SYSTEM RENAME COMPUTE GROUP ${clusterName3} ${clusterName2}"""
        clearFileCacheOnAllBackends()

        sleep(15000)

        for (int i = 0; i < 100; i++) {
            sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""
        }
        sleep(15000)
        logWarmUpRowsetMetrics(clusterName2)
        logFileCacheDownloadMetrics(clusterName2)
        checkTTLCacheSizeSumEqual(clusterName1, clusterName2)

        def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals(jobInfo[0][0], jobId)
        assertEquals(jobInfo[0][1], clusterName1)
        assertEquals(jobInfo[0][2], clusterName2)
        assertEquals(jobInfo[0][4], "CLUSTER")
        assertTrue(jobInfo[0][3] in ["RUNNING", "PENDING"],
            "JobState is ${jobInfo[0][3]}, expected RUNNING or PENDING")
        assertEquals(jobInfo[0][5], "EVENT_DRIVEN (LOAD)")

        // Cancel job and confirm
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        def cancelInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals(cancelInfo[0][3], "CANCELLED")

        // Clean up
        sql """DROP TABLE IF EXISTS customer"""
    }
}
