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

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64

suite('test_warm_up_cluster_periodic_slow_job', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
        'enable_debug_points=true',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true

    def setDebugPoint = {ip, port, op, name ->
        def urlStr = "http://${ip}:${port}/api/debug_point/${op}/${name}"
        def url = new URL(urlStr)
        def conn = url.openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput = true

        // Add Basic Auth header
        def authString = "root:"
        def encodedAuth = Base64.encoder.encodeToString(authString.getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encodedAuth}")

        // Send empty body (required to trigger POST)
        conn.outputStream.withWriter { it << "" }

        // Read response
        def responseText = conn.inputStream.text
        def json = new JsonSlurper().parseText(responseText)

        return json?.msg == "OK" && json?.code == 0
    }

    def addDebugPoint = { ip, http_port, name ->
        return setDebugPoint(ip, http_port, 'add', name)
    }

    def removeDebugPoint = { ip, http_port, name ->
        return setDebugPoint(ip, http_port, 'remove', name)
    }

    def addFEDebugPoint = { name ->
        def fe = cluster.getMasterFe()
        return addDebugPoint(fe.host, fe.httpPort, name)
    }

    def removeFEDebugPoint = { name ->
        def fe = cluster.getMasterFe()
        return removeDebugPoint(fe.host, fe.httpPort, name)
    }

    def getWarmUpJobInfo = { jobId ->
        def jobInfo = sql """SHOW WARM UP JOB WHERE id = ${jobId}"""

        if (jobInfo == null || jobInfo.isEmpty()) {
            fail("No warm up job found with ID: ${jobId}")
        }

        def job = jobInfo[0]
        def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        def sTime = LocalDateTime.parse(job[7], formatter)
        def cTime = LocalDateTime.parse(job[6], formatter)

        return [
            status    : job[3],
            createTime: cTime,
            startTime : sTime,
        ]
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
        sleep(5000)
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

    def getTTLCacheSize = { ip, port ->
        return getBrpcMetrics(ip, port, "ttl_cache_size")
    }

    def checkTTLCacheSizeSumEqual = { cluster1, cluster2 ->
        def backends = sql """SHOW BACKENDS"""

        def srcBes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster1}\"""") }
        def tgtBes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster2}\"""") }

        long srcSum = 0
        for (src in srcBes) {
            def ip = src[1]
            def port = src[5]
            def size = getTTLCacheSize(ip, port)
            srcSum += size
            logger.info("src be ${ip}:${port} ttl cache size ${size}")
        }

        long tgtSum = 0
        for (tgt in tgtBes) {
            def ip = tgt[1]
            def port = tgt[5]
            def size = getTTLCacheSize(ip, port)
            tgtSum += size
            logger.info("dst be ${ip}:${port} ttl cache size ${size}")
        }

        logger.info("ttl_cache_size: src=${srcSum} dst=${tgtSum}")
        assertTrue(srcSum > 0, "ttl_cache_size should > 0")
        assertEquals(srcSum, tgtSum)
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

        // Clear hotspot statistics
        sql """truncate table __internal_schema.cloud_cache_hotspot;"""

        clearFileCacheOnAllBackends()

        // Simple setup to simulate data load and access
        sql """CREATE TABLE IF NOT EXISTS customer (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        sql """INSERT INTO customer VALUES (1, 'A'), (2, 'B'), (3, 'C')"""

        addFEDebugPoint("CloudWarmUpJob.FakeLastBatchNotDone")

        def sync_sec = 1

        // Start warm up job
        def jobId_ = sql """
            WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "${sync_sec}"
            )
        """

        def jobId = jobId_[0][0]
        logger.info("Warm-up job ID: ${jobId}")

        for (int i = 0; i < 1000; i++) {
            sql """SELECT * FROM customer"""
        }

        def wait_sec = sync_sec * 5
        sleep(wait_sec * 2 * 1000)

        def hotspot = sql """select * from __internal_schema.cloud_cache_hotspot;"""
        logger.info("hotspot: {}", hotspot)

        def info1 = getWarmUpJobInfo(jobId)
        logger.info("info1 {}", info1)
        assertEquals("RUNNING", info1.status)

        sleep(wait_sec * 2 * 1000)

        def info2 = getWarmUpJobInfo(jobId)
        logger.info("info2 {}", info2)
        assertEquals("RUNNING", info2.status)
        assertEquals(info1.createTime, info2.createTime)
        assertEquals(info1.startTime, info2.startTime)

        removeFEDebugPoint("CloudWarmUpJob.FakeLastBatchNotDone")
        sleep(wait_sec * 1000)

        def info3 = getWarmUpJobInfo(jobId)
        logger.info("info3 {}", info3)
        assertEquals(info1.createTime, info3.createTime)
        assertTrue(Duration.between(info1.startTime, info3.startTime).seconds.abs() > wait_sec * 4)

        sleep(wait_sec * 1000)
        def info4 = getWarmUpJobInfo(jobId)
        logger.info("info4 {}", info4)
        assertEquals(info1.createTime, info4.createTime)
        assertTrue(Duration.between(info3.startTime, info4.startTime).seconds.abs() > sync_sec)

        logFileCacheDownloadMetrics(clusterName2)
        checkTTLCacheSizeSumEqual(clusterName1, clusterName2)

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
