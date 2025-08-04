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
import org.apache.doris.regression.util.NodeType
import groovy.json.JsonSlurper

suite('test_warm_up_cluster_event_compaction_sync_wait_timeout', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'warm_up_rowset_slow_log_ms=1',
        'enable_warm_up_rowset_sync_wait_on_compaction=true',
        'warm_up_rowset_sync_wait_min_timeout_ms=10000',
        'warm_up_rowset_sync_wait_max_timeout_ms=10000',
    ]
    options.enableDebugPoints()
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
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    def getBeIpAndPort = { cluster ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }

        if (cluster_bes.isEmpty()) {
            throw new RuntimeException("No BE found for cluster: ${cluster}")
        }

        def firstBe = cluster_bes[0]
        return [ip: firstBe[1], http_port:firstBe[4], rpc_port: firstBe[5]]
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
            def compaction_sync_wait = getBrpcMetrics(ip, port, "file_cache_warm_up_rowset_wait_for_compaction_num")
            logger.info("${cluster} be ${ip}:${port}, submitted_segment=${submitted_segment}"
                    + ", finished_segment=${finished_segment}, failed_segment=${failed_segment}"
                    + ", submitted_index=${submitted_index}"
                    + ", finished_index=${finished_index}"
                    + ", failed_index=${failed_index}"
                    + ", compaction_sync_wait=${compaction_sync_wait}")
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

    def waitForBrpcMetricValue = { ip, port, metricName, targetValue, timeoutMs ->
        def delta_time = 100
        def useTime = 0

        for(int t = delta_time; t <= timeoutMs; t += delta_time){
            try {
                def currentValue = getBrpcMetrics(ip, port, metricName)

                if (currentValue == targetValue) {
                    logger.info("BE ${ip}:${port} metric ${metricName} reached target value: ${targetValue}")
                    return true
                }

                logger.info("BE ${ip}:${port} metric ${metricName} current value: ${currentValue}, target: ${targetValue}")

            } catch (Exception e) {
                logger.warn("Failed to get metric ${metricName} from BE ${ip}:${port}: ${e.message}")
            }

            useTime = t
            sleep(delta_time)
        }

        assertTrue(useTime <= timeoutMs, "waitForBrpcMetricValue timeout")
    }

    def getTabletStatus = { ip, port, tablet_id ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${ip}:${port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
    }

    def checkFileCacheRecycle = { cluster, rowsets ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        assert cluster_bes.size() > 0, "No backend found for cluster ${cluster}"
        def be = cluster_bes[0]
        def ip = be[1]
        def port = be[4]

        for (int i = 0; i < rowsets.size(); i++) {
            def rowsetStr = rowsets[i]
            // [12-12] 1 DATA NONOVERLAPPING 02000000000000124843c92c13625daa8296c20957119893 1011.00 B
            def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
            def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
            def rowset_id = rowsetStr.split(" ")[4]

            logger.info("rowset ${i}, start: ${start_version}, end: ${end_version}, id: ${rowset_id}")
            def data = Http.GET("http://${ip}:${port}/api/file_cache?op=list_cache&value=${rowset_id}_0.dat", true)
            logger.info("file cache data: ${data}")
            // in this case only [2-11] and [12-12] should have data in cache
            if ((start_version == 2 && end_version == 11) || (start_version == 12)) {
                assertTrue(data.size() > 0)
            } else {
                assertTrue(data.size() == 0)
            }
        }
    }

    docker(options) {
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        // Add two clusters
        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)

        def tag1 = getCloudBeTagByName(clusterName1)
        def tag2 = getCloudBeTagByName(clusterName2)

        logger.info("Cluster tag1: {}", tag1)
        logger.info("Cluster tag2: {}", tag2)

        def jsonSlurper = new JsonSlurper()

        def getJobState = { jobId ->
            def jobStateResult = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            return jobStateResult[0][3]
        }

        // Ensure we are in source cluster
        sql """use @${clusterName1}"""

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

        sql """
            create table test (
                col0 int not null,
                col1 variant NOT NULL
            ) UNIQUE KEY(`col0`)
            DISTRIBUTED BY HASH(col0) BUCKETS 1
            PROPERTIES ("file_cache_ttl_seconds" = "3600", "disable_auto_compaction" = "true");
        """

        clearFileCacheOnAllBackends()
        sleep(15000)

        sql """use @${clusterName1}"""
        // load data
        sql """insert into test values (1, '{"a" : 1.0}')"""
        sql """insert into test values (2, '{"a" : 111.1111}')"""
        sql """insert into test values (3, '{"a" : "11111"}')"""
        sql """insert into test values (4, '{"a" : 1111111111}')"""
        sql """insert into test values (5, '{"a" : 1111.11111}')"""
        sql """insert into test values (6, '{"a" : "11111"}')"""
        sql """insert into test values (7, '{"a" : 11111.11111}')"""
        sql """insert into test values (7, '{"a" : 11111.11111}')"""
        sleep(15000)

        def tablets = sql_return_maparray """ show tablets from test; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        def be = getBeIpAndPort(clusterName2)

        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)
        def num_submitted = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_segment_num")
        def num_finished = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num")
        assertTrue(num_submitted >= 8)
        assertEquals(num_finished, num_submitted)

        // inject slow io, which should cause the warmup takes longger than 10s
        GetDebugPoint().enableDebugPoint(be.ip, be.http_port as int, NodeType.BE, "S3FileReader::read_at_impl.io_slow", [sleep:20])

        // trigger and wait compaction async
        def future = thread {
            sql """use @${clusterName1}"""
            trigger_and_wait_compaction("test", "cumulative")
        }
        // wait until the warmup for compaction started
        waitForBrpcMetricValue(be.ip, be.rpc_port, "file_cache_warm_up_rowset_wait_for_compaction_num", 1, /*timeout*/10000)
        sleep(1000)
        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)
        assertEquals(num_submitted + 1, getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_segment_num"))
        assertEquals(num_finished, getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num"))

        // in this moment, compaction has completed, but not commited, it's waiting for warm up
        // trigger a query on read cluster, can't read the compaction data
        sql """use @${clusterName2}"""
        sql "select * from test"
        def tablet_status = getTabletStatus(be.ip, be.http_port, tablet_id)
        def rowsets = tablet_status ["rowsets"]
        for (int i = 0; i < rowsets.size(); i++) {
            def rowsetStr = rowsets[i]
            // [12-12] 1 DATA NONOVERLAPPING 02000000000000124843c92c13625daa8296c20957119893 1011.00 B
            def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
            def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
            if (start_version != 0) {
                assertEquals(start_version, end_version)
            }
        }

        // wait the compaction complete
        // we inject 20s sleep on s3 file read, so the compaction will be timeout
        future.get()

        // still not finished, so `num_finished` not change
        assertEquals(num_finished, getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num"))
        assertEquals(1, getBrpcMetrics(be.ip, be.rpc_port, "file_cache_warm_up_rowset_wait_for_compaction_timeout_num"))

        // a new insert will trigger the sync rowset operation in the following query
        sql """insert into test values (9, '{"a" : 11111.11111}')"""

        // now the compaction rowsets it accessible
        sql """use @${clusterName2}"""
        sql "select * from test"
        tablet_status = getTabletStatus(be.ip, be.http_port, tablet_id)
        rowsets = tablet_status ["rowsets"]
        def found_compaction_rowsets = false
        for (int i = 0; i < rowsets.size(); i++) {
            def rowsetStr = rowsets[i]
            // [12-12] 1 DATA NONOVERLAPPING 02000000000000124843c92c13625daa8296c20957119893 1011.00 B
            def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
            def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
            if (start_version != 0) {
                if (start_version != end_version) {
                    found_compaction_rowsets = true;
                }
            }
        }
        assertTrue(found_compaction_rowsets)

        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)
        // checkTTLCacheSizeSumEqual(clusterName1, clusterName2)

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
    }
}
