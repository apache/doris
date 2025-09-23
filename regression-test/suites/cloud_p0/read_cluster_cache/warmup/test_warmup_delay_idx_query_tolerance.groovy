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

suite('test_warmup_delay_idx_query_tolerance', 'docker') {
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
        'enable_compaction_delay_commit_for_warm_up=true',
        'warm_up_rowset_sync_wait_min_timeout_ms=100', // to cauase timeout
        'warm_up_rowset_sync_wait_max_timeout_ms=100',
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
            logger.info("Metric ${name} on ${ip}:${port} is ${matcher[0][1]}")
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

    def do_cumu_compaction = { def be, def tbl, def tablet_id, int start, int end ->
        GetDebugPoint().enableDebugPoint(be.ip, be.http_port as int, NodeType.BE, "CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets", [tablet_id: "${tablet_id}", start_version: "${start}", end_version: "${end}"])
        trigger_and_wait_compaction(tbl, "cumulative")
        GetDebugPoint().disableDebugPoint(be.ip, be.http_port as int, NodeType.BE, "CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
    }

    def getBrpcMetricsByCluster = {cluster, name->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        assert cluster_bes.size() > 0, "No backend found for cluster ${cluster}"
        def be = cluster_bes[0]
        def ip = be[1]
        def port = be[5]
        return getBrpcMetrics(ip, port, name)
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
                col1 int NOT NULL,
                INDEX idx1(col1) USING INVERTED
            ) UNIQUE KEY(`col0`)
            DISTRIBUTED BY HASH(col0) BUCKETS 1
            PROPERTIES ("file_cache_ttl_seconds" = "3600", "disable_auto_compaction" = "true", "enable_unique_key_merge_on_write" = "false");
        """

        clearFileCacheOnAllBackends()

        sql """use @${clusterName1}"""
        // load data
        sql """insert into test values (1, 1)"""
        sql """insert into test(col0,col1,__DORIS_DELETE_SIGN__) values (1, 2, 1)"""
        sql """insert into test values (3, 3)"""
        sql """insert into test values (4, 4)"""
        sql """insert into test values (5, 5)"""
        sql """insert into test values (6, 6)"""
        sleep(5000)

        def tablets = sql_return_maparray """ show tablets from test; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        def be = getBeIpAndPort(clusterName2)
        def src_be = getBeIpAndPort(clusterName1)

        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)
        def num_submitted = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_segment_num")
        def num_finished = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num")
        def num_idx_submitted = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_index_num")
        def num_idx_finished = getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_index_num")
        assert num_submitted >= 6
        assert num_finished == num_submitted
        assert num_idx_submitted >= 6
        assert num_idx_finished == num_idx_submitted

        sql """use @${clusterName2}"""
        // ensure that base rowsets' meta are loaded on target cluster
        qt_cluster2_0 "select * from test order by col0, __DORIS_VERSION_COL__;"
        sql """use @${clusterName1}"""

        // inject sleep when read cluster warm up rowset for compaction and load
        GetDebugPoint().enableDebugPoint(be.ip, be.http_port as int, NodeType.BE, "CloudInternalServiceImpl::warm_up_rowset.download_inverted_idx", [sleep:10])

        // trigger and wait compaction async
        def future = thread {
            sql """use @${clusterName1}"""
            do_cumu_compaction(src_be, "test", tablet_id, 2, 5)
        }
        sleep(500)
        // wait until the warmup for compaction started
        waitForBrpcMetricValue(be.ip, be.rpc_port, "file_cache_warm_up_rowset_wait_for_compaction_num", 1, /*timeout*/10000)
        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)
        assert num_submitted + 1 == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_segment_num")
        assert num_finished + 1 == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num")
        assert num_idx_submitted + 1 == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_submitted_index_num")
        assert num_idx_finished == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_index_num")


        // a new insert will trigger the sync rowset operation in the following query
        sql """insert into test values (9, 9)"""

        // trigger a query on read cluster without query tolerance, read the origin data
        sql """use @${clusterName2}"""
        sql "set skip_delete_sign=true;"
        sql "set show_hidden_columns=true;"
        sql "set skip_storage_engine_merge=true;"
        qt_cluster2_1 "select * from test order by col0, __DORIS_VERSION_COL__;"
        def tablet_status = getTabletStatus(be.ip, be.http_port, tablet_id)
        def rowsets = tablet_status ["rowsets"]
        assert rowsets[1].contains("[2-5]")
        assert rowsets[2].contains("[6-6]")
        assert rowsets[3].contains("[7-7]")
        assert rowsets[4].contains("[8-8]")
        
        sql "set enable_profile=true;"
        sql "set profile_level=2;"

        // trigger a query on read cluster without query tolerance, read the compacted data
        sql "set query_freshness_tolerance_ms = 5000"
        def t1 = System.currentTimeMillis()
        def queryFreshnessToleranceCount = getBrpcMetricsByCluster(clusterName2, "capture_with_freshness_tolerance_count")
        def fallbackCount = getBrpcMetricsByCluster(clusterName2, "capture_with_freshness_tolerance_fallback_count")
        // should not contains (9,9)
        sql "set skip_delete_sign=true;"
        sql "set show_hidden_columns=true;"
        sql "set skip_storage_engine_merge=true;"
        qt_cluster2_2 "select * from test order by col0, __DORIS_VERSION_COL__;"
        def t2 = System.currentTimeMillis()
        logger.info("query in cluster2 cost=${t2 - t1} ms")
        assert t2 - t1 < 3000
        assert getBrpcMetricsByCluster(clusterName2, "capture_with_freshness_tolerance_count") == queryFreshnessToleranceCount + 1
        assert getBrpcMetricsByCluster(clusterName2, "capture_with_freshness_tolerance_fallback_count") == fallbackCount

        logFileCacheDownloadMetrics(clusterName2)
        logWarmUpRowsetMetrics(clusterName2)

        sleep(10000)
        // assert num_finished + 2 == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num")
        assert 1 == getBrpcMetrics(be.ip, be.rpc_port, "file_cache_warm_up_rowset_wait_for_compaction_timeout_num")

        assert getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_segment_num")
                == getBrpcMetrics(src_be.ip, src_be.rpc_port, "file_cache_event_driven_warm_up_requested_segment_num")
        assert getBrpcMetrics(be.ip, be.rpc_port, "file_cache_event_driven_warm_up_finished_index_num")
                == getBrpcMetrics(src_be.ip, src_be.rpc_port, "file_cache_event_driven_warm_up_requested_index_num")
    }
}
