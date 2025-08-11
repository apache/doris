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

suite('test_query_driven_warmup_conflict_with_warmup_job', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'block_file_cache_monitor_interval_sec=1',
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

    def updateBeConf = {cluster, key, value ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[4]
            def (code, out, err) = update_be_config(ip, port, key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
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

    def getBrpcMetricsByCluster = {cluster, name->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        assert cluster_bes.size() > 0, "No backend found for cluster ${cluster}"
        def be = cluster_bes[0]
        def ip = be[1]
        def port = be[5]
        return getBrpcMetrics(ip, port, name)
    }

    def injectS3FileReadSlow = {cluster, sleep_s ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        def injectName = 'S3FileReader::read_at_impl.io_slow'
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[4]
            GetDebugPoint().enableDebugPoint(ip, port as int, NodeType.BE, injectName, [sleep:sleep_s, execute:4])
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

        updateBeConf(clusterName2, "enable_query_driven_warmup", "true")

        def jsonSlurper = new JsonSlurper()
        def clusterId1 = jsonSlurper.parseText(tag1).compute_group_id
        def clusterId2 = jsonSlurper.parseText(tag2).compute_group_id

        // Ensure we are in source cluster
        sql """use @${clusterName1}"""

        sql """
            create table test (
                col0 int not null,
                col1 variant NOT NULL
            ) DUPLICATE KEY(`col0`)
            DISTRIBUTED BY HASH(col0) BUCKETS 1
            PROPERTIES ("file_cache_ttl_seconds" = "3600", "disable_auto_compaction" = "true");
        """

        clearFileCacheOnAllBackends()
        sleep(15000)

        sql """use @${clusterName1}"""
        sql """insert into test values (1, '{"a" : 1.0}')"""
        sql """insert into test values (2, '{"a" : 111.1111}')"""
        sql """insert into test values (3, '{"a" : "11111"}')"""
        sql """insert into test values (4, '{"a" : 1111111111}')"""
        sql """insert into test values (5, '{"a" : 1111.11111}')"""

        // make warm up job slow
        injectS3FileReadSlow(clusterName2, 5)
        def jobId_ = sql "WARM UP COMPUTE GROUP ${clusterName2} WITH TABLE test"
        sleep(1000)
        // switch to read cluster, run query, should not trigger any warmup
        sql """use @${clusterName2}"""
        qt_sql """select * from test"""
        // no duplicate warmup
        def triggerd_by_rowset = getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_triggered_by_sync_rowset_num")
        def triggerd_by_job = getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_triggered_by_job_num");
        assertEquals(5, triggerd_by_rowset + triggerd_by_job)

        // switch to source cluster and trigger compaction
        sql """use @${clusterName1}"""
        trigger_and_wait_compaction("test", "cumulative")
        sql """insert into test values (6, '{"a" : 1111.11111}')"""
        sleep(2000)

        // switch to read cluster, trigger a sync rowset
        sql """use @${clusterName2}"""
        qt_sql """select * from test"""
        // wait until the injection complete
        sleep(3000)

        assertTrue(getBrpcMetricsByCluster(clusterName2, "file_cache_download_submitted_num") >= 7)
        def triggerd_by_rowset1 = getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_triggered_by_sync_rowset_num")
        def triggerd_by_job1 = getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_triggered_by_job_num");
        assertEquals(triggerd_by_rowset + 2, triggerd_by_rowset1)
        assertEquals(7, triggerd_by_rowset1 + triggerd_by_job1)
        assertEquals(1, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_num"))
        assertEquals(1, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_add_num"))
        assertEquals(0, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_add_failure_num"))
    }
}
