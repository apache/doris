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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite('test_query_driven_warmup_compaction_conflict', 'docker') {
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

    def injectAddOverlapRowsetSleep = {cluster, sleep_s ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        def injectName = 'CloudTablet.warm_up_done_cb.inject_sleep_s'
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[4]
            GetDebugPoint().enableDebugPoint(ip, port as int, NodeType.BE, injectName, [sleep:sleep_s])
        }
    }

    def triggerCompaction = { cluster, tablet_id, compact_type ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        assert cluster_bes.size() > 0, "No backend found for cluster ${cluster}"
        def be = cluster_bes[0]
        def ip = be[1]
        def port = be[4]
        // trigger compactions for all tablets in ${tableName}
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://${ip}:${port}")
        sb.append("/api/compaction/run?tablet_id=")
        sb.append(tablet_id)
        sb.append("&compact_type=${compact_type}")

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        return out
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

        sql """insert into test values (1, '{"a" : 1.0}')"""
        sql """insert into test values (2, '{"a" : 111.1111}')"""
        sql """insert into test values (3, '{"a" : "11111"}')"""
        sql """insert into test values (4, '{"a" : 1111111111}')"""
        sql """insert into test values (5, '{"a" : 1111.11111}')"""

        // switch to read cluster, trigger a sync rowset
        sql """use @${clusterName2}"""
        qt_sql """select * from test"""
        assertEquals(5, getBrpcMetricsByCluster(clusterName2, "file_cache_download_submitted_num"))
        assertEquals(0, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_num"))
        assertEquals(0, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_add_num"))

        // switch to source cluster and trigger compaction
        sql """use @${clusterName1}"""
        trigger_and_wait_compaction("test", "cumulative")
        sql """insert into test values (6, '{"a" : 1111.11111}')"""
        sql """insert into test values (7, '{"a" : 1.0}')"""
        sql """insert into test values (8, '{"a" : 111.1111}')"""
        sql """insert into test values (9, '{"a" : "11111"}')"""
        sql """insert into test values (10, '{"a" : 1111111111}')"""
        sql """insert into test values (11, '{"a" : 1111.11111}')"""
        sleep(2000)

        // switch to read cluster, trigger a sync rowset
        sql """use @${clusterName2}"""
        sql """set enable_profile=true"""

        // inject sleep on warm_up_done_cb, make warm up inflight for longger time
        injectAddOverlapRowsetSleep(clusterName2, 20);
        qt_sql """select * from test"""
        sleep(1000)
        assertTrue(getBrpcMetricsByCluster(clusterName2, "file_cache_download_submitted_num") >= 11)
        assertEquals(1, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_num"))
        assertEquals(0, getBrpcMetricsByCluster(clusterName2, "file_cache_query_driven_warmup_delayed_rowset_add_num"))

        def tablets = sql_return_maparray """show tablets from test"""
        // cluster2 trigger cumu compaction failed (conflict with warmup rowsets)
        assertTrue(triggerCompaction(clusterName2, tablets[0].TabletId, "cumulative").contains("E-2000"));

        // wait injection complete
        sleep(20000)
        // cluster2 trigger cumu compaction succeed
        assertTrue(triggerCompaction(clusterName2, tablets[0].TabletId, "cumulative").contains("Success"));
    }
}
