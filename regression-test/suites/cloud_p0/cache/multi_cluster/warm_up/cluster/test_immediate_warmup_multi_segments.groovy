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
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.NodeType
import groovy.json.JsonSlurper

suite('test_immediate_warmup_multi_segments', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'block_file_cache_monitor_interval_sec=1',
        'tablet_rowset_stale_sweep_time_sec=0',
        'vacuum_stale_rowsets_interval_s=10',
        'doris_scanner_row_bytes=1',
    ]
    options.enableDebugPoints()
    options.cloudMode = true

    def testTable = "test"

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
            GetDebugPoint().enableDebugPoint(ip, port as int, NodeType.BE, injectName, [sleep:sleep_s, execute:1])
        }
    }

    def getTabletStatus = { cluster, tablet_id, rowsetIndex, lastRowsetSegmentNum, enableAssert = false ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        assert cluster_bes.size() > 0, "No backend found for cluster ${cluster}"
        def be = cluster_bes[0]
        def ip = be[1]
        def port = be[4]
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

        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        assertTrue(tabletJson.rowsets.size() >= rowsetIndex)
        def rowset = tabletJson.rowsets.get(rowsetIndex - 1)
        logger.info("rowset: ${rowset}")

        int start_index = rowset.indexOf("]")
        int end_index = rowset.indexOf("DATA")
        def segmentNumStr = rowset.substring(start_index + 1, end_index).trim()
        logger.info("segmentNumStr: ${segmentNumStr}")
        if (enableAssert) {
            assertEquals(lastRowsetSegmentNum, Integer.parseInt(segmentNumStr))
        } else {
            return lastRowsetSegmentNum == Integer.parseInt(segmentNumStr);
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

        updateBeConf(clusterName2, "enable_warmup_immediately_on_new_rowset", "true")

        // Ensure we are in source cluster
        sql """use @${clusterName1}"""
        sql """ DROP TABLE IF EXISTS ${testTable} """
        sql """ CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` int(11) NULL,
            `k2` int(11) NULL,
            `v3` int(11) NULL,
            `v4` int(11) NULL
        ) unique KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
            );
        """

        clearFileCacheOnAllBackends()
        sleep(15000)

        def tablets = sql_return_maparray """ show tablets from test; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        try {
            // load 1
            streamLoad {
                table "${testTable}"
                set 'column_separator', ','
                set 'compress_type', 'GZ'
                file 'test_schema_change_add_key_column.csv.gz'
                time 10000 // limit inflight 10s

                check { res, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(res)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(8192, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            def rowCount1 = sql """ select count() from ${testTable}; """
            logger.info("rowCount1: ${rowCount1}")
            // check generate 3 segments
            getTabletStatus(clusterName1, tablet_id, 2, 3, true)

            // switch to read cluster, trigger a sync rowset
            injectS3FileReadSlow(clusterName2, 10)
            // the query will be blocked by the injection, we call it async
            def future = thread {
                sql """use @${clusterName2}"""
                sql """select * from test"""
            }
            sleep(1000)
            assertEquals(1, getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_triggered_by_sync_rowset_num"))
            assertEquals(2, getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_segment_complete_num"))
            assertEquals(0, getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_complete_num"))

            future.get()
            assertEquals(3, getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_segment_complete_num"))
            assertEquals(1, getBrpcMetricsByCluster(clusterName2, "file_cache_warm_up_rowset_complete_num"))
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
