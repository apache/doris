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

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType
import groovy.json.JsonSlurper

suite('test_mow_agg_delete_bitmap', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'enable_workload_group=false',
    ]
    options.beConfigs += [
        'enable_debug_points=true',
        'tablet_rowset_stale_sweep_time_sec=0',
        'vacuum_stale_rowsets_interval_s=10',
    ]

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]

    def triggerCompaction = { tablet ->
        def compact_type = "cumulative"
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        if (compact_type == "cumulative") {
            def (code_1, out_1, err_1) = be_run_cumulative_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
            assertEquals(code_1, 0)
            return out_1
        } else if (compact_type == "full") {
            def (code_2, out_2, err_2) = be_run_full_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_2 + ", out=" + out_2 + ", err=" + err_2)
            assertEquals(code_2, 0)
            return out_2
        } else {
            assertFalse(True)
        }
    }

    def getTabletStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
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

    def waitForCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def running = true
        do {
            Thread.sleep(1000)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            def process = command.execute()
            def code = process.waitFor()
            def out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def getLocalDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get local delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    def getMsDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_ms?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get ms delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    docker(options) {
        def testTable = "test_mow"

        // add cluster1
        cluster.addBackend(1, "cluster1")
        cluster.addBackend(1, "cluster2")
        def ret = sql_return_maparray """show clusters"""
        logger.info("clusters: " + ret)
        def cluster0 = ret.stream().filter(cluster -> cluster.is_current == "TRUE").findFirst().orElse(null)
        def cluster1 = ret.stream().filter(cluster -> cluster.cluster == "cluster1").findFirst().orElse(null)
        def cluster2 = ret.stream().filter(cluster -> cluster.cluster == "cluster2").findFirst().orElse(null)
        assertTrue(cluster1 != null)
        assertTrue(cluster2 != null)
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

        sql """
            create table ${testTable} (`k` int NOT NULL, `v` int NOT NULL)
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            );
        """
        // get tablet in cluster0
        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets in cluster 0: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        // get tablet in cluster1
        sql """use @${cluster1.cluster}"""
        tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets in cluster 1: " + tablets)
        assertEquals(1, tablets.size())
        def tablet1 = tablets[0]
        // get tablet in cluster2
        sql """use @${cluster2.cluster}"""
        tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets in cluster 2: " + tablets)
        assertEquals(1, tablets.size())
        def tablet2 = tablets[0]

        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")

        // 1. insert some data
        sql """use @${cluster0.cluster}"""
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql "sync"
        order_qt_sql1 """ select * from ${testTable}; """

        // read data from cluster2
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def url = "jdbc:mysql://${fes[0].Host}:${fes[0].QueryPort}/"
        logger.info("url: " + url)
        def databases = sql 'SELECT DATABASE()'
        def dbName = databases[0][0]
        AtomicBoolean query_result = new AtomicBoolean(true)
        def query = {
            connect( context.config.jdbcUser,  context.config.jdbcPassword,  url) {
                sql """use @${cluster2.cluster}"""
                logger.info("query start")
                def results = sql_return_maparray """ select * from ${dbName}.${testTable}; """
                logger.info("query result: " + results)
                Set<String> keys = new HashSet<>()
                for (final def result in results) {
                    if (keys.contains(result.k)) {
                        logger.info("find duplicate key: " + result.k)
                        query_result.set(false)
                        break
                    }
                    keys.add(result.k)
                }
                logger.info("query finish. query_result: " + query_result.get())
            }
        }
        def tablet2_backendId = tablet2.BackendId
        GetDebugPoint().enableDebugPoint(backendId_to_backendIP[tablet2_backendId], backendId_to_backendHttpPort[tablet2_backendId] as int, NodeType.BE, "CloudMetaMgr::sync_tablet_rowsets.sync_tablet_delete_bitmap.block")
        Thread query_thread = new Thread(() -> query())
        query_thread.start()
        sleep(100)

        // 2. trigger compaction 0
        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        logger.info("after compaction 1")
        getTabletStatus(tablet)
        GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.vacuum_stale_rowsets") // cloud
        def local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 0.2: " + local_dm)
        assertEquals(0, local_dm.delete_bitmap_count)
        assertEquals(0, local_dm.cardinality)
        def ms_dm = getMsDeleteBitmapStatus(tablet)
        logger.info("ms_dm: " + ms_dm)
        assertEquals(0, ms_dm.delete_bitmap_count)
        assertEquals(0, ms_dm.cardinality)

        GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.sync_tablet_delete_bitmap.block")
        query_thread.join()
        assertTrue(query_result.get(), "find duplicated keys")

        sql """use @${cluster1.cluster}"""
        order_qt_sql2 """ select * from ${testTable}; """

        // 3. insert some data
        logger.info("use cluster 0")
        sql """use @${cluster0.cluster}"""
        sql """ INSERT INTO ${testTable} VALUES (1,100); """
        sql """ INSERT INTO ${testTable} VALUES (2,100); """
        sql """ INSERT INTO ${testTable} VALUES (3,100); """
        sql """ INSERT INTO ${testTable} VALUES (4,100); """
        sql """ INSERT INTO ${testTable} VALUES (5,100); """
        sql """ sync """
        order_qt_sql3 """ select * from ${testTable}; """
        getTabletStatus(tablet)
        local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 0.3: " + local_dm)
        assertEquals(4, local_dm.delete_bitmap_count)
        assertEquals(4, local_dm.cardinality)

        sql """use @${cluster1.cluster}"""
        order_qt_sql4 """ select * from ${testTable}; """
        local_dm = getLocalDeleteBitmapStatus(tablet1)
        logger.info("local_dm 1.3: " + local_dm)
        assertEquals(4, local_dm.delete_bitmap_count)
        assertEquals(4, local_dm.cardinality)

        logger.info("use cluster 0")
        sql """use @${cluster0.cluster}"""

        // 4. trigger compaction 1
        GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                [tablet_id:"${tablet_id}", start_version: 7, end_version: 11]);
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        def tablet_status = getTabletStatus(tablet)
        assertEquals(3, tablet_status.rowsets.size())
        ms_dm = getMsDeleteBitmapStatus(tablet)
        logger.info("ms_dm: " + ms_dm)
        assertEquals(1, ms_dm.delete_bitmap_count)
        assertEquals(4, ms_dm.cardinality)
        for (int i = 0; i < 100; i++) {
            local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info("local_dm 0.4: " + local_dm)
            if (local_dm.delete_bitmap_count == 1) {
                break
            }
            sleep(2000)
        }
        assertEquals(1, local_dm.delete_bitmap_count)
        assertEquals(4, local_dm.cardinality)
        sql """ insert into ${testTable} values (6, 100); """
        sql """ sync """

        logger.info("use cluster 1")
        sql """use @${cluster1.cluster}"""
        order_qt_sql5 """ select * from ${testTable}; """
        getTabletStatus(tablet1)
        for (int i = 0; i < 100; i++) {
            local_dm = getLocalDeleteBitmapStatus(tablet1)
            logger.info("local_dm 1.4: " + local_dm)
            if (local_dm.delete_bitmap_count == 1) {
                break
            }
            sleep(2000)
        }
        assertEquals(1, local_dm.delete_bitmap_count)
        assertEquals(4, local_dm.cardinality)
    }
}
