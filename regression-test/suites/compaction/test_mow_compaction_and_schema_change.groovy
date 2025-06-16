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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_mow_compaction_and_schema_change", "nonConcurrent") {
    def dbName = "regression_test_compaction"
    def testTable = ""
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }

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

    def block_convert_historical_rowsets = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    def getAlterTableState = {
        sql "use ${dbName};"
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${testTable}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    get_be_param("tablet_rowset_stale_sweep_time_sec")
    get_be_param("enable_agg_and_remove_pre_rowsets_delete_bitmap")

    try {
        set_be_param("tablet_rowset_stale_sweep_time_sec", "0")
        for (int method = 0; method < 2; method++) {
            if (method == 0) {
                // off
                set_be_param("enable_agg_and_remove_pre_rowsets_delete_bitmap", "false")
            } else if (method == 1) {
                // solution2: no duplicated key problems
                set_be_param("enable_agg_and_remove_pre_rowsets_delete_bitmap", "true")
            }

            testTable = "test_mow_compaction_and_schema_change_${method}"
            sql """ DROP TABLE IF EXISTS ${testTable} """
            sql """
            create table ${testTable} (`k` int NOT NULL, `v` varchar(10) NOT NULL)
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
            """

            def tablets = sql_return_maparray """ show tablets from ${testTable}; """
            logger.info("tablets: " + tablets)
            assertEquals(1, tablets.size())
            def tablet = tablets[0]

            // 1. write some data
            sql """ INSERT INTO ${testTable} VALUES (1,'99'); """
            sql """ INSERT INTO ${testTable} VALUES (2,'99'); """
            sql """ INSERT INTO ${testTable} VALUES (3,'99'); """
            sql """ INSERT INTO ${testTable} VALUES (4,'99'); """
            sql """ INSERT INTO ${testTable} VALUES (5,'99'); """
            sql "sync"
            order_qt_sql1 """ select * from ${testTable}; """

            // 2. trigger compaction to generate base rowset
            getTabletStatus(tablet)
            assertTrue(triggerCompaction(tablet).contains("Success"))
            waitForCompaction(tablet)
            getTabletStatus(tablet)
            def local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info(testTable + ", local_dm 0: " + local_dm)
            order_qt_sql2 "select * from ${testTable}"

            GetDebugPoint().enableDebugPointForAllBEs("Tablet.delete_expired_stale_rowset.start_delete_unused_rowset")
            GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset") // solution 2

            // 3.0 write some data
            sql """ INSERT INTO ${testTable} VALUES (1, '100'), (2, '97'); """
            sql " sync "

            // 3.1 schema change and block data convert
            block_convert_historical_rowsets()
            sql """alter table ${testTable} modify column v int not null;"""
            // wait for schema change state is running
            def alter_state = ""
            for (int i = 0; i < 100; i++) {
                alter_state = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE tablename='${testTable}' ORDER BY createtime DESC LIMIT 1 """
                if (alter_state.size() > 0 && alter_state[0].State == "RUNNING") {
                    break
                }
                sleep(100)
            }
            logger.info("alter_state: " + alter_state)

            // 3.0 write some data
            sql """ INSERT INTO ${testTable} VALUES (2, '100'); """
            sql """ INSERT INTO ${testTable} VALUES (3, '100'); """
            sql """ INSERT INTO ${testTable} VALUES (4, '100'); """
            sql """ INSERT INTO ${testTable} VALUES (5, '100'); """
            sql """ sync """
            order_qt_sql3 "select * from ${testTable}"

            // 4. trigger compaction
            getTabletStatus(tablet)
            local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info(testTable + ", local_dm 1: " + local_dm)
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: "${tablet.TabletId}", start_version: 7, end_version: 11]);
            assertTrue(triggerCompaction(tablet).contains("Success"))
            waitForCompaction(tablet)
            order_qt_sql4 "select * from ${testTable}"

            // wait for no stale rowsets
            for (int i = 0; i < 20; i++) {
                def tablet_status = getTabletStatus(tablet)
                if (tablet_status["stale_rowsets"].size() == 0 && tablet_status["rowsets"].size() <= 4) {
                    break
                }
                sleep(1000)
            }
            logger.info("wait for no stale rowsets")
            def tablet_status = getTabletStatus(tablet)
            assertEquals(0, tablet_status["stale_rowsets"].size())
            if (isCloudMode()) {
                assertEquals(4, tablet_status["rowsets"].size()) // compaction select [8-11]
            } else {
                assertEquals(3, tablet_status["rowsets"].size())
            }

            // unused rowsets are not deleted
            GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.vacuum_stale_rowsets") // cloud
            GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset") // local
            local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info(testTable + ", local_dm 2: " + local_dm)
            if (method == 0) {
                if (isCloudMode()) {
                    assertEquals(5, local_dm["delete_bitmap_count"])
                    assertEquals(6, local_dm["cardinality"])
                } else {
                    assertEquals(5, local_dm["delete_bitmap_count"])
                    assertEquals(6, local_dm["cardinality"])
                }
            } else if (method == 1) {
                if (isCloudMode()) {
                    assertEquals(3, local_dm["delete_bitmap_count"])
                    assertEquals(6, local_dm["cardinality"]) // the last one is agged
                } else {
                    assertEquals(10, local_dm["cardinality"]) // the last one is agged
                }
            } else if (method == 2) {
                if (isCloudMode()) { // compaction select [8-11]
                    assertEquals(2, local_dm["delete_bitmap_count"])
                    assertEquals(6, local_dm["cardinality"])
                } else {
                    assertEquals(2, local_dm["delete_bitmap_count"])
                    assertEquals(6, local_dm["cardinality"])
                }
            }

            // 5. unblock schema change and wait for schema change done
            GetDebugPoint().clearDebugPointsForAllBEs()
            alter_state = getAlterTableState()
            logger.info("alter_state: " + alter_state)
            order_qt_sql5 "select * from ${testTable}"

            // 6. check duplicated keys
            def result = sql "select `k`, count(*) from ${testTable} group by `k` having count(*) > 1"
            if (method == 0 || method == 1) {
                logger.info("no duplicated keys: " + result)
                assertEquals(0, result.size())
            } /*else if (method == 2) {
                logger.info("find duplicated keys: " + result)
                assertEquals(2, result.size())
            }*/
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    } finally {
        reset_be_param("tablet_rowset_stale_sweep_time_sec")
        reset_be_param("enable_agg_and_remove_pre_rowsets_delete_bitmap")
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
