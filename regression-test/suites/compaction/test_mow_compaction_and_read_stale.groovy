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

import java.util.concurrent.atomic.AtomicBoolean;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_mow_compaction_and_read_stale", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    def testTable = "test_mow_compaction_and_read_stale"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

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

    AtomicBoolean query_result = new AtomicBoolean(true)
    def query = {
        logger.info("query start")
        def results = sql_return_maparray """ select * from ${testTable}; """
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

    sql """ DROP TABLE IF EXISTS ${testTable} """
    sql """
        create table ${testTable} (`k` int NOT NULL, `v` int NOT NULL)
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
    String tablet_id = tablet.TabletId

    GetDebugPoint().clearDebugPointsForAllBEs()
    get_be_param("compaction_promotion_version_count")
    get_be_param("tablet_rowset_stale_sweep_time_sec")
    set_be_param("compaction_promotion_version_count", "5")
    set_be_param("tablet_rowset_stale_sweep_time_sec", "0")

    try {
        // write some data
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql """ INSERT INTO ${testTable} VALUES (5,99); """
        sql "sync"
        order_qt_sql1 """ select * from ${testTable}; """

        // trigger compaction to generate base rowset
        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        order_qt_sql2 "select * from ${testTable}"

        // write some data
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql """ INSERT INTO ${testTable} VALUES (5,99); """
        sql """ sync """
        order_qt_sql3 "select * from ${testTable}"

        // trigger and block one query
        GetDebugPoint().enableDebugPointForAllBEs("NewOlapScanner::_init_tablet_reader_params.block")
        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")
        GetDebugPoint().enableDebugPointForAllBEs("Tablet.delete_expired_stale_rowset.start_delete_unused_rowset")
        Thread query_thread = new Thread(() -> query())
        query_thread.start()
        sleep(100)

        // trigger compaction
        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        // wait for stale rowsets are deleted
        boolean is_stale_rowsets_deleted = false
        for (int i= 0; i < 100; i++) {
            def tablet_status = getTabletStatus(tablet)
            if (tablet_status["stale_rowsets"].size() == 0) {
                is_stale_rowsets_deleted = true
                break
            }
            sleep(500)
        }
        assertTrue(is_stale_rowsets_deleted, "stale rowsets are not deleted")
        // check to delete bitmap of stale rowsets is not deleted
        sleep(1000)
        def local_dm_status = getLocalDeleteBitmapStatus(tablet)
        // 4 delete bitmap for unused rowsets, 1 delete bitmap is useful
        assertTrue(local_dm_status["delete_bitmap_count"] <= 5)

        // unnlock query and check no duplicated keys
        GetDebugPoint().disableDebugPointForAllBEs("NewOlapScanner::_init_tablet_reader_params.block")
        query_thread.join()
        assertTrue(query_result.get(), "find duplicated keys")

        // check delete bitmap of compaction2 stale rowsets are deleted
        // write some data
        sql """ INSERT INTO ${testTable} VALUES (1,99); """
        sql """ INSERT INTO ${testTable} VALUES (2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql """ INSERT INTO ${testTable} VALUES (5,100); """
        sql "sync"
        order_qt_sql4 "select * from ${testTable}"
        // trigger compaction
        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        boolean is_compaction_finished = false
        for (int i =0 ; i < 100; i++) {
            def tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 4) {
                is_compaction_finished = true
                break
            }
            sleep(500)
        }
        assertTrue(is_compaction_finished, "compaction is not finished")
        // check delete bitmap count
        boolean is_local_dm_deleted = false
        for (int i = 0; i < 100; i++) {
            local_dm_status = getLocalDeleteBitmapStatus(tablet)
            if (local_dm_status["delete_bitmap_count"] < 10) {
                is_local_dm_deleted = true
                break
            }
            sleep(500)
        }
        assertTrue(is_local_dm_deleted, "delete bitmap of compaction2 stale rowsets are not deleted")
        order_qt_sql5 "select * from ${testTable}"
    } finally {
        reset_be_param("compaction_promotion_version_count")
        reset_be_param("tablet_rowset_stale_sweep_time_sec")
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
