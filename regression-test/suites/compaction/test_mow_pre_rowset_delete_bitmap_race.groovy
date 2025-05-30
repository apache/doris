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

// after cu compaction, when agg delete bitmap for pre rowsets, block merge delete bitmap to tablet meta
// compaction for pre rowsets, and wait for they are moved from stale rowsets(in unused rowsets and can not be deleted because the first step hold rowset reference)
// unblock the merge, the delete bitmap of these delete bitmap can be deleted
suite("test_mow_pre_rowset_delete_bitmap_race", "nonConcurrent") {
    def testTable = "test_mow_pre_rowset_delete_bitmap_race"
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

    def triggerCompaction = { tablet, compact_type = "cumulative" ->
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

    GetDebugPoint().clearDebugPointsForAllBEs()
    get_be_param("tablet_rowset_stale_sweep_time_sec")
    set_be_param("tablet_rowset_stale_sweep_time_sec", "0")
    get_be_param("compaction_promotion_version_count")
    set_be_param("compaction_promotion_version_count", "5")

    onFinish {
        GetDebugPoint().clearDebugPointsForAllBEs()
        reset_be_param("tablet_rowset_stale_sweep_time_sec")
        reset_be_param("compaction_promotion_version_count")
    }

    GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")
    GetDebugPoint().enableDebugPointForAllBEs("Tablet.delete_expired_stale_rowset.start_delete_unused_rowset")
    GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.vacuum_stale_rowsets") // cloud
    GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset") // local

    // 1. write some data
    sql """ INSERT INTO ${testTable} VALUES (1,99); """
    sql """ INSERT INTO ${testTable} VALUES (2,99); """
    sql """ INSERT INTO ${testTable} VALUES (3,99); """
    sql """ INSERT INTO ${testTable} VALUES (4,99); """
    sql """ INSERT INTO ${testTable} VALUES (5,99); """
    sql "sync"
    getTabletStatus(tablet)
    getLocalDeleteBitmapStatus(tablet)

    // 2. trigger compaction
    assertTrue(triggerCompaction(tablet).contains("Success"))
    waitForCompaction(tablet)
    // wait for compaction finish
    for (int i = 0; i < 10; i++) {
        def tablet_status = getTabletStatus(tablet)
        if (tablet_status["rowsets"].size() <= 2) {
            break
        }
        sleep(200)
    }

    // 3. write some data
    sql """ INSERT INTO ${testTable} VALUES (1,100); """
    sql """ INSERT INTO ${testTable} VALUES (2,100); """
    sql """ INSERT INTO ${testTable} VALUES (3,100); """
    sql """ INSERT INTO ${testTable} VALUES (4,100); """
    sql """ INSERT INTO ${testTable} VALUES (5,100); """
    sql "sync"
    def row_count = sql "select count() from ${testTable};"
    logger.info("row_count: " + row_count)

    /**
     * case：
     1. 导入r1-r5，触发cu，产生r1-5
     2. 导入r6-r10，（有delete bitmap，删除了r1-5中的数据）触发cu，block住merge(拿着r1-5的rowset的指针)
     3. 再做一个compaction，对r1-r10，并触发清理，新增的逻辑这里会检查引用计数，导致清理不掉(拿着r1-5的rowset的指针)；原来的逻辑在删除stale的时候清理，后面没有清理的时机了，所以是有问题的，但存算一体上没有问题
     4. 允许merge，merge后
     */

    // 4. block compaction merge delete bitmap of pre rowset; trigger compaction
    GetDebugPoint().enableDebugPointForAllBEs("BaseTablet.agg_delete_bitmap_for_stale_rowsets.merge_delete_bitmap.block")
    GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
            [tablet_id: "${tablet.tablet_id}", start_version: 7, end_version: 11]);
    assertTrue(triggerCompaction(tablet).contains("Success"))
    waitForCompaction(tablet)
    // wait for compaction finish
    for (int i = 0; i < 10; i++) {
        def tablet_status = getTabletStatus(tablet)
        if (tablet_status["rowsets"].size() == 3) {
            break
        }
        sleep(200)
    }
    def local_dm = getLocalDeleteBitmapStatus(tablet)
    assertEquals(5, local_dm["delete_bitmap_count"])
    assertEquals(5, local_dm["cardinality"])

    // 5. trigger base compaction, wait for no stale rowsets
    assertTrue(triggerCompaction(tablet, "full").contains("Success"))
    waitForCompaction(tablet)
    // wait for compaction finish
    for (int i = 0; i < 10; i++) {
        def tablet_status = getTabletStatus(tablet)
        if (tablet_status["rowsets"].size() <= 2) {
            break
        }
        sleep(200)
    }
    // unused rowset can not be deleted because it's referenced by the agg step
    local_dm = getLocalDeleteBitmapStatus(tablet)
    assertEquals(5, local_dm["delete_bitmap_count"])
    assertEquals(5, local_dm["cardinality"])
    getTabletStatus(tablet)

    GetDebugPoint().disableDebugPointForAllBEs("BaseTablet.agg_delete_bitmap_for_stale_rowsets.merge_delete_bitmap.block")
    for (int i = 0; i < 20; i++) {
        def local_delete_bitmap_status = getLocalDeleteBitmapStatus(tablet)
        if (local_delete_bitmap_status["delete_bitmap_count"] == 0) {
            break
        }
        sleep(100)
    }
}
