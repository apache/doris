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

// when compaction for one rowsets with multiple segments, the delete bitmap can be deleted
suite("test_mow_compact_multi_segments", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()
    def tableName = "test_mow_compact_multi_segments"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

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

    def getTabletStatus = { tablet, rowsetIndex, lastRowsetSegmentNum, enableAssert = false ->
        String compactionUrl = tablet["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
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

    def getLocalDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
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

    // batch_size is 4164 in csv_reader.cpp
    // _batch_size is 8192 in vtablet_writer.cpp
    onFinish {
        GetDebugPoint().clearDebugPointsForAllBEs()
        reset_be_param("doris_scanner_row_bytes")
        reset_be_param("tablet_rowset_stale_sweep_time_sec")
    }
    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
    GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")
    GetDebugPoint().enableDebugPointForAllBEs("Tablet.delete_expired_stale_rowset.start_delete_unused_rowset")
    get_be_param("doris_scanner_row_bytes")
    set_be_param("doris_scanner_row_bytes", "1")
    get_be_param("tablet_rowset_stale_sweep_time_sec")
    set_be_param("tablet_rowset_stale_sweep_time_sec", "0")

    tableName = "test_compact_multi_segments_"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
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
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    def tablet = tablets[0]
    String tablet_id = tablet.TabletId
    def backend_id = tablet.BackendId

    // load 1
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'compress_type', 'GZ'
        file 'test_schema_change_add_key_column.csv.gz'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(8192, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    def rowCount1 = sql """ select count() from ${tableName}; """
    logger.info("rowCount1: ${rowCount1}")
    // check generate 3 segments
    getTabletStatus(tablet, 2, 3)

    // trigger compaction
    GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
            [tablet_id: "${tablet.TabletId}", start_version: 2, end_version: 2])
    def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
    logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def compactJson = parseJson(out.trim())
    logger.info("compact json: " + compactJson)
    // check generate 1 segments
    for (int i = 0; i < 20; i++) {
        if (getTabletStatus(tablet, 2, 1, false)) {
            break
        }
        sleep(100)
    }
    getTabletStatus(tablet, 2, 1)
    sql """ select * from ${tableName} limit 1; """

    // load 2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'compress_type', 'GZ'
        file 'test_schema_change_add_key_column1.csv.gz'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20480, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    def rowCount2 = sql """ select count() from ${tableName}; """
    logger.info("rowCount2: ${rowCount2}")
    // check generate 3 segments
    getTabletStatus(tablet, 3, 6)
    def local_dm = getLocalDeleteBitmapStatus(tablet)
    logger.info("local delete bitmap 1: " + local_dm)

    // trigger compaction for load 2
    GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
            [tablet_id: "${tablet.TabletId}", start_version: 3, end_version: 3])
    (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
    logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    compactJson = parseJson(out.trim())
    logger.info("compact json: " + compactJson)
    waitForCompaction(tablet)
    // check generate 1 segments
    for (int i = 0; i < 20; i++) {
        if (getTabletStatus(tablet, 3, 1, false)) {
            break
        }
        sleep(100)
    }
    getTabletStatus(tablet, 3, 1)

    GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.vacuum_stale_rowsets") // cloud
    GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset") // local
    local_dm = getLocalDeleteBitmapStatus(tablet)
    logger.info("local delete bitmap 2: " + local_dm)
    assertEquals(1, local_dm["delete_bitmap_count"])
}
