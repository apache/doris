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

suite("test_compact_multi_segments", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()
    def tableName = "test_compact_multi_segments"

    def getTabletStatus = { rowsetNum, lastRowsetSegmentNum ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        logger.info("tablets: ${tablets}")
        assertTrue(tablets.size() >= 1)
        String compactionUrl = ""
        for (Map<String, String> tablet : tablets) {
            compactionUrl = tablet["CompactionStatus"]
        }
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        assertTrue(tabletJson.rowsets.size() >= rowsetNum)
        def rowset = tabletJson.rowsets.get(rowsetNum - 1)
        logger.info("rowset: ${rowset}")
        int start_index = rowset.indexOf("]")
        int end_index = rowset.indexOf("DATA")
        def segmentNumStr = rowset.substring(start_index + 1, end_index).trim()
        logger.info("segmentNumStr: ${segmentNumStr}")
        assertEquals(lastRowsetSegmentNum, Integer.parseInt(segmentNumStr))
    }

    // batch_size is 4164 in csv_reader.cpp
    // _batch_size is 8192 in vtablet_writer.cpp
    def backendId_to_params = get_be_param("doris_scanner_row_bytes")
    onFinish {
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
        set_original_be_param("doris_scanner_row_bytes", backendId_to_params)
    }
    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
    set_be_param.call("doris_scanner_row_bytes", "1")

    for (int j = 0; j < 2; j++) {
        tableName = "test_compact_multi_segments_" + j
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL, 
                `k2` int(11) NULL, 
                `v3` int(11) NULL,
                `v4` int(11) NULL
            ) unique KEY(`k1`, `k2`) 
            cluster by(`v3`, `v4`) 
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            """ + (j == 1 ? "\"function_column.sequence_col\"='v4', " : "") +
            """
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
            """

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            file 'test_schema_change_add_key_column.csv'
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
        // check generate 3 segments
        sql """ select * from ${tableName} limit 1; """
        getTabletStatus(2, 3)

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            file 'test_schema_change_add_key_column1.csv'
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
        // check generate 3 segments
        sql """ select * from ${tableName} limit 1; """
        getTabletStatus(3, 6)

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            file 'test_schema_change_add_key_column2.csv'
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
        // check generate 3 segments
        sql """ select * from ${tableName} limit 1; """
        getTabletStatus(4, 6)

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            file 'test_schema_change_add_key_column3.csv'
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
        // check generate 3 segments
        sql """ select * from ${tableName} limit 1; """
        getTabletStatus(5, 6)

        def rowCount1 = sql """ select count() from ${tableName}; """
        logger.info("rowCount1: ${rowCount1}")

        // trigger compaction
        // get be info
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def backend_id = tablet.BackendId

            def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            logger.info("compact json: " + compactJson)

            for (int i = 0; i < 10; i++) {
                (code, out, err) = be_show_tablet_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("loop " + i + ", Show tablet status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def json = parseJson(out.trim())
                logger.info("tablet rowsets: " + json)
                if (json.rowsets.size() <= 2) {
                    break
                }
                sleep(2000)
            }
        }

        // check generate 1 segments
        sql """ select * from ${tableName} limit 1; """
        getTabletStatus(2, 1) // [2-5]

        // check row count
        def rowCount2 = sql """ select count() from ${tableName}; """
        logger.info("rowCount2: ${rowCount2}")
        assertEquals(rowCount1[0][0], rowCount2[0][0])
        // check no duplicated key
        def result = sql """ select `k1`, `k2`, count(*) a from ${tableName} group by `k1`, `k2` having a > 1; """
        logger.info("result: ${result}")
        assertEquals(0, result.size())
        // check one row value
        order_qt_select1 """ select * from ${tableName} where `k1` = 12345; """
        order_qt_select2 """ select * from ${tableName} where `k1` = 17320; """
        order_qt_select3 """ select * from ${tableName} where `k1` = 59832 and `k2` = 36673; """
    }
}
