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

suite("test_mow_load_delete_bitmap_segcompaction_race", "nonConcurrent") {
    if (isCloudMode()) {
        logger.info("skip test in cloud mode")
        return
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    def segcompactionBatchSize = get_be_param("segcompaction_batch_size")
    def maxSegmentNumPerRowset = get_be_param("max_segment_num_per_rowset")
    def dorisScannerRowBytes = get_be_param("doris_scanner_row_bytes")
    def enableAdaptiveBatchSize = get_be_param("enable_adaptive_batch_size")

    onFinish {
        GetDebugPoint().clearDebugPointsForAllBEs()
        set_original_be_param("segcompaction_batch_size", segcompactionBatchSize)
        set_original_be_param("max_segment_num_per_rowset", maxSegmentNumPerRowset)
        set_original_be_param("doris_scanner_row_bytes", dorisScannerRowBytes)
        set_original_be_param("enable_adaptive_batch_size", enableAdaptiveBatchSize)
    }

    sql "DROP TABLE IF EXISTS test_mow_load_delete_bitmap_segcompaction_race FORCE"
    sql """
        CREATE TABLE test_mow_load_delete_bitmap_segcompaction_race (
            k1 int NOT NULL,
            c1 int,
            c2 int
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_mow_load_delete_bitmap_segcompaction_race VALUES
            (100001, 1, 1),
            (100002, 2, 2),
            (100003, 3, 3)
    """
    sql "sync"

    set_be_param.call("segcompaction_batch_size", "2")
    set_be_param.call("max_segment_num_per_rowset", "3")
    set_be_param.call("doris_scanner_row_bytes", "1")
    set_be_param.call("enable_adaptive_batch_size", "false")
    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
    GetDebugPoint().enableDebugPointForAllBEs(
            "BetaRowsetWriter.generate_delete_bitmap.sleep_before_build_tmp",
            [sleep_ms: 2000])
    GetDebugPoint().enableDebugPointForAllBEs(
            "BetaRowsetWriter.add_segment.sleep_before_segcompaction",
            [segment_id: 1, sleep_ms: 200])

    def checkLastRowsetSegmentNum = { expectedSegmentNum ->
        def tablets = sql_return_maparray """ show tablets from test_mow_load_delete_bitmap_segcompaction_race; """
        logger.info("tablets: ${tablets}")
        String compactionUrl = tablets[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        def rowset = tabletJson.rowsets.get(tabletJson.rowsets.size() - 1)
        logger.info("last rowset: ${rowset}")
        int startIndex = rowset.indexOf("]")
        int endIndex = rowset.indexOf("DATA")
        def segmentNum = Integer.parseInt(rowset.substring(startIndex + 1, endIndex).trim())
        assertEquals(expectedSegmentNum, segmentNum)
    }

    String content = ""
    (1..8192).each {
        content += "${it},${it},${it}\n"
    }
    content += content

    streamLoad {
        table "test_mow_load_delete_bitmap_segcompaction_race"
        set "column_separator", ","
        inputStream new ByteArrayInputStream(content.getBytes())
        time 120000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            logger.info("stream load result: ${result}")
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(16384, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    checkLastRowsetSegmentNum(3)
    qt_sql "select count() from test_mow_load_delete_bitmap_segcompaction_race;"
    qt_dup_key_count """select count() from (
            select k1, count() as cnt
            from test_mow_load_delete_bitmap_segcompaction_race
            group by k1
            having cnt > 1
        ) t;"""
}
