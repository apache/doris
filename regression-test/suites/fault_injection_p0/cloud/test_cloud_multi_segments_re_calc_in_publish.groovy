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

suite("test_cloud_multi_segments_re_calc_in_publish", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def table1 = "test_cloud_multi_segments_re_calc_in_publish"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(99999,99999,99999);"
    sql "insert into ${table1} values(88888,88888,88888);"
    sql "insert into ${table1} values(77777,77777,77777);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    def checkSegmentNum = { rowsetNum, lastRowsetSegmentNum ->
        def tablets = sql_return_maparray """ show tablets from ${table1}; """
        logger.info("tablets: ${tablets}")
        String compactionUrl = tablets[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        assert tabletJson.rowsets.size() == rowsetNum + 1
        def rowset = tabletJson.rowsets.get(tabletJson.rowsets.size() - 1)
        logger.info("rowset: ${rowset}")
        int start_index = rowset.indexOf("]")
        int end_index = rowset.indexOf("DATA")
        def segmentNumStr = rowset.substring(start_index + 1, end_index).trim()
        logger.info("segmentNumStr: ${segmentNumStr}")
        assert lastRowsetSegmentNum == Integer.parseInt(segmentNumStr)
    }

    def loadMultiSegmentData = { tableName, rows, succ, String err="" ->
        // load data that will have multi segments and there are duplicate keys between segments
        String content = ""
        (1..rows).each {
            content += "${it},${it},${it}\n"
        }
        content += content
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            inputStream new ByteArrayInputStream(content.getBytes())
            time 30000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                if (succ) {
                    assert "success" == json.Status.toLowerCase()
                    assert rows*2 == json.NumberTotalRows
                    assert 0 == json.NumberFilteredRows
                } else {
                    assert "fail" == json.Status.toLowerCase()
                    assert json.Message.contains(err)
                }
            }
        }
    }

    // to cause multi segments
    def customBeConfig = [
        doris_scanner_row_bytes : 1
    ]

    setBeConfigTemporary(customBeConfig) {
        try {
            // batch_size is 4164 in csv_reader.cpp
            // _batch_size is 8192 in vtablet_writer.cpp
            // to cause multi segments
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            // inject cache miss so that it will re-calculate delete bitmaps for all historical data in publish
            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")

            Thread.sleep(1000)

            loadMultiSegmentData(table1, 4096, true)

            GetDebugPoint().clearDebugPointsForAllBEs()
            Thread.sleep(2000)

            qt_sql "select count() from ${table1};"

            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"

            // ensure that we really write multi segments
            checkSegmentNum(4, 3)
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }

    // abnormal case, fail when calc between segments
    def table2 = "test_cloud_multi_segments_re_calc_in_publish_fail"
    sql "DROP TABLE IF EXISTS ${table2} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table2} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table2} values(99999,99999,99999);"
    sql "insert into ${table2} values(88888,88888,88888);"
    sql "insert into ${table2} values(77777,77777,77777);"
    sql "sync;"
    qt_sql "select * from ${table2} order by k1;"

    setBeConfigTemporary(customBeConfig) {
        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")

            // fail in calc_delete_bitmap_between_segments
            GetDebugPoint().enableDebugPointForAllBEs("_handle_rowset.inject.before.calc_between_segments")

            Thread.sleep(1000)

            loadMultiSegmentData(table2, 4096, false, "injected MemoryLimitExceeded error")

            GetDebugPoint().clearDebugPointsForAllBEs()
            Thread.sleep(2000)

            qt_sql "select count() from ${table2};"

            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table2} group by k1 having cnt > 1) A;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
