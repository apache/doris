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

suite("test_skip_calc_between_segments", "nonConcurrent") {

    def table1 = "test_skip_calc_between_segments"
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

    def block_sc = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
        }
    }
    
    def unblock_sc = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
        }
    }

    def block_publish = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
            
        }
    }

    def unblock_publish = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
            
        }
    }

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

            block_publish()

            // block sc to let load skip to calculate delete bitmap in flush and commit phase
            block_sc()

            sql "alter table ${table1} modify column c1 varchar(100);"

            Thread.sleep(3000)

            def t1 = Thread.start {
                // load data that will have multi segments and there are duplicate keys between segments
                String content = ""
                (1..4096).each {
                    content += "${it},${it},${it}\n"
                }
                content += content
                streamLoad {
                    table "${table1}"
                    set 'column_separator', ','
                    inputStream new ByteArrayInputStream(content.getBytes())
                    time 30000 // limit inflight 10s

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
            }


            Thread.sleep(2000)

            // let sc finish and wait for tablet state to be RUNNING
            unblock_sc()
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                time 1000
            }
            logger.info("wait for schema change done")

            Thread.sleep(500)

            unblock_publish()

            t1.join()

            sql "set disable_nereids_rules='ELIMINATE_GROUP_BY';"
            qt_sql "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"

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
}
