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

suite("test_mow_alter_seq_multi_segments", "nonConcurrent") {
    def table1 = "test_mow_alter_seq_multi_segments"
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

    sql """insert into ${table1} values(1,1,1);"""
    qt_sql "select * from ${table1} order by k1;"
    sql """alter table ${table1} ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type"="int");"""

    // to cause multi segments and segment compaction
    def customBeConfig = [
        doris_scanner_row_bytes : 1
    ]

    setBeConfigTemporary(customBeConfig) {
        try {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
            // batch_size is 4164 in csv_reader.cpp
            // _batch_size is 8192 in vtablet_writer.cpp
            // to cause multi segments
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
            
            Thread.sleep(1000)

            int rows = 4064
            // load data that will have multi segments and there are duplicate keys between segments
            String content = ""
            (1..rows).each {
                int x = it
                content += "${x},${x},${x},1\n"
            }
            (1..rows).each {
                int x = it
                content += "${x},${x},${x},2\n"
            }
            def t1 = Thread.start {
                streamLoad {
                    table "${table1}"
                    set 'column_separator', ','
                    set 'columns', 'k1,c1,c2,seq'
                    set 'function_column.sequence_col', 'seq'
                    inputStream new ByteArrayInputStream(content.getBytes())
                    time 30000

                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        def json = parseJson(result)
                        assert "success" == json.Status.toLowerCase()
                    }
                }
            }
            t1.join()
            qt_sql "select count() from ${table1};"
            // qt_sql "select *,__DORIS_VERSION_COL__ as ver, __DORIS_DELETE_SIGN__ as del,__DORIS_SEQUENCE_COL__ as seq from ${table1} where k1<=10 order by k1,__DORIS_VERSION_COL__;"
            sql "set disable_nereids_rules='ELIMINATE_GROUP_BY';"
            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
