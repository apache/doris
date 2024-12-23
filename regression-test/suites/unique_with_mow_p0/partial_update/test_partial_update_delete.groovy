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

suite('test_partial_update_delete') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_nereids : [true, false]) {
        for (def use_row_store : [false, true]) {
            logger.info("current params: use_nereids: ${use_nereids}, use_row_store: ${use_row_store}")
            connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                sql "use ${db};"
                if (use_nereids) {
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                } else {
                    sql "set enable_nereids_planner=false"
                }
                sql "sync;"

                def tableName1 = "test_partial_update_delete1"
                sql "DROP TABLE IF EXISTS ${tableName1};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int NOT NULL,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_mow_light_delete" = "false",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """


                def tableName2 = "test_partial_update_delete2"
                sql "DROP TABLE IF EXISTS ${tableName2};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
                            `k` BIGINT NULL
                        ) UNIQUE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_mow_light_delete" = "false",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """

                sql "insert into ${tableName1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
                qt_sql1 "select * from ${tableName1} order by k1;"
                sql "insert into ${tableName2} values(1),(3);"
                sql "delete from ${tableName1} A using ${tableName2} B where A.k1=B.k;"
                qt_sql1 "select * from ${tableName1} order by k1;"

                sql "delete from ${tableName1} where c2=2;"
                qt_sql1 "select * from ${tableName1} order by k1;"

                sql "set skip_delete_sign=true;"
                sql "set skip_storage_engine_merge=true;"
                sql "set skip_delete_bitmap=true;"
                qt_with_delete_sign1 "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName1} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
                sql "drop table if exists ${tableName1};"
                sql "drop table if exists ${tableName2};"


                sql "set skip_delete_sign=false;"
                sql "set skip_storage_engine_merge=false;"
                sql "set skip_delete_bitmap=false;"
                def tableName3 = "test_partial_update_delete3"
                sql "DROP TABLE IF EXISTS ${tableName3};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableName3} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int NOT NULL,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_mow_light_delete" = "false",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"); """

                sql "insert into ${tableName3} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5),(6,6,6,6,6);"
                qt_sql2 "select k1,c1,c2,c3,c4 from ${tableName3} order by k1,c1,c2,c3,c4;"
                streamLoad {
                    table "${tableName3}"
                    set 'column_separator', ','
                    set 'format', 'csv'
                    set 'columns', 'k1'
                    set 'partial_columns', 'true'
                    set 'merge_type', 'DELETE'
                    file 'partial_update_delete.csv'
                    time 10000
                }
                sql "sync"
                qt_sql2 "select k1,c1,c2,c3,c4 from ${tableName3} order by k1,c1,c2,c3,c4;"

                sql "set enable_insert_strict=false;"
                sql "set enable_unique_key_partial_update=true;"
                sql "sync;"
                sql "insert into ${tableName3}(k1, __DORIS_DELETE_SIGN__) values(8,1),(4,1),(9,1);"
                qt_sql2 "select k1,c1,c2,c3,c4 from ${tableName3} order by k1,c1,c2,c3,c4;"
                sql "set enable_insert_strict=true;"
                sql "set enable_unique_key_partial_update=false;"
                sql "sync;"

                sql "set skip_delete_sign=true;"
                sql "set skip_storage_engine_merge=true;"
                sql "set skip_delete_bitmap=true;"
                qt_sql2 "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName3} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
                sql "drop table if exists ${tableName3};"


                sql "set skip_delete_sign=false;"
                sql "set skip_storage_engine_merge=false;"
                sql "set skip_delete_bitmap=false;"
                def tableName4 = "test_partial_update_delete4"
                sql "DROP TABLE IF EXISTS ${tableName4};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableName4} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int NOT NULL,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_mow_light_delete" = "false",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}",
                        "function_column.sequence_col" = "c3"); """

                sql "insert into ${tableName4} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5),(6,6,6,6,6);"
                qt_sql3 "select k1,c1,c2,c3,c4 from ${tableName4} order by k1,c1,c2,c3,c4;"
                // if the table has sequence map col, can not set sequence map col when merge_type=delete
                streamLoad {
                    table "${tableName4}"
                    set 'column_separator', ','
                    set 'format', 'csv'
                    set 'columns', 'k1'
                    set 'partial_columns', 'true'
                    set 'merge_type', 'DELETE'
                    file 'partial_update_delete.csv'
                    time 10000
                }
                sql "sync"
                qt_sql3 "select k1,c1,c2,c3,c4 from ${tableName4} order by k1,c1,c2,c3,c4;"

                sql "set enable_insert_strict=false;"
                sql "set enable_unique_key_partial_update=true;"
                sql "sync;"
                sql "insert into ${tableName4}(k1, __DORIS_DELETE_SIGN__) values(8,1),(4,1),(9,1);"
                qt_sql3 "select k1,c1,c2,c3,c4 from ${tableName4} order by k1,c1,c2,c3,c4;"
                sql "set enable_insert_strict=true;"
                sql "set enable_unique_key_partial_update=false;"
                sql "sync;"

                sql "set skip_delete_sign=true;"
                sql "set skip_storage_engine_merge=true;"
                sql "set skip_delete_bitmap=true;"
                qt_sql3 "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__ from ${tableName4} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
                sql "drop table if exists ${tableName4};"


                sql "set skip_delete_sign=false;"
                sql "set skip_storage_engine_merge=false;"
                sql "set skip_delete_bitmap=false;"
                def tableName5 = "test_partial_update_delete5"
                sql "DROP TABLE IF EXISTS ${tableName5};"
                sql """ CREATE TABLE IF NOT EXISTS ${tableName5} (
                        `k1` int NOT NULL,
                        `c1` int,
                        `c2` int,
                        `c3` int NOT NULL,
                        `c4` int
                        )UNIQUE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES (
                        "enable_unique_key_merge_on_write" = "true",
                        "enable_mow_light_delete" = "false",
                        "disable_auto_compaction" = "true",
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}",
                        "function_column.sequence_type" = "int"); """
                sql "insert into ${tableName5}(k1,c1,c2,c3,c4,__DORIS_SEQUENCE_COL__) values(1,1,1,1,1,1),(2,2,2,2,2,2),(3,3,3,3,3,3),(4,4,4,4,4,4),(5,5,5,5,5,5),(6,6,6,6,6,6);"
                qt_sql4 "select k1,c1,c2,c3,c4 from ${tableName5} order by k1,c1,c2,c3,c4;"
                // if the table has sequence type col, users must set sequence col even if merge_type=delete
                streamLoad {
                    table "${tableName5}"
                    set 'column_separator', ','
                    set 'format', 'csv'
                    set 'columns', 'k1'
                    set 'partial_columns', 'true'
                    set 'merge_type', 'DELETE'
                    file 'partial_update_delete.csv'
                    time 10000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("Stream load result: ${result}".toString())
                        def json = parseJson(result)
                        assertEquals("fail", json.Status.toLowerCase())
                        assertTrue(json.Message.contains('need to specify the sequence column'))
                    }
                }
                sql "drop table if exists ${tableName5};"
            }
        }
    }
}
