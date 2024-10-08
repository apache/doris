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

suite('nereids_delete_mow_partial_update') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            sql 'set enable_nereids_planner=true'
            sql 'set enable_fallback_to_original_planner=false'
            sql "set experimental_enable_nereids_planner=true;"
            sql 'set enable_nereids_dml=true'
            
            sql "sync"

            def tableName1 = "nereids_delete_mow_partial_update1"
            sql "DROP TABLE IF EXISTS ${tableName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                        `uid` BIGINT NULL,
                        `v1` BIGINT NULL 
                    )UNIQUE KEY(uid)
                DISTRIBUTED BY HASH(uid) BUCKETS 3
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "false",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            def tableName2 = "nereids_delete_mow_partial_update2"
            sql "DROP TABLE IF EXISTS ${tableName2};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
                        `uid` BIGINT NULL
                    ) UNIQUE KEY(uid)
                DISTRIBUTED BY HASH(uid) BUCKETS 3
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "false",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName1} values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5);"
            qt_sql "select * from ${tableName1} order by uid;"
            sql "insert into ${tableName2} values(1), (3);"
            explain {
                // delete from using command should use partial update
                sql "delete from ${tableName1} A using ${tableName2} B where A.uid=B.uid;"
                contains "IS_PARTIAL_UPDATE: true"
            }
            sql "delete from ${tableName1} A using ${tableName2} B where A.uid=B.uid;"
            qt_sql "select * from ${tableName1} order by uid;"
            // when using parital update insert stmt for delete stmt, it will use delete bitmap or delete sign rather than 
            // delete predicate to "delete" the rows
            sql "set skip_delete_predicate=true;"
            sql "sync"
            qt_sql_skip_delete_predicate "select * from ${tableName1} order by uid;"
            sql "set skip_delete_predicate=false;"
            sql "sync"

            explain {
                // delete from command should use partial update
                sql "delete from ${tableName1} where ${tableName1}.uid=2;"
                contains "IS_PARTIAL_UPDATE: true"
            }

            explain {
                // delete from command should use partial update
                sql "delete from ${tableName1} where ${tableName1}.v1=4;"
                contains "IS_PARTIAL_UPDATE: true"
            }

            sql "delete from ${tableName1} where ${tableName1}.v1=4;"
            qt_sql "select * from ${tableName1} order by uid;"

            sql "set skip_delete_sign=true;"
            sql "set skip_storage_engine_merge=true;"
            sql "set skip_delete_bitmap=true;"
            sql "sync"
            qt_sql "select uid, v1, __DORIS_DELETE_SIGN__ from ${tableName1} order by uid, v1, __DORIS_DELETE_SIGN__;"
            sql "drop table if exists ${tableName1};"
            sql "drop table if exists ${tableName2};"

            sql "set skip_delete_sign=false;"
            sql "set skip_storage_engine_merge=false;"
            sql "set skip_delete_bitmap=false;"
            sql "sync"
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
                    "disable_auto_compaction" = "true",
                    "enable_mow_light_delete" = "false",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName3} values(1, 1, 1, 1, 1), (2, 2, 2, 2, 2), (3, 3, 3, 3, 3), (4, 4, 4, 4, 4), (5, 5, 5, 5, 5);"
            qt_sql "select k1, c1, c2, c3, c4 from ${tableName3} order by k1, c1, c2, c3, c4;"
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
            qt_sql "select k1, c1, c2, c3, c4 from ${tableName3} order by k1, c1, c2, c3, c4;"
            sql "set skip_delete_sign=true;"
            sql "set skip_storage_engine_merge=true;"
            sql "set skip_delete_bitmap=true;"
            sql "sync"
            qt_sql "select k1, c1, c2, c3, c4, __DORIS_DELETE_SIGN__ from ${tableName3} order by k1, c1, c2, c3, c4, __DORIS_DELETE_SIGN__;"
            sql "drop table if exists ${tableName3};"
        }
    }
}
