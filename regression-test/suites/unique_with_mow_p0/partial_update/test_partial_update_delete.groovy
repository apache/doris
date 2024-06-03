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

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

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
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
            qt_sql "select * from ${tableName1} order by k1;"
            sql "insert into ${tableName2} values(1),(2),(3);"
            sql "delete from ${tableName1} A using ${tableName2} B where A.k1=B.k;"
            qt_sql "select * from ${tableName1} order by k1;"
            sql "set skip_delete_sign=true;"
            sql "set skip_storage_engine_merge=true;"
            sql "set skip_delete_bitmap=true;"
            qt_with_delete_sign "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName1} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
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
                    `c3` int,
                    `c4` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName3} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
            qt_sql "select k1,c1,c2,c3,c4 from ${tableName3} order by k1,c1,c2,c3,c4;"
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
            qt_sql "select k1,c1,c2,c3,c4 from ${tableName3} order by k1,c1,c2,c3,c4;"
            sql "set skip_delete_sign=true;"
            sql "set skip_storage_engine_merge=true;"
            sql "set skip_delete_bitmap=true;"
            qt_sql "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName3} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
            sql "drop table if exists ${tableName3};"
        }
    }
}
