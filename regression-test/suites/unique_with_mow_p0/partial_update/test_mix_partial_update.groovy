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

suite('test_mix_partial_update') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"

            def tableInsertName1 = "test_mix_partial_update"
            sql "DROP TABLE IF EXISTS ${tableInsertName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableInsertName1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `seq` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "function_column.sequence_col" = "seq",
                    "store_row_column" = "${use_row_store}"); """
            sql "insert into ${tableInsertName1} values(1,1,1,1,1),(1,1,1,1,10),(2,2,2,2,2),(3,3,3,3,3)"
            // 1,1,1,1,10
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select1 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,10,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select2 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,20,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select3 "select * from ${tableInsertName1} order by k1"
            sql "insert into ${tableInsertName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,1,0)"
            // error
            // 1,10,null,10,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select4 "select * from ${tableInsertName1} order by k1"
            sql "update ${tableInsertName1} set seq = 30 where k1 = 1"
            // 1,10,null,30,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select5 "select * from ${tableInsertName1} order by k1"

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"

            sql "insert into ${tableInsertName1} (k1,seq) values(2,1)"
            // seq的更新没生效
            // error
            // 1,10,null,30,1
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select6 "select * from ${tableInsertName1} order by k1"

            sql "insert into ${tableInsertName1} (k1,seq,__DORIS_DELETE_SIGN__) values(3,1,1)"
            // 部分列更新能更新delete sign？
            // error
            // 1,10,null,30,1
            // 2,2,2,2,1
            // 3,3,3,3,3
            qt_select7 "select * from ${tableInsertName1} order by k1"

            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            sql "insert into ${tableInsertName1} values(4,4,4,4,4)"
            // 1,10,null,30,1
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,4
            qt_select8 "select * from ${tableInsertName1} order by k1"

            sql "update ${tableInsertName1} set seq = 1 where k1 = 4"
            // update没有更新
            // error
            // 1,10,null,30,1
            // 2,2,2,2,1
            // 3,3,3,3,3
            // 4,4,4,4,1
            qt_select9 "select * from ${tableInsertName1} order by k1"

            def tableStreamName1 = "test_mix_partial_update"
            sql "DROP TABLE IF EXISTS ${tableStreamName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableStreamName1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `seq` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "function_column.sequence_col" = "seq",
                    "store_row_column" = "${use_row_store}"); """
            //sql "insert into ${tableStreamName1} values(1,1,1,1,1),(1,1,1,1,10),(2,2,2,2,2),(3,3,3,3,3)"
            // 1,1,1,1,10
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select1 "select * from ${tableStreamName1} order by k1"
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,10,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select2 "select * from ${tableStreamName1} order by k1"
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,20,1)"
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select3 "select * from ${tableStreamName1} order by k1"
            //sql "insert into ${tableStreamName1} (k1,c1,c2,c3,seq,__DORIS_DELETE_SIGN__) values(1,10,null,10,1,0)"
            // 1,10,null,10,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select4 "select * from ${tableStreamName1} order by k1"
            //sql "update ${tableStreamName1} set seq = 30"
            // 1,10,null,30,1
            // 2,2,2,2,2
            // 3,3,3,3,3
            qt_select5 "select * from ${tableStreamName1} order by k1"
        }
    }
}