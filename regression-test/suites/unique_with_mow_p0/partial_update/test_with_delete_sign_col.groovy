
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

suite("test_with_delete_sign_col", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    def inspectRows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }


    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"
            def table1 = "test_with_delete_sign_col"
            sql "DROP TABLE IF EXISTS ${table1} FORCE;"
            sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int default "999", 
                    `c3` int default "888",
                    `c4` int default "777"
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "enable_mow_light_delete" = "false",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1"); """

            sql """insert into ${table1} select number,number,number,number,number from numbers("number"="11");"""
            qt_sql_1 "select * from ${table1} order by k1;"

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"

            sql "insert into ${table1}(k1,c3,c4,__DORIS_DELETE_SIGN__) values(2,22,22,1),(3,33,33,1),(5,55,55,0),(6,66,66,1),(7,77,77,1),(10,1010,1010,0);"
            qt_sql_1 "select * from ${table1} order by k1;"

            sql "insert into ${table1}(k1,c1,c2,__DORIS_DELETE_SIGN__) values(1,11,11,0),(2,22,22,0),(3,33,33,0),(6,66,66,0),(9,99,99,1);"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            sql "sync;"
            qt_sql_1 "select * from ${table1} order by k1;"


            sql "truncate table ${table1};"
            sql """insert into ${table1} select number,number,number,number,number from numbers("number"="11");"""
            qt_sql_2 "select * from ${table1} order by k1;"

            streamLoad {
                table "${table1}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k1,c3,c4,del'
                set 'partial_columns', 'true'
                set 'merge_type', 'MERGE'
                set 'delete', 'del=1'
                file 'with_delete1.csv'
                time 10000
            }
            qt_sql_2 "select * from ${table1} order by k1;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2,__DORIS_DELETE_SIGN__) values(1,11,11,0),(2,22,22,0),(3,33,33,0),(6,66,66,0),(9,99,99,1);"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            sql "sync;"
            qt_sql_1 "select * from ${table1} order by k1;"
        }
    }
}
