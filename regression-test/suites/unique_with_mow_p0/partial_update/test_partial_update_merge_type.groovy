
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

suite("test_partial_update_merge_type", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            def tableName = "test_partial_update_merge_type"
            // // 1. merge_type=MERGE, no sequence col
            // sql """ DROP TABLE IF EXISTS ${tableName} """
            // sql """ CREATE TABLE ${tableName} (
            //         `k` BIGINT NOT NULL,
            //         `c1` int,
            //         `c2` int,
            //         `c3` int)
            //         UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            //         PROPERTIES(
            //             "replication_num" = "1",
            //             "enable_unique_key_merge_on_write" = "true",
            //             "store_row_column" = "${use_row_store}"); """
            // sql """insert into ${tableName} select number,number,number,number from numbers("number"="5");"""
            // sql "sync"
            // qt_sql """select * from ${tableName} order by k;"""
            // streamLoad {
            //     table "${tableName}"
            //     set 'column_separator', ','
            //     set 'format', 'csv'
            //     set 'columns', 'k,c2,del'
            //     set 'partial_columns', 'true'
            //     set 'merge_type', 'MERGE'
            //     set 'delete', 'del=1'
            //     file 'merge1.csv'
            //     time 10000
            // }
            // qt_sql """select * from ${tableName} order by k;"""

            // // 2, merge_type=DELETE, no sequence col
            // streamLoad {
            //     table "${tableName}"
            //     set 'column_separator', ','
            //     set 'format', 'csv'
            //     set 'columns', 'k,c1'
            //     set 'partial_columns', 'true'
            //     set 'merge_type', 'DELETE'
            //     file 'merge2.csv'
            //     time 10000
            // }
            // qt_sql """select * from ${tableName} order by k;"""


            // 3. merge_type=MERGE, has sequence type col
            tableName = "test_partial_update_merge_type2"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                    `k` BIGINT NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int)
                    UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "function_column.sequence_type" = "BIGINT",
                        "store_row_column" = "${use_row_store}"); """
            sql """insert into ${tableName}(k,c1,c2,c3,__DORIS_SEQUENCE_COL__) select number,number,number,number,1 from numbers("number"="5");"""
            qt_sql """select * from ${tableName} order by k;"""
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,c2,seq,del'
                set 'partial_columns', 'true'
                set 'function_column.sequence_col', 'seq'
                set 'merge_type', 'MERGE'
                set 'delete', 'del=1'
                file 'merge3.csv'
                time 10000
            }
            qt_sql """select * from ${tableName} order by k;"""

            // 4. merge_type=DELETE, has sequence type col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,c2,seq'
                set 'partial_columns', 'true'
                set 'function_column.sequence_col', 'seq'
                set 'merge_type', 'DELETE'
                file 'merge3.csv'
                time 10000
            }
            qt_sql """select * from ${tableName} order by k;"""

            sql """ DROP TABLE IF EXISTS ${tableName}; """
        }
    }
}
