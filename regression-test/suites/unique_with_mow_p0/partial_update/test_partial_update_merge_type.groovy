
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

    def inspect_rows = { sqlStr ->
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

            def tableName = "test_partial_update_merge_type"
            sql """ DROP TABLE IF EXISTS ${tableName} force"""
            sql """ CREATE TABLE ${tableName} (
                    `k` BIGINT NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int)
                    UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """
            sql """insert into ${tableName} select number,number,number,number from numbers("number"="9");"""
            sql "sync"
            qt_sql """select * from ${tableName} order by k;"""
            // 1.1 merge_type=MERGE, no sequence col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,c2,del'
                set 'partial_columns', 'true'
                set 'merge_type', 'MERGE'
                set 'delete', 'del=1'
                file 'merge1.csv'
                time 10000
            }
            sql "sync"
            qt_sql_1_1 """select * from ${tableName} order by k;"""
            // 1.2 merge_type=MERGE, no sequence col, no value col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,del'
                set 'partial_columns', 'true'
                set 'merge_type', 'MERGE'
                set 'delete', 'del=1'
                file 'merge5.csv'
                time 10000
            }
            sql "sync"
            qt_sql_1_2 """select * from ${tableName} order by k;"""
            // 2.1 merge_type=DELETE, no sequence col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,c1'
                set 'partial_columns', 'true'
                set 'merge_type', 'DELETE'
                file 'merge2.csv'
                time 10000
            }
            qt_sql_2_1 """select * from ${tableName} order by k;"""
            // 2.2 merge_type=DELETE, no sequence col, no value col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k'
                set 'partial_columns', 'true'
                set 'merge_type', 'DELETE'
                file 'merge6.csv'
                time 10000
            }
            sql "sync"
            qt_sql_2_2 """select * from ${tableName} order by k;"""


            tableName = "test_partial_update_merge_type2"
            sql """ DROP TABLE IF EXISTS ${tableName} force"""
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
            sql """insert into ${tableName}(k,c1,c2,c3,__DORIS_SEQUENCE_COL__) select number,number,number,number,1 from numbers("number"="9");"""
            qt_sql """select * from ${tableName} order by k;"""
            // 3.1 merge_type=MERGE, has sequence type col
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
            sql "sync"
            qt_sql_3_1 """select * from ${tableName} order by k;"""
            inspect_rows """select k,c1,c2,c3,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_SEQUENCE_COL__;"""
            // 3.2 merge_type=MERGE, has sequence type col, no value col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,seq,del'
                set 'partial_columns', 'true'
                set 'function_column.sequence_col', 'seq'
                set 'merge_type', 'MERGE'
                set 'delete', 'del=1'
                file 'merge7.csv'
                time 10000
            }
            sql "sync"
            qt_sql_3_2 """select * from ${tableName} order by k;"""
            inspect_rows """select k,c1,c2,c3,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_SEQUENCE_COL__;"""

            // 4.1 merge_type=DELETE, has sequence type col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,c2,seq'
                set 'partial_columns', 'true'
                set 'function_column.sequence_col', 'seq'
                set 'merge_type', 'DELETE'
                file 'merge4.csv'
                time 10000
            }
            sql "sync"
            qt_sql_4_1 """select * from ${tableName} order by k;"""
            inspect_rows """select k,c1,c2,c3,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_SEQUENCE_COL__;"""
            // 4.2 merge_type=DELETE, has sequence type col, no value col
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'k,seq'
                set 'partial_columns', 'true'
                set 'function_column.sequence_col', 'seq'
                set 'merge_type', 'DELETE'
                file 'merge8.csv'
                time 10000
            }
            sql "sync"
            qt_sql_4_2 """select * from ${tableName} order by k;"""
            inspect_rows """select k,c1,c2,c3,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_SEQUENCE_COL__;"""

            sql """ DROP TABLE IF EXISTS ${tableName}; """
        }
    }
}
