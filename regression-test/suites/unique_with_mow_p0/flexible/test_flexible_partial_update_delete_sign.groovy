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

suite('test_flexible_partial_update_delete_sign') {

    def inspect_rows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

    for (def use_row_store : [false]) {
        // logger.info("current params: use_row_store: ${use_row_store}")

        // 1. table without sequence col
        def tableName = "test_flexible_partial_update_delete_sign_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
        order_qt_no_seq_col_1 "select k,v1,v2,v3,v4,v5 from ${tableName};"

        // update rows(2,4,5), delete rows(1,3), insert new rows(6), delete new rows(7)
        // should not fail although rows(1,3,7) doesn't specify v3(which is not nullable and has no default value)
        // because they have delete sign marked
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete1.json"
            time 20000
        }
        order_qt_no_seq_col_2 "select k,v1,v2,v3,v4,v5 from ${tableName};"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        // delete rows(1,7) which have been deleted by delete sign before
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete2.json"
            time 20000
        }
        order_qt_no_seq_col_3 "select k,v1,v2,v3,v4,v5 from ${tableName};"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"


        // 2. table with sequence map col
        tableName = "test_flexible_partial_update_delete_sign2_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "v5",
            "store_row_column" = "${use_row_store}"); """

        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
        order_qt_seq_map_col_1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName};"
        // update rows(2,4,5), delete rows(1,3), insert new rows(6), delete new rows(7)
        // __DORIS_SEQUENCE_COL__ should be filled from old rows for rows(1,3)
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete1.json"
            time 20000
        }
        order_qt_seq_map_col_2 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__ from ${tableName};"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        // ==============================================================================================================================
        // 3. test insert after delete in one load
        // 3.1 without seqeunce column
        tableName = "test_flexible_partial_update_delete_sign3_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "10"); """
        qt_insert_after_delete_3_1 "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete3.json"
            time 20000
        }
        qt_insert_after_delete_3_1 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        tableName = "test_flexible_partial_update_delete_sign4_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "10"); """
        qt_insert_after_delete_3_1 "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete4.json"
            time 20000
        }
        qt_insert_after_delete_3_1 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        // 3.2 with sequence type column
        tableName = "test_flexible_partial_update_delete_sign5_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL DEFAULT "5432",
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL DEFAULT "9753"
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_type" = "int",
            "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, null from numbers("number" = "15"); """
        qt_insert_after_delete_3_2_1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        // rows(1,2,3,4,5,6) are all without sequence column
        // rows(7,8,9,10,11,12) are all with sequence column, seq col value is increasing
        // row(1,7): delete + insert
        // row(2,8): insert + delete
        // row(3,9): delete + insert + delete
        // row(4,10): insert + delete + insert
        // row(5,11): delete + insert + delete + insert
        // row(6,12): insert + delete + insert + delete
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete5.json"
            time 20000
        }
        qt_insert_after_delete_3_2_2 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"
        
        sql "truncate table ${tableName};"
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, null from numbers("number" = "15"); """
        qt_insert_after_delete_3_2_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        // rows(7,8,9,10,11,12) same as above, insert some rows with lower seq value
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete6.json"
            time 20000
        }
        qt_insert_after_delete_3_2_3 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        sql "truncate table ${tableName};"
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, number*10 from numbers("number" = "13"); """
        qt_insert_after_delete_3_2_4 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete7.json"
            time 20000
        }
        qt_insert_after_delete_3_2_4 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        // 3.3 with sequence map col (no default value)
        tableName = "test_flexible_partial_update_delete_sign6_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL DEFAULT "5432",
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL DEFAULT "9753"
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "v1",
            "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5) select number, null, number, number, number, number from numbers("number" = "15"); """
        qt_insert_after_delete_3_3_1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        // rows(1,2,3,4,5,6) are all without sequence column
        // rows(7,8,9,10,11,12) are all with sequence column, seq col value is increasing
        // row(1,7): delete + insert
        // row(2,8): insert + delete
        // row(3,9): delete + insert + delete
        // row(4,10): insert + delete + insert
        // row(5,11): delete + insert + delete + insert
        // row(6,12): insert + delete + insert + delete
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete8.json"
            time 20000
        }
        qt_insert_after_delete_3_3_2 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"
        
        sql "truncate table ${tableName};"
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5) select number, null, number, number, number, number from numbers("number" = "15"); """
        qt_insert_after_delete_3_3_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        // rows(7,8,9,10,11,12) same as above, insert some rows with lower seq value
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete9.json"
            time 20000
        }
        qt_insert_after_delete_3_3_4 "select k,v1,v2,v3,v4,v5,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"

        sql "truncate table ${tableName};"
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5) select number, number*10, number, number, number, number from numbers("number" = "13"); """
        qt_insert_after_delete_3_3_5 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete10.json"
            time 20000
        }
        qt_insert_after_delete_3_3_6 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"


        // 3.4 with seq map col (has default value)
        tableName = "test_flexible_partial_update_delete_sign6_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL default "30",
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL DEFAULT "5432",
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL DEFAULT "9753"
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "v1",
            "store_row_column" = "${use_row_store}"); """
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "delete11.json"
            time 20000
        }
        qt_insert_after_delete_3_4 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__,v1,v2,v3,v4,v5;"
    }
}