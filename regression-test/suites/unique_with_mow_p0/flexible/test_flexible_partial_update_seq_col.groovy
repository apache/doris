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

suite('test_flexible_partial_update_seq_col') {

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        // 1.1. sequence map col(without default value)
        def tableName = "test_flexible_partial_update_seq_map_col1_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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

        sql """insert into ${tableName} select number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_map_no_default_val1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        // update rows(1,5) with lower seq map col
        // update rows(2,4) with higher seq map col
        // update rows(3) wihout seq map col, should use original seq map col
        // insert new row(6) without seq map col, should be filled with null
        // insert new row(7) with seq map col 
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test2.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_no_default_val2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


        // 1.2. sequence map col(with default value)
        tableName = "test_flexible_partial_update_seq_map_col2_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL DEFAULT "31"
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "function_column.sequence_col" = "v5",
        "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName} select number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_map_has_default_val1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // update rows(1,5) with lower seq map col
        // update rows(2,4) with higher seq map col
        // update rows(3) wihout seq map col, should use original seq map col
        // insert new row(6) without seq map col, should be filled with default value
        // insert new row(7) with seq map col 
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test2.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_has_default_val2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


        // 1.3. sequence type col
        tableName = "test_flexible_partial_update_seq_type_col1_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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
        "function_column.sequence_type" = "int",
        "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_type_col1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // update rows(1,5) with lower seq type col
        // update rows(2,4) with higher seq type col
        // update rows(3) wihout seq type col, should use original seq type col
        // insert new row(6) without seq map col, should be filled null value
        // insert new row(7) with seq map col
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test3.json"
            time 20000 // limit inflight 10s
        }
        // TODO(bobhan1): behavior here may be changed, maybe we need to force user to specify __DORIS_SEQUENCE_COL__ for tables
        // with sequence type col and discard rows without it in XXXReader
        order_qt_seq_type_col2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


        // ==============================================================================================================================
        // the below cases will have many rows with same keys in one load. Among rows with the same keys, some of them specify sequence col(sequence map col),
        // some of them don't. Those

        // 2.1. sequence type col
        tableName = "test_flexible_partial_update_seq_type_col2_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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
        "function_column.sequence_type" = "int",
        "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_type_col_multi_rows_1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // rows with same keys are neighbers
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test4.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_type_col_multi_rows_2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // rows with same keys are interleaved
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test5.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_type_col_multi_rows_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        // 2.2. sequence map col(without default value)
        tableName = "test_flexible_partial_update_seq_map_col3_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL,
        `v6` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "function_column.sequence_col" = "v6",
        "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,v6) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_map_col_no_default_val_multi_rows_1 "select k,v1,v2,v3,v4,v5,v6,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // 2.2.1 rows with same keys are neighbers
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test6.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_col_no_default_val_multi_rows_2 "select k,v1,v2,v3,v4,v5,v6,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // 2.2.2 rows with same keys are interleaved
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test7.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_col_no_default_val_multi_rows_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        // 2.3. sequence map col(with default value)
        tableName = "test_flexible_partial_update_seq_map_col4_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL,
        `v6` BIGINT NULL default "60"
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "function_column.sequence_col" = "v6",
        "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,v6) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
        order_qt_seq_map_col_has_default_val_multi_rows_1 "select k,v1,v2,v3,v4,v5,v6,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        // 2.3.1. rows with same keys are neighbers
        // after merge in memtable, newly inserted rows(key=6) will has two rows, one with sequence map col value=30
        // one without sequence map value. Because the default value of sequence map col(`v6`) is 60, larger than 30,
        // so the row with sequence map col will be deleted by the row without sequence map col
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test6.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_col_has_default_val_multi_rows_2 "select k,v1,v2,v3,v4,v5,v6,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // 2.3.2. rows with same keys are interleaved
        // after merge in memtable, newly inserted rows(key=7) will has two rows, one with sequence map col value=70
        // one without sequence map value. Because the default value of sequence map col(`v6`) is 60, smaller than 70,
        // so the row without sequence map col will be deleted by the row with sequence map col
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test7.json"
            time 20000 // limit inflight 10s
        }
        order_qt_seq_map_col_has_default_val_multi_rows_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        // ==============================================================================================================================
        // other cases

        // 3.1 sequence map with no default value
        tableName = "test_flexible_partial_update_seq_map_col5_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` BIGINT NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "c1");
            """
        sql "insert into ${tableName} values(1,256,1,1),(2,255,2,2),(3,3,3,3),(4,4,4,4);"
        qt_seq1 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "seq1.json"
            time 20000 // limit inflight 10s
        }
        qt_seq1 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k,c1,c2,c3,__DORIS_SEQUENCE_COL__;"
        sql "set skip_delete_bitmap=true;"
        sql "sync;"
        qt_seq1 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k,c1,c2,c3,__DORIS_SEQUENCE_COL__;"
        sql "set skip_delete_bitmap=false;"
        sql "sync;"

        // 3.2 sequence map with default value
        tableName = "test_flexible_partial_update_seq_map_col6_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` BIGINT NOT NULL,
            `c1` int default "256",
            `c2` int,
            `c3` int
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "c1");
            """
        sql "insert into ${tableName} values(1,256,1,1),(2,255,2,2),(3,3,3,3),(4,4,4,4);"
        qt_seq2 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "seq1.json"
            time 20000 // limit inflight 10s
        }
        qt_seq2 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k,c1,c2,c3,__DORIS_SEQUENCE_COL__;"
        sql "set skip_delete_bitmap=true;"
        sql "sync;"
        qt_seq2 "select k,c1,c2,c3,__DORIS_SEQUENCE_COL__ from ${tableName} order by k,c1,c2,c3,__DORIS_SEQUENCE_COL__;"
        sql "set skip_delete_bitmap=false;"
        sql "sync;"
    }
}