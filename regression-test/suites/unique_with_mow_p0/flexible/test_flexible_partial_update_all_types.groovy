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

suite('test_flexible_partial_update_all_types') {
    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        // 1. basic types
        def table1 = "test_f_basic_uniq_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${table1} force """
        sql """ CREATE TABLE ${table1} (
                k INT               NOT NULL,
                c1 BOOLEAN         NULL,
                c2 TINYINT         NULL,
                c3 SMALLINT        NULL,
                c4 INT             NULL,
                c5 BIGINT          NULL,
                c6 LARGEINT        NULL,
                c7 FLOAT           NULL,
                c8 DOUBLE          NULL,
                c9 DECIMAL(9,3)    NULL,
                c10 DATETIME        NULL,
                c11 DATE            NULL,
                c12 CHAR(12)        NULL,
                c13 VARCHAR(12)         NULL,
                c14 STRING          NULL,
                index idx_inverted_k(k) USING INVERTED,
                index idx_inverted_c4(c4) USING INVERTED,
                index idx_inverted_c10(c10) USING INVERTED,
                index idx_inverted_c12(c12) USING INVERTED,
                index idx_inverted_c13(c13) USING INVERTED,
                index idx_inverted_c14(c14) USING INVERTED
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        def table2 = "test_f_basic_agg"
        sql """ DROP TABLE IF EXISTS ${table2} force """
        sql """ CREATE TABLE ${table2} (
                k INT               NOT NULL,
                c1 BOOLEAN         replace_if_not_null null,
                c2 TINYINT         replace_if_not_null null,
                c3 SMALLINT        replace_if_not_null null,
                c4 INT             replace_if_not_null null,
                c5 BIGINT          replace_if_not_null null,
                c6 LARGEINT        replace_if_not_null null,
                c7 FLOAT           replace_if_not_null null,
                c8 DOUBLE          replace_if_not_null null,
                c9 DECIMAL(9,3)    replace_if_not_null null,
                c10 DATETIME        replace_if_not_null null,
                c11 DATE            replace_if_not_null null,
                c12 CHAR(12)        replace_if_not_null null,
                c13 VARCHAR         replace_if_not_null null,
                c14 STRING          replace_if_not_null null
            ) aggregate KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true"); """

        for (int i = 0; i < 10; i++) {
            def t1 = Thread.start {
                streamLoad {
                    table "${table1}"
                    set 'format', 'json'
                    set 'read_json_by_line', 'true'
                    set 'strict_mode', 'false'
                    set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                    file "all_types/basic_${i}.json"
                    time 20000
                }
            }
            def t2 = Thread.start {
                streamLoad {
                    table "${table2}"
                    set 'format', 'json'
                    set 'read_json_by_line', 'true'
                    set 'strict_mode', 'false'
                    file "all_types/basic_${i}.json"
                    time 20000
                }
            }
            t1.join()
            t2.join()
            def res_uniq = sql "select * from ${table1} order by k;"
            def res_agg = sql "select * from ${table2} order by k;"
            logger.info("\nres_uniq: ${res_uniq}\n res_agg:${res_agg}\n");
            assertEquals(res_uniq, res_agg)
        }
        qt_sql "select * from ${table1} order by k;"


        // 2. array types
        table1 = "test_f_array_uniq_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${table1} force """
        sql """ CREATE TABLE ${table1} (
                k INT               NOT NULL,
                c1 array<BOOLEAN>          null,
                c2 array<TINYINT>          null,
                c3 array<SMALLINT>         null,
                c4 array<INT>              null,
                c5 array<BIGINT>           null,
                c6 array<LARGEINT>         null,
                c7 array<FLOAT>            null,
                c8 array<DOUBLE>           null,
                c9 array<DECIMAL(9,3)>     null,
                c10 array<DATETIME>         null,
                c11 array<DATE>             null,
                c12 array<CHAR(12)>         null,
                c13 array<VARCHAR(12)>          null,
                c14 array<STRING>           null,
                index idx_inverted_k(k) USING INVERTED,
                index idx_inverted_c4(c4) USING INVERTED,
                index idx_inverted_c10(c10) USING INVERTED,
                index idx_inverted_c12(c12) USING INVERTED,
                index idx_inverted_c13(c13) USING INVERTED,
                index idx_inverted_c14(c14) USING INVERTED
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """
        
        table2 = "test_f_array_agg"
        sql """ DROP TABLE IF EXISTS ${table2} force """
        sql """ CREATE TABLE ${table2} (
                k INT               NOT NULL,
                c1 array<BOOLEAN>         replace_if_not_null null,
                c2 array<TINYINT>         replace_if_not_null null,
                c3 array<SMALLINT>        replace_if_not_null null,
                c4 array<INT>             replace_if_not_null null,
                c5 array<BIGINT>          replace_if_not_null null,
                c6 array<LARGEINT>        replace_if_not_null null,
                c7 array<FLOAT>           replace_if_not_null null,
                c8 array<DOUBLE>          replace_if_not_null null,
                c9 array<DECIMAL(9,3)>    replace_if_not_null null,
                c10 array<DATETIME>        replace_if_not_null null,
                c11 array<DATE>            replace_if_not_null null,
                c12 array<CHAR(12)>        replace_if_not_null null,
                c13 array<VARCHAR(12)>         replace_if_not_null null,
                c14 array<STRING>          replace_if_not_null null
            ) aggregate KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 4
            PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true"); """

        for (int i = 0; i < 8; i++) {
            def t1 = Thread.start {
                streamLoad {
                    table "${table1}"
                    set 'format', 'json'
                    set 'read_json_by_line', 'true'
                    set 'strict_mode', 'false'
                    set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                    file "all_types/array_${i}.json"
                    time 20000
                }
            }
            def t2 = Thread.start {
                streamLoad {
                    table "${table2}"
                    set 'format', 'json'
                    set 'read_json_by_line', 'true'
                    set 'strict_mode', 'false'
                    file "all_types/array_${i}.json"
                    time 20000
                }
            }
            t1.join()
            t2.join()
            def res_uniq = sql "select * from ${table1} order by k;"
            def res_agg = sql "select * from ${table2} order by k;"
            logger.info("\nres_uniq: ${res_uniq}\n res_agg:${res_agg}\n");
            assertEquals(res_uniq, res_agg)
        }
        qt_sql "select * from ${table1} order by k;"
    }
}