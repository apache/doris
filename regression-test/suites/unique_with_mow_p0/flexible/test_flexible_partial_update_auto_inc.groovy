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

suite('test_flexible_partial_update_auto_inc') {

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        // 1. key col is auto-inc col
        def tableName = "test_flexible_partial_update_auto_inc_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` BIGINT NOT NULL AUTO_INCREMENT, 
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

        sql """insert into ${tableName} select (1+number)*1000, number, number, number, number, number from numbers("number" = "6"); """
        order_qt_autoinc_key_1 "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "autoinc1.json"
            time 20000
        }
        qt_autoinc_key_2 "select count(distinct k) from ${tableName};"
        // new rows' auto-inc col will be filled with values
        order_qt_autoinc_key_3 "select v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} where k<1000;"
        // old rows should be updated
        order_qt_autoinc_key_4 "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} where k>=1000;"

        // 2. value col is auto-inc col
        tableName = "test_flexible_partial_update_auto_inc2_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` BIGINT, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL AUTO_INCREMENT,
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
        order_qt_autoinc_val_1 "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
        // rows(2,3) specify v3, it will be updated
        // row(1) doesn't specify v3, it will use old values
        // insert new row(8,10), don't specify v3, it will be filled generated value
        // insert new row(9), specify v3
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "autoinc2.json"
            time 20000
        }
        qt_autoinc_val_2 "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} where k not in (8,10) order by k;"
        qt_autoinc_val_3 "select k,v1,v2,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} where k in (8,10) order by k;"
        qt_autoinc_val_4 "select count(distinct v3) from ${tableName} where k in (8,10);"
    }
}