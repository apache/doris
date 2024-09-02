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

suite("test_partial_update_seq_map_col", "p0") {
    for (def use_nereids : [true, false]) {
        for (def use_row_store : [false, true]) {
            logger.info("current params: use_nereids: ${use_nereids}, use_row_store: ${use_row_store}")
            if (use_nereids) {
                sql """ set enable_nereids_planner=true; """
                sql """ set enable_fallback_to_original_planner=false; """
            } else {
                sql """ set enable_nereids_planner = false; """
            }
            sql "set enable_insert_strict=false;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set show_hidden_columns=true;"
            sql "sync;"

            def tableName = "test_partial_update_seq_map_col1"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` datetime(6) null default current_timestamp(6),
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "c2",
                "store_row_column" = "${use_row_store}"); """ 
            sql "insert into ${tableName}(k,c1) values(1,1);"
            sql "insert into ${tableName}(k,c1) values(2,2);"
            sql "insert into ${tableName}(k,c1) values(3,3);"
            sql "insert into ${tableName}(k,c1) values(4,4);"
            qt_sql1 "select k,c1 from ${tableName} where c2=__DORIS_SEQUENCE_COL__;"


            tableName = "test_partial_update_seq_map_col2"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` datetime not null default current_timestamp,
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "c2",
                "store_row_column" = "${use_row_store}"); """ 
            sql "insert into ${tableName}(k,c1) values(1,1);"
            sql "insert into ${tableName}(k,c1) values(2,2);"
            sql "insert into ${tableName}(k,c1) values(3,3);"
            sql "insert into ${tableName}(k,c1) values(4,4);"
            qt_sql2 "select k,c1 from ${tableName} where c2=__DORIS_SEQUENCE_COL__;"


            tableName = "test_partial_update_seq_map_col3"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int not null default "999",
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "c2",
                "store_row_column" = "${use_row_store}"); """ 
            sql "insert into ${tableName}(k,c1) values(1,1);"
            sql "insert into ${tableName}(k,c1) values(2,2);"
            sql "insert into ${tableName}(k,c1) values(3,3);"
            sql "insert into ${tableName}(k,c1) values(4,4);"
            qt_sql3 "select k,c1,c2,__DORIS_SEQUENCE_COL__ from ${tableName} where c2=__DORIS_SEQUENCE_COL__;"


            tableName = "test_partial_update_seq_map_col4"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int null,
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "c2",
                "store_row_column" = "${use_row_store}"); """ 
            sql "insert into ${tableName}(k,c1) values(1,1);"
            sql "insert into ${tableName}(k,c1) values(2,2);"
            sql "insert into ${tableName}(k,c1) values(3,3);"
            sql "insert into ${tableName}(k,c1) values(4,4);"
            qt_sql4 "select k,c1,c2,__DORIS_SEQUENCE_COL__ from ${tableName};"


            tableName = "test_partial_update_seq_map_col5"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int not null
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "c2",
                "store_row_column" = "${use_row_store}"); """ 
            test {
                sql "insert into ${tableName}(k,c1) values(1,1);"
                exception "the unmentioned column `c2` should have default value or be nullable for newly inserted rows in non-strict mode partial update"
            }
        }
    }
}
