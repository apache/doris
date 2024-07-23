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

suite("test_partial_update_after_delete", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"
            sql "SET enable_nereids_planner=true;"
            sql "SET enable_fallback_to_original_planner=false;"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            def tableName1 = "test_partial_update_after_delete1"
            sql "DROP TABLE IF EXISTS ${tableName1};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                    `k1` INT NULL,
                    `v1` INT NULL,
                    `v2` INT NULL
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """
            
            sql "insert into ${tableName1} values(1,1,1);"
            sql "delete from ${tableName1} where k1=1;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName1}(k1, v1) values(1,2);"
            qt_select1 "select * from ${tableName1};"

            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict=true;"
            sql "SET enable_nereids_planner=false;"
            sql "SET enable_fallback_to_original_planner=false;"
            def tableName2 = "test_partial_update_after_delete2"
            sql "DROP TABLE IF EXISTS ${tableName2};"
            sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
                    `k1` INT NULL,
                    `v1` INT NULL,
                    `v2` INT NULL
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "store_row_column" = "${use_row_store}"); """
            
            sql "insert into ${tableName2} values(1,1,1);"
            sql "delete from ${tableName2} where k1=1;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "insert into ${tableName2}(k1, v1) values(1,2);"
            qt_select2 "select * from ${tableName2};"
        }
    }
}
