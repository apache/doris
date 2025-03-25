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

suite('test_flexible_partial_update_downgrade_base_data', 'p0,restart_fe') {
    String db = context.config.getDbNameByFile(context.file)
    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        def tableName = "test_f_downgrade_${use_row_store}"
        
        sql "use ${db};"
        def tbls = sql "show tables;"
        boolean shouldSkip = false
        for (def tbl : tbls) {
            if (tbl[0] == tableName) {
                logger.info("skip to create table ${tableName};")
                shouldSkip = true
                break;
            }
        }
        if (shouldSkip) {
            continue
        }
        
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
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test1.json"
            time 20000
        }
    }
}