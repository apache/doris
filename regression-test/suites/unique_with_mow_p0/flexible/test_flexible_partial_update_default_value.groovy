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

suite('test_flexible_partial_update_default_value') {

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        def tableName = "test_f_default_value_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` varchar(100) NOT NULL default "test",
            `v4` BIGINT DEFAULT null,
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        sql """insert into ${tableName} select number, number, number, number, number from numbers("number" = "4"); """
        qt_sql "select k,v1,v2,v3,v4 from ${tableName} order by k;"

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "default1.json"
            time 20000
        }
        qt_sql "select k,v1,v2,v3,v4 from ${tableName} order by k;"

        tableName = "test_f_default_value2_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL,
            `t1` datetime default current_timestamp,
            `t2` datetime(6) default current_timestamp(6),
            `t3` DATE DEFAULT CURRENT_DATE
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "default2.json"
            time 20000
        }
        qt_sql "select k,v1,v2 from ${tableName} where t1 > '2024-10-28 00:00:00' order by k;"
        qt_sql "select k,v1,v2 from ${tableName} where t2 > '2024-10-28 00:00:00' order by k;"
        qt_sql "select k,v1,v2 from ${tableName} where t3 > '2024-10-28 00:00:00' order by k;"
        // these generated default value should be the same in one batch
        qt_sql "select count(distinct t1) from ${tableName};"
        qt_sql "select count(distinct t2) from ${tableName};"
        qt_sql "select count(distinct t3) from ${tableName};"


        tableName = "test_f_default_value3_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL,
            `t1` datetime default current_timestamp on update current_timestamp,
            `t2` datetime(6) default current_timestamp(6) on update current_timestamp(6),
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${use_row_store}"); """
        sql """insert into ${tableName} select number, number, number, '2020-01-01 00:00:00', '2020-01-01 00:00:00' from numbers("number" = "5"); """
        qt_sql "select k,v1,v2,t1,t2 from ${tableName} order by k;"
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "default3.json"
            time 20000
        }
        qt_sql "select k,v1,v2 from ${tableName} order by k;"
        qt_sql "select k,v1,v2 from ${tableName} where t1 > '2024-10-28 00:00:00' order by k;"
        qt_sql "select k,v1,v2 from ${tableName} where t2 > '2024-10-28 00:00:00' order by k;"
        qt_sql "select k,v1,v2 from ${tableName} where t1 > '2024-10-28 00:00:00' and t2 > '2024-10-28 00:00:00' order by k;"
    }
}