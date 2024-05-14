
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

suite("test_partial_update_native_insert_stmt", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"
            sql "sync;"

            // sql 'set enable_fallback_to_original_planner=false'
            def tableName = "test_partial_update_native_insert_stmt"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                    CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true","store_row_column" = "${use_row_store}"); """

            sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
            qt_1 """ select * from ${tableName} order by id; """
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            // partial update using insert stmt in non-strict mode,
            // existing rows should be updated and new rows should be inserted with unmentioned columns filled with default or null value
            sql """insert into ${tableName}(id,score) values(2,400),(1,200),(4,400)"""
            qt_1 """ select * from ${tableName} order by id; """
            test {
                sql """insert into ${tableName} values(2,400),(1,200),(4,400)"""
                exception "Column count doesn't match value count"
            }
            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            sql """ DROP TABLE IF EXISTS ${tableName} """


            def tableName2 = "test_partial_update_native_insert_stmt2" 
            sql """ DROP TABLE IF EXISTS ${tableName2} """
            sql """
                    CREATE TABLE ${tableName2} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321",
                        `update_time` date NULL)
                    UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "function_column.sequence_col" = "update_time",
                        "store_row_column" = "${use_row_store}"); """

            sql """ insert into ${tableName2} values
                    (2, "doris2", 2000, 223, 1, '2023-01-01'),
                    (1, "doris", 1000, 123, 1, '2023-01-01');"""
            qt_2 "select * from ${tableName2} order by id;"
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            // partial update with seq col
            sql """ insert into ${tableName2}(id,score,update_time) values
                    (2,2500,"2023-07-19"),
                    (2,2600,"2023-07-20"),
                    (1,1300,"2022-07-19"),
                    (3,1500,"2022-07-20"),
                    (3,2500,"2022-07-18"); """
            qt_2 "select * from ${tableName2} order by id;"
            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            sql """ DROP TABLE IF EXISTS ${tableName2}; """


            def tableName3 = "test_partial_update_native_insert_stmt3"
            sql """ DROP TABLE IF EXISTS ${tableName3}; """
            sql """
                    CREATE TABLE ${tableName3} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """

            sql """insert into ${tableName3} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1);"""
            qt_3 """ select * from ${tableName3} order by id; """
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            // in partial update, the unmentioned columns should have default values or be nullable
            // but field `name` is not nullable and doesn't have default value
            test {
                sql """insert into ${tableName3}(id,score) values(2,400),(1,200),(4,400)"""
                exception "the unmentioned column `name` should have default value or be nullable"
            }
            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            qt_3 """ select * from ${tableName3} order by id; """
            sql """ DROP TABLE IF EXISTS ${tableName3} """


            def tableName4 = "test_partial_update_native_insert_stmt4"
            sql """ DROP TABLE IF EXISTS ${tableName4} """
            sql """
                    CREATE TABLE ${tableName4} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                        `score` int(11) NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """

            sql """insert into ${tableName4} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1),(3,"doris3",5000,34,345);"""
            qt_4 """ select * from ${tableName4} order by id; """
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            // partial update with delete sign
            sql "insert into ${tableName4}(id,__DORIS_DELETE_SIGN__) values(2,1);"
            qt_4 """ select * from ${tableName4} order by id; """
            sql "set enable_unique_key_partial_update=false;"
            sql "sync;"
            sql """ DROP TABLE IF EXISTS ${tableName4} """


            def tableName5 = "test_partial_update_native_insert_stmt5"
            sql """ DROP TABLE IF EXISTS ${tableName5} """
            sql """
                    CREATE TABLE ${tableName5} ( 
                        `id` int(11) NULL, 
                        `name` varchar(10) NULL,
                        `age` int(11) NULL DEFAULT "20", 
                        `city` varchar(10) NOT NULL DEFAULT "beijing", 
                        `balance` decimalv3(9, 0) NULL, 
                        `last_access_time` datetime NULL 
                    ) ENGINE = OLAP UNIQUE KEY(`id`) 
                    COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
                    BUCKETS AUTO PROPERTIES ( 
                        "replication_allocation" = "tag.location.default: 1", 
                        "storage_format" = "V2", 
                        "enable_unique_key_merge_on_write" = "true", 
                        "light_schema_change" = "true", 
                        "disable_auto_compaction" = "false", 
                        "enable_single_replica_compaction" = "false",
                        "store_row_column" = "${use_row_store}"); """

            sql """insert into ${tableName5} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
            qt_5 """select * from ${tableName5} order by id;"""
            sql "set enable_insert_strict = true;"
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            // partial update using insert stmt in strict mode, the max_filter_ratio is always 0
            test {
                sql """ insert into ${tableName5}(id,balance,last_access_time) values(1,500,"2023-07-03 12:00:01"),(3,23,"2023-07-03 12:00:02"),(18,9999999,"2023-07-03 12:00:03"); """
                exception "Insert has filtered data in strict mode"
            }
            qt_5 """select * from ${tableName5} order by id;"""
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            sql """ DROP TABLE IF EXISTS ${tableName5}; """

            def tableName6 = "test_partial_update_native_insert_stmt6"
            sql """ DROP TABLE IF EXISTS ${tableName6} """
            sql """create table ${tableName6} (
                k int null,
                v int null,
                v2 int null,
                v3 int null
            ) unique key (k) distributed by hash(k) buckets 1
            properties("replication_num" = "1",
            "enable_unique_key_merge_on_write"="true",
            "disable_auto_compaction"="true",
            "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName6} values(1,1,3,4),(2,2,4,5),(3,3,2,3),(4,4,1,2);"
            qt_6 "select * from ${tableName6} order by k;"
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${tableName6}(k,v) select v2,v3 from ${tableName6};"
            qt_6 "select * from ${tableName6} order by k;"
            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict = false;"
            sql "sync;"
            sql """ DROP TABLE IF EXISTS ${tableName6}; """

            def tableName7 = "test_partial_update_native_insert_stmt7"
            sql """ DROP TABLE IF EXISTS ${tableName7} """
            sql """create table ${tableName7} (
                k1 int null,
                k2 int null,
                k3 int null,
                v1 int null,
                v2 int null
            ) unique key (k1,k2,k3) distributed by hash(k1,k2) buckets 4
            properties("replication_num" = "1",
            "enable_unique_key_merge_on_write"="true",
            "disable_auto_compaction"="true",
            "store_row_column" = "${use_row_store}"); """

            sql "insert into ${tableName7} values(1,1,1,3,4),(2,2,2,4,5),(3,3,3,2,3),(4,4,4,1,2);"
            qt_7 "select * from ${tableName7} order by k1;"
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            test {
                sql "insert into ${tableName7}(k1,k2,v2) select k2,k3,v1 from ${tableName7};"
                exception "Partial update should include all key columns, missing: k3"
            }
            qt_7 "select * from ${tableName7} order by k1;"
            sql """ DROP TABLE IF EXISTS ${tableName7}; """

            sql "set enable_unique_key_partial_update=false;"
            sql "set enable_insert_strict = false;"
            sql "set experimental_enable_nereids_planner=true;"
            sql "sync;"
        }
    }

    // test that session variable `enable_unique_key_partial_update` will only affect unique tables
    for (def use_nerieds : [true, false]) {
        logger.info("current params: use_nerieds: ${use_nerieds}")
        if (use_nerieds) {
            sql "set enable_nereids_planner=true;"
            sql "set enable_nereids_dml=true;"
            sql "set enable_fallback_to_original_planner=false;"
            sql "sync;"
        } else {
            sql "set enable_nereids_planner=false;"
            sql "set enable_nereids_dml=false;"
            sql "sync;"
        }

        sql "set enable_unique_key_partial_update=true;"
        sql "sync;"

        def tableName8 = "test_partial_update_native_insert_stmt_agg_${use_nerieds}"
        sql """ DROP TABLE IF EXISTS ${tableName8}; """
        sql """ CREATE TABLE IF NOT EXISTS ${tableName8} (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `timestamp` DATETIME NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `sex` TINYINT,
            `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00",
            `cost` BIGINT SUM DEFAULT "0",
            `max_dwell_time` INT MAX DEFAULT "0",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
        )AGGREGATE KEY(`user_id`, `date`, `timestamp` ,`city`, `age`, `sex`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

        sql """insert into ${tableName8} values
        (10000,"2017-10-01","2017-10-01 08:00:05","北京",20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01","2017-10-01 09:00:05","北京",20,0,"2017-10-01 07:00:00",15,2,2);  """
        qt_sql "select * from ${tableName8} order by user_id;"


        def tableName9 = "test_partial_update_native_insert_stmt_dup_${use_nerieds}"
        sql """ DROP TABLE IF EXISTS ${tableName9}; """
        sql """ CREATE TABLE IF NOT EXISTS ${tableName9} (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `timestamp` DATETIME NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `sex` TINYINT,
            `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00",
            `cost` BIGINT DEFAULT "0",
            `max_dwell_time` INT DEFAULT "0",
            `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间"
        )DUPLICATE KEY(`user_id`, `date`, `timestamp` ,`city`, `age`, `sex`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");"""

        sql """insert into ${tableName9} values
        (10000,"2017-10-01","2017-10-01 08:00:05","北京",20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01","2017-10-01 09:00:05","北京",20,0,"2017-10-01 07:00:00",15,2,2);  """
        qt_sql "select * from ${tableName9} order by user_id;"


        def tableName10 = "test_partial_update_native_insert_stmt_mor_${use_nerieds}"
        sql """ DROP TABLE IF EXISTS ${tableName10}; """
        sql """ CREATE TABLE IF NOT EXISTS ${tableName10} (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `timestamp` DATETIME NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `sex` TINYINT,
            `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00",
            `cost` BIGINT DEFAULT "0",
            `max_dwell_time` INT DEFAULT "0",
            `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间"
        )UNIQUE KEY(`user_id`, `date`, `timestamp` ,`city`, `age`, `sex`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1", "enable_unique_key_merge_on_write" = "false");"""

        sql """insert into ${tableName10} values
        (10000,"2017-10-01","2017-10-01 08:00:05","北京",20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01","2017-10-01 09:00:05","北京",20,0,"2017-10-01 07:00:00",15,2,2);  """
        qt_sql "select * from ${tableName10} order by user_id;"

        sql """ DROP TABLE IF EXISTS ${tableName8}; """
        sql """ DROP TABLE IF EXISTS ${tableName9}; """
        sql """ DROP TABLE IF EXISTS ${tableName10}; """
    }
}
