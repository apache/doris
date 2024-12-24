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

suite("test_partial_update_auto_inc") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_nereids_planner : [false, true]) {
        logger.info("current params: use_nereids_planner: ${use_nereids_planner}")
        connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"

            if (use_nereids_planner) {
                sql """ set enable_nereids_planner=true; """
                sql """ set enable_fallback_to_original_planner=false; """
            } else {
                sql """ set enable_nereids_planner = false; """
            }

            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc """
            sql """ CREATE TABLE test_primary_key_partial_update_auto_inc (
                        `id` BIGINT NOT NULL AUTO_INCREMENT,
                        `name` varchar(65533) NOT NULL COMMENT "用户姓名" )
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true"); """

            sql """ set enable_unique_key_partial_update=true; """
            sql "sync"
            // insert stmt only misses auto-inc key column
            sql """ insert into test_primary_key_partial_update_auto_inc(name) values("doris1"); """
            sql """ set enable_unique_key_partial_update=false; """
            sql "sync"
            sql """ insert into test_primary_key_partial_update_auto_inc(name) values("doris2"); """
            // stream load only misses auto-inc key column
            streamLoad {
                table "test_primary_key_partial_update_auto_inc"
                set 'partial_columns', 'true'
                set 'column_separator', ','
                set 'columns', 'name'
                file 'partial_update_autoinc1.csv'
                time 10000
            }
            qt_select_1 """ select name from test_primary_key_partial_update_auto_inc order by name; """
            qt_select_2 """ select count(distinct id) from test_primary_key_partial_update_auto_inc; """

            sql """ set enable_unique_key_partial_update=true; """
            sql "sync"
            // insert stmt withou column list
            sql """ insert into test_primary_key_partial_update_auto_inc values(100,"doris5"); """
            // insert stmt, column list include all visible columns
            sql """ insert into test_primary_key_partial_update_auto_inc(id,name) values(102,"doris6"); """
            sql """ set enable_unique_key_partial_update=false; """
            sql "sync"
            sql """ insert into test_primary_key_partial_update_auto_inc values(101, "doris7"); """
            // stream load withou column list
            streamLoad {
                table "test_primary_key_partial_update_auto_inc"
                set 'partial_columns', 'true'
                set 'column_separator', ','
                file 'partial_update_autoinc2.csv'
                time 10000
            }
            // stream load, column list include all visible columns
            streamLoad {
                table "test_primary_key_partial_update_auto_inc"
                set 'partial_columns', 'true'
                set 'column_separator', ','
                set 'columns', 'id,name'
                file 'partial_update_autoinc3.csv'
                time 10000
            }
            qt_select_3 """ select name from test_primary_key_partial_update_auto_inc order by name; """
            qt_select_4 """ select count(distinct id) from test_primary_key_partial_update_auto_inc; """
            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc """


            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc2 """
            sql """ CREATE TABLE test_primary_key_partial_update_auto_inc2 (
                        `id` BIGINT NOT NULL,
                        `c1` int,
                        `c2` int,
                        `cid` BIGINT NOT NULL AUTO_INCREMENT)
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true"); """
            sql "insert into test_primary_key_partial_update_auto_inc2 values(1,10,10,10),(2,20,20,20),(3,30,30,30),(4,40,40,40);"
            order_qt_select_5 "select * from test_primary_key_partial_update_auto_inc2"
            sql """ set enable_unique_key_partial_update=true; """
            sql "sync;"
            // insert stmt only misses auto-inc value column, its value should not change when do partial update
            sql "insert into test_primary_key_partial_update_auto_inc2(id,c1,c2) values(1,99,99),(2,99,99);"
            // stream load only misses auto-inc value column, its value should not change when do partial update
            streamLoad {
                table "test_primary_key_partial_update_auto_inc2"
                set 'partial_columns', 'true'
                set 'column_separator', ','
                set 'columns', 'id,c1,c2'
                file 'partial_update_autoinc4.csv'
                time 10000
            }
            order_qt_select_6 "select * from test_primary_key_partial_update_auto_inc2"
            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc2 """

            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc3 force; """
            sql """ create table test_primary_key_partial_update_auto_inc3
                (
                    `id`                      bigint         not null AUTO_INCREMENT,
                    `project_code`            varchar(20)    not null,
                    `period_num`              int,
                    `c2`     int
                ) unique KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS auto
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
                );   """
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            
            sql "insert into test_primary_key_partial_update_auto_inc3(project_code,period_num) values ('test1',15),('test2',29),('test3',49);"
            qt_sql "select project_code,period_num from test_primary_key_partial_update_auto_inc3 order by project_code,period_num;"
            qt_sql "select count(distinct id) from test_primary_key_partial_update_auto_inc3;"


            sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc4 """
            sql """ CREATE TABLE test_primary_key_partial_update_auto_inc4 (
                            `k1` BIGINT NOT NULL AUTO_INCREMENT,
                            `k2` int,
                            `c1` int,
                            `c2` int,
                            `c3` int)
                            UNIQUE KEY(`k1`,`k2`) DISTRIBUTED BY HASH(`k1`,`k2`) BUCKETS 1
                            PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true"); """
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"

            test {
                sql "insert into test_primary_key_partial_update_auto_inc4(c1,c2) values(1,1),(2,2),(3,3)"
                exception "Partial update should include all key columns, missing: k2"
            }

        }
    }
}
