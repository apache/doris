
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

    for (def use_mow : [false, true]) {
        for (def use_nereids_planner : [false, true]) {
            logger.info("current params: use_mow: ${use_mow}, use_nereids_planner: ${use_nereids_planner}")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql "use ${db};"

                if (use_nereids_planner) {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                    sql """ set enable_nereids_planner = false; """
                }

                // create table
                sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc """
                sql """ CREATE TABLE test_primary_key_partial_update_auto_inc (
                            `id` BIGINT NOT NULL AUTO_INCREMENT,
                            `name` varchar(65533) NOT NULL COMMENT "用户姓名" )
                            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                            PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "${use_mow}"); """

                sql """ set enable_unique_key_partial_update=true; """
                sql """ insert into test_primary_key_partial_update_auto_inc(name) values("doris1"); """
                sql """ set enable_unique_key_partial_update=false; """
                sql """ insert into test_primary_key_partial_update_auto_inc(name) values("doris2"); """
                sql "sync"

                qt_select_1 """ select name from test_primary_key_partial_update_auto_inc order by name; """
                qt_select_2 """ select count(distinct id) from test_primary_key_partial_update_auto_inc; """

                sql """ set enable_unique_key_partial_update=true; """
                sql """ insert into test_primary_key_partial_update_auto_inc values(100,"doris3"); """
                sql """ set enable_unique_key_partial_update=false; """
                sql """ insert into test_primary_key_partial_update_auto_inc values(101, "doris4"); """
                sql "sync"
                qt_select_3 """ select name from test_primary_key_partial_update_auto_inc order by name; """
                qt_select_4 """ select count(distinct id) from test_primary_key_partial_update_auto_inc; """

                sql """ DROP TABLE IF EXISTS test_primary_key_partial_update_auto_inc """
            }
        }
    }
}
