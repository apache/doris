
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

suite("test_partial_update_case_insensitivity", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        for (def use_nereids_planner : [false, true]) {
            logger.info("current params: use_row_store: ${use_row_store}, use_nereids_planner: ${use_nereids_planner}")
            connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                sql "use ${db};"
                if (use_nereids_planner) {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                    sql """ set enable_nereids_planner = false; """
                }

                def tableName = "test_partial_update_case_insensitivity"
                sql """ DROP TABLE IF EXISTS ${tableName} """
                sql """ CREATE TABLE ${tableName} (
                        name varchar(300),
                        status int,
                        MY_COLUMN int,
                        UpAndDown int
                        ) ENGINE=OLAP
                        UNIQUE KEY(name) COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(name) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}");"""
                
                sql "set enable_unique_key_partial_update = true;"
                sql "set enable_insert_strict = false;"
                sql "sync;"

                sql """ insert into ${tableName}(name, STATUS) values("t1", 1); """
                qt_sql "select * from ${tableName} order by name;"
                sql """ insert into ${tableName}(name, my_column) values("t1", 2); """
                qt_sql "select * from ${tableName} order by name;"
                sql """ insert into ${tableName}(name, My_Column, uPaNddOWN) values("t2", 20, 30); """
                qt_sql "select * from ${tableName} order by name;"
                sql """ insert into ${tableName}(NAME, StAtUs, upanddown) values("t1", 999, 888); """
                qt_sql "select * from ${tableName} order by name;"
                sql """ insert into ${tableName}(NaMe, StAtUs, mY_CoLUmN, upAndDoWn) values("t3", 123, 456, 789); """
                qt_sql "select * from ${tableName} order by name;"

                sql """ DROP TABLE IF EXISTS ${tableName} """
            }
        }
    }
}
