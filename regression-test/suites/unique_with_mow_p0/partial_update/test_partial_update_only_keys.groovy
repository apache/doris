
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

suite("test_partial_update_only_keys", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"
            def tableName = "test_partial_update_only_keys"
            sql """ DROP TABLE IF EXISTS ${tableName} force"""
            sql """ CREATE TABLE ${tableName} (
                    `k` BIGINT NOT NULL,
                    `c1` int,
                    `c2` int,
                    `c3` int)
                    UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """
            sql """insert into ${tableName} select number,number,number,number from numbers("number"="3");"""
            qt_sql """select * from ${tableName} order by k;"""
            // new rows will be appended
            sql "set enable_unique_key_partial_update=true;"
            sql "sync"
            sql "insert into ${tableName}(k) values(0),(1),(4),(5),(6);"
            qt_sql """select * from ${tableName} order by k;"""

            // fail if has new rows when partial_update_new_key_behavior=ERROR
            sql """set partial_update_new_key_behavior="ERROR";"""
            sql "sync"
            sql "insert into ${tableName}(k) values(0),(1),(4),(5),(6);"
            qt_sql """select * from ${tableName} order by k;"""
            test {
                sql "insert into ${tableName}(k) values(0),(1),(10),(11);"
                exception "[E-7003]Can't append new rows in partial update when partial_update_new_key_behavior is ERROR"
            }
            qt_sql """select * from ${tableName} order by k;"""
        }
    }
}
