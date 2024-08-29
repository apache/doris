
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

suite("test_partial_update_columns_check", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_nereids_planner : [true, false]) {
        logger.info("current params: use_nereids_planner: ${use_nereids_planner}")
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
            sql "sync;"

            sql """ DROP TABLE IF EXISTS t1 """
            sql """  CREATE TABLE IF NOT EXISTS t1 (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int
                ) DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
                """
            sql """ DROP TABLE IF EXISTS t_mow """
            sql """  CREATE TABLE IF NOT EXISTS t_mow (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
                );
                """
            sql """ DROP TABLE IF EXISTS t_mor """
            sql """  CREATE TABLE IF NOT EXISTS t_mor (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int
                ) UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "false"
                );
                """
            sql """ DROP TABLE IF EXISTS t_dup """
            sql """  CREATE TABLE IF NOT EXISTS t_dup (
                `k` BIGINT NOT NULL,
                `c1` int,
                `c2` int
                ) DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
                """
            sql """ DROP TABLE IF EXISTS t_agg """
            sql """  CREATE TABLE IF NOT EXISTS t_agg (
                `k` BIGINT NOT NULL,
                `c1` int max,
                `c2` int min
                ) AGGREGATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
                """
            streamLoad {
                table "t1"
                set 'columns', 'k,c1,c2'
                set 'column_separator', ','
                set 'format', 'csv'
                file 'partial_update_columns_check1.csv'
                time 10000
            }
            order_qt_sql "select * from t1;"


            sql "set enable_unique_key_partial_update = true;"
            sql "set enable_insert_strict = false;"
            sql "sync;"

            test {
                sql "insert into t_mow select * from t1;"
                exception "You must explicitly specify the columns to be updated when updating partial columns using the INSERT statement."
            }

            sql "insert into t_mor select * from t1;"
            order_qt_mor "select * from t_mor;"
            sql "insert into t_dup select * from t1;"
            order_qt_dup "select * from t_dup;"
            sql "insert into t_agg select * from t1;"
            order_qt_agg "select * from t_agg;"

            sql """ DROP TABLE IF EXISTS t1;"""
            sql """ DROP TABLE IF EXISTS t_mow;"""
            sql """ DROP TABLE IF EXISTS t_mor;"""
            sql """ DROP TABLE IF EXISTS t_dup;"""
            sql """ DROP TABLE IF EXISTS t_agg;"""
        }
    }
}
