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

suite("test_delete_on_mor") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_nereids_planner : [false, true]) {
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

            def tableA = "test_delete_on_mor1"
            sql """ DROP TABLE IF EXISTS ${tableA} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableA} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别")
                UNIQUE KEY(`user_id`)
                DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "false",
                    "replication_allocation" = "tag.location.default: 1"
                );"""

            def tableB = "test_delete_on_mor2"
            sql """ DROP TABLE IF EXISTS ${tableB} """
            sql """ CREATE TABLE IF NOT EXISTS ${tableB} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别")
                UNIQUE KEY(`user_id`)
                DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "false",
                    "replication_allocation" = "tag.location.default: 1"
                );"""

            sql """insert into ${tableA} values
                (10000,"u1","北京",19,1),
                (10000,"u1","北京",20,1),
                (10001,"u3","北京",30,0),
                (10002,"u4","上海",20,1),
                (10003,"u5","重庆",32,0),
                (10004,"u6","重庆",35,1),
                (10004,"u7","重庆",35,1);  """

            sql """insert into ${tableB} values
                (10000,"u1","北京",18,1),
                (10000,"u1","北京",20,1),
                (10001,"u3","北京",30,0),
                (10002,"u4","上海",20,1),
                (10003,"u5","广州",32,0),
                (10004,"u6","深圳",35,1),
                (10004,"u7","深圳",35,1); """
            
            sql "sync;"
            qt_sql "select * from ${tableA} order by user_id;"
            qt_sql "select * from ${tableB} order by user_id;"
            sql """ DELETE FROM ${tableA} a USING ${tableB} b
                WHERE a.user_id = b.user_id AND a.city = b.city
                and b.city = '北京' AND b.age = 20;"""
            sql "sync;"
            qt_sql "select * from ${tableA} order by user_id;"

            sql """DELETE from ${tableA} USING ${tableA}  a
            JOIN (
                SELECT a.user_id, a.city
                FROM ${tableA} a
                JOIN ${tableB} ON a.user_id = ${tableB}.user_id AND a.city = ${tableB}.city
                WHERE ${tableB}.city = '上海' AND ${tableB}.age = 20
            ) AS matched_rows
            ON ${tableA}.user_id = matched_rows.user_id AND ${tableA}.city = matched_rows.city; """
            qt_sql "select * from ${tableA} order by user_id;"

            sql "DROP TABLE IF EXISTS ${tableA};"
            sql "DROP TABLE IF EXISTS ${tableB};"
        }
    }
}
