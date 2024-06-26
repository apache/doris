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

suite("nereids_update_on_current_timestamp") {
    sql 'set experimental_enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql "sync;"


    def t1 = "nereids_update_on_current_timestamp1"
    sql """ DROP TABLE IF EXISTS ${t1};"""
    sql """ CREATE TABLE ${t1} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321",
                `update_time` datetime default current_timestamp on update current_timestamp,
                `update_time2` datetime(6) default current_timestamp(5) on update current_timestamp(5))
            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );"""
    
    def res = sql "show create table ${t1};"
    assertTrue(res.toString().containsIgnoreCase("`update_time` datetime NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    assertTrue(res.toString().containsIgnoreCase("`update_time2` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP(5) ON UPDATE CURRENT_TIMESTAMP(5)"))
    
    // set enable_unique_key_partial_update=false, it's a row update
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"
    sql """ insert into ${t1}(id,name,score,test,dft) values
            (2, "doris2", 2000, 223, 1),
            (1, "doris", 1000, 123, 1);"""
    qt_sql "select id,name,score,test,dft from ${t1} order by id;"
    // rows with id=1 or id=2 will have the same value 't1' in `update_time` and `update_time2`
    qt_1 "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';" // 1
    qt_1 "select count(distinct update_time2) from ${t1} where update_time2 > '2023-10-01 00:00:00';" // 1
    sleep(2000)


    // set enable_unique_key_partial_update=true, it's a partial update
    // don't specify the `update_time` column
    // it will be automatically updated to current_timestamp()
    sql "set enable_unique_key_partial_update=true;"
    sql "set enable_insert_strict=false;"
    sql "sync;"
    sql """ insert into ${t1}(id, score) values
            (2, 2999),
            (1, 1999),
            (3, 3999),
            (4, 4999);"""
    qt_2 "select id,name,score,test,dft from ${t1} order by id;"
    // the existing rows(id=1,2) and newly inserted rows(id=3,4) are updated at the same time
    // so they will have the same value 't1 + 2000ms' in `update_time` and `update_time2` 
    qt_2 "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';" // 1
    qt_2 "select count(distinct update_time2) from ${t1} where update_time2 > '2023-10-01 00:00:00';" // 1
    sleep(2000)

    // when user specify that column, it will be filled with the input value
    sql """ insert into ${t1}(id, update_time) values
            (1, "2000-01-01 00:00:01"),
            (2, "2000-01-02 00:00:01");"""
    qt_3 "select id,name,score,test,dft from ${t1} order by id;"
    // rows with id=1,2 are updated, the value of `update_time2` will be updated to `t1 + 4000ms`
    qt_3 "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';" // 1
    qt_3 "select count(distinct update_time) from ${t1};" // 3 := (1)(2)(3,4)
    qt_3 "select count(distinct update_time2) from ${t1} where update_time2 > '2023-10-01 00:00:00';" // 2 := (1,2)(3,4)
    sleep(2000)

    // test update statement
    sql """ update ${t1} set score = score * 2 where id < 3;"""
    // rows with id=1,2 are updated, the value of `update_time`, `update_time2` will be updated to `t1 + 6000ms`
    qt_4 "select id,name,score,test,dft from ${t1} order by id;"
    qt_4 "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';" // 2 := (1,2)(3,4)
    qt_4 "select count(distinct update_time2) from ${t1} where update_time2 > '2023-10-01 00:00:00';" // 2 := (1,2)(3,4)
    sleep(2000)

    sql """ update ${t1} set update_time = "2023-10-02 00:00:00" where id > 3;"""
    // rows with id=4 are updated, the value of `update_time`, `update_time2` will be updated to `t1 + 8000ms`
    qt_5 "select id,name,score,test,dft from ${t1} order by id;"
    qt_5 "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';" // 3 := (1,2)(3)(4)
    qt_5 "select count(distinct update_time2) from ${t1} where update_time2 > '2023-10-01 00:00:00';" // 3 := (1,2)(3)(4)

    // illegal case 1: the default value is not current_timestamp
    def illegal_t1 = "nereids_update_on_current_timestamp_illegal_1"
    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t1} """
        sql """ CREATE TABLE ${illegal_t1} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321",
                    `update_time` datetime default "2020-01-01 00:00:00" on update current_timestamp)
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true");"""
        exception "You must set the default value of the column[update_time] to CURRENT_TIMESTAMP when using 'ON UPDATE CURRENT_TIMESTAMP'."
    }

    // illegal case 2: the default value is not set
    def illegal_t2 = "nereids_update_on_current_timestamp_illegal_2"
    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t2} """
        sql """ CREATE TABLE ${illegal_t2} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321",
                    `update_time` datetime on update current_timestamp)
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true");"""
        exception "You must set the default value of the column[update_time] to CURRENT_TIMESTAMP when using 'ON UPDATE CURRENT_TIMESTAMP'."
    }

    // illegal case 3: the precision of the default value is not the same
    def illegal_t3 = "nereids_update_on_current_timestamp_illegal_3"
    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t3} """
        sql """ CREATE TABLE ${illegal_t3} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321",
                    `update_time` datetime(6) default current_timestamp(4) on update current_timestamp(3))
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true");"""
        exception "The precision of the default value of column[update_time] should be the same with the precision in 'ON UPDATE CURRENT_TIMESTAMP'."
    }

    // illegal case 4: use 'update on current_timestamp' on incorrect table models
    def illegal_t4 = "nereids_update_on_current_timestamp_illegal_4"
    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t4} """
        sql """ CREATE TABLE ${illegal_t4} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321",
                    `update_time` datetime(6) default current_timestamp(3) on update current_timestamp(3))
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false");"""
        exception "'ON UPDATE CURRENT_TIMESTAMP' is only supportted in unique table with merge-on-write enabled."
    }

    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t4} """
        sql """ CREATE TABLE ${illegal_t4} (
                    `id` int(11) NOT NULL COMMENT "用户 ID",
                    `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                    `score` int(11) NOT NULL COMMENT "用户得分",
                    `test` int(11) NULL COMMENT "null test",
                    `dft` int(11) DEFAULT "4321",
                    `update_time` datetime default current_timestamp on update current_timestamp)
                DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1");"""
        exception "'ON UPDATE CURRENT_TIMESTAMP' is only supportted in unique table with merge-on-write enabled."
    }

    test {
        sql """ DROP TABLE IF EXISTS ${illegal_t4} """
        sql """ CREATE TABLE IF NOT EXISTS ${illegal_t4} (
                k int,
                `update_time` datetime(6) default current_timestamp(4) on update current_timestamp(3)) replace,
            ) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1");"""
        exception "Syntax error in line 3"
    }
}
