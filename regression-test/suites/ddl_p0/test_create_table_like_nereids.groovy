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

suite("test_create_table_like_nereids") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists mal_test_create_table_like"
    sql """create table mal_test_create_table_like(pk int, a int, b int) distributed by hash(pk) buckets 10
    properties('replication_num' = '1');"""
    sql """insert into mal_test_create_table_like values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
    ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);"""
    sql "alter table mal_test_create_table_like add rollup ru1(a,pk);"
    waitForSchemaChangeDone {
        sql """show alter table rollup where tablename='mal_test_create_table_like' order by createtime desc limit 1"""
        time 600
    }
    sql "alter table mal_test_create_table_like add rollup ru2(b,pk)"
    waitForSchemaChangeDone {
        sql """show alter table rollup where tablename='mal_test_create_table_like' order by createtime desc limit 1"""
        time 600
    }

    // no rollup
    sql "drop table if exists table_like"
    sql "CREATE TABLE table_like LIKE mal_test_create_table_like;"
    sql """insert into table_like values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
    ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);"""
    qt_test_without_roll_up "select * from table_like order by pk,a,b;"

    // with all rollup
    sql "drop table if exists table_like_with_roll_up"
    sql "CREATE TABLE table_like_with_roll_up LIKE mal_test_create_table_like with rollup;"
    explain {
        sql ("select sum(a) from table_like_with_roll_up group by a")
        contains "ru1"
    } ;
    explain {
        sql ("select sum(b) from table_like_with_roll_up group by b,pk ;")
        contains "ru2"
    } ;

    // with partial rollup
    sql "drop table if exists table_like_with_partial_roll_up;"
    sql "CREATE TABLE table_like_with_partial_roll_up LIKE mal_test_create_table_like with rollup (ru1);"
    sql "select * from table_like_with_partial_roll_up order by pk, a, b"
    explain {
        sql("select sum(a) from table_like_with_partial_roll_up group by a")
        contains("ru1")
    } ;
    explain {
        sql ("select sum(b) from table_like_with_partial_roll_up group by b,pk ;")
        notContains "ru2"
    } ;
    sql """insert into table_like_with_partial_roll_up values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
    ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);"""
    sleep(2000)
    sql "select sum(a) from table_like_with_partial_roll_up group by a order by 1"

    // test if not exists
    sql "drop table if exists table_like_with_partial_roll_up_exists"
    sql """CREATE TABLE if not exists table_like_with_partial_roll_up_exists
    LIKE mal_test_create_table_like with rollup (ru1);"""

    sql "drop table if exists test_create_table_like_char_255"
    sql """
        CREATE TABLE test_create_table_like_char_255
        (
        `id` INT NOT NULL,
        `name` CHAR(255)
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES (
        "replication_num" = "1",
        "light_schema_change" = "true"
        );
    """
    sql "drop table if exists new_char_255"
    qt_test_char_255 """
        create table new_char_255 like test_create_table_like_char_255;
    """
    def res1 = sql "show create table new_char_255"
    mustContain(res1[0][1], "character(255)")

    sql "insert into new_char_255 values(123,'abcdddddd')"
    qt_select "select * from new_char_255"
}