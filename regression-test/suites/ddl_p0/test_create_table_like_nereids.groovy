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

    sql "drop table if exists mal_test_create_table_like"
    sql """create table mal_test_create_table_like(pk int, a int, b int) distributed by hash(pk) buckets 10
    properties('replication_num' = '1');"""
    sql """insert into mal_test_create_table_like values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
    ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);"""
    sql "sync"
    sql "alter table mal_test_create_table_like add rollup ru1(a,pk);"
    sleep(2000)
    sql "alter table mal_test_create_table_like add rollup ru2(b,pk);"
    sleep(2000)

    // no rollup
    sql "drop table if exists table_like"
    sql "CREATE TABLE table_like LIKE mal_test_create_table_like;"
    sql """insert into table_like values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
    ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);"""
    "sync"
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
    sql "sync"
    sleep(2000)
    sql "select sum(a) from table_like_with_partial_roll_up group by a order by 1"

    // test if not exists
    sql "drop table if exists table_like_with_partial_roll_up_exists"
    sql """CREATE TABLE if not exists table_like_with_partial_roll_up_exists
    LIKE mal_test_create_table_like with rollup (ru1);"""
}