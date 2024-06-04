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

suite("eliminate_aggregate_casewhen") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'DROP DATABASE IF EXISTS test_eliminate_aggregate_casewhen'
    sql 'CREATE DATABASE IF NOT EXISTS test_eliminate_aggregate_casewhen'
    sql 'use test_eliminate_aggregate_casewhen'


    // create tables
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""
    sql """create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');"""
    sql """create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');"""

    sql "insert into t1 values (101, 101)"
    sql "insert into t2 values (null, null)"
    sql "insert into t4 values (102, 102)"
    sql "insert into t4 values (103, 103)"

    /* ******** with one row ******** */
    qt_basic_1 """explain shape plan select max(case when t1.c1 > 100 then 10 end) from t1;"""
    qt_basic_2 """explain shape plan select count(case when t1.c1 > 100 then 10 end) from t1;"""
    qt_basic_3 """explain shape plan select t1.c1, count(case when t1.c1 > 100 then 10 end) from t1 group by t1.c1;"""
    qt_basic_4 """explain shape plan select max(case when t1.c1 > 100 then 10 end) from t1;"""
    qt_basic_5 """explain shape plan select count(case when t1.c1 > 100 then 10 end) from t1;"""
    qt_basic_6 """explain shape plan select t1.c1, count(case when t1.c1 > 100 then 10 end) from t1 group by t1.c1;"""

    /* ******** with one row "null" ******** */
    qt_basic_2_1 """explain shape plan select max(case when t2.c2 > 100 then 10 end) from t2;"""
    qt_basic_2_2 """explain shape plan select count(case when t2.c2 > 100 then 10 end) from t2;"""
    qt_basic_2_3 """explain shape plan select t2.c2, count(case when t2.c2 > 100 then 10 end) from t2 group by t2.c2;"""
    qt_basic_2_4 """explain shape plan select max(case when t2.c2 > 100 then 10 end) from t2;"""
    qt_basic_2_5 """explain shape plan select count(case when t2.c2 > 100 then 10 end) from t2;"""
    qt_basic_2_6 """explain shape plan select t2.c2, count(case when t2.c2 > 100 then 10 end) from t2 group by t2.c2;"""

    /* ******** with empty table ******** */
    qt_basic_3_1 """explain shape plan select max(case when t3.c3 > 100 then 10 end) from t3;"""
    qt_basic_3_2 """explain shape plan select count(case when t3.c3 > 100 then 10 end) from t3;"""
    qt_basic_3_3 """explain shape plan select t3.c3, count(case when t3.c3 > 100 then 10 end) from t3 group by t3.c3;"""
    qt_basic_3_4 """explain shape plan select max(case when t3.c3 > 100 then 10 end) from t3;"""
    qt_basic_3_5 """explain shape plan select count(case when t3.c3 > 100 then 10 end) from t3;"""
    qt_basic_3_6 """explain shape plan select t3.c3, count(case when t3.c3 > 100 then 10 end) from t3 group by t3.c3;"""

    /* ******** with different group table ******** */
    qt_basic_4_1 """explain shape plan select max(case when t4.c4 > 100 then 10 end) from t4;"""
    qt_basic_4_2 """explain shape plan select count(case when t4.c4 > 100 then 10 end) from t4;"""
    qt_basic_4_4 """explain shape plan select t4.c4, count(case when t4.c4 > 100 then 10 end) from t4 group by t4.c4;"""
    qt_basic_4_4 """explain shape plan select max(case when t4.c4 > 100 then 10 end) from t4;"""
    qt_basic_4_5 """explain shape plan select count(case when t4.c4 > 100 then 10 end) from t4;"""
    qt_basic_4_6 """explain shape plan select t4.c4, count(case when t4.c4 > 100 then 10 end) from t4 group by t4.c4;"""

    /* ******** Output ******** */

    /* ******** with one row ******** */
    order_qt_basic_1 """select max(case when t1.c1 > 100 then 10 end) from t1;"""
    order_qt_basic_2 """select count(case when t1.c1 > 100 then 10 end) from t1;"""
    order_qt_basic_3 """select t1.c1, count(case when t1.c1 > 100 then 10 end) from t1 group by t1.c1;"""
    order_qt_basic_4 """select max(case when t1.c1 > 100 then 10 end) from t1;"""
    order_qt_basic_5 """select count(case when t1.c1 > 100 then 10 end) from t1;"""
    order_qt_basic_6 """select t1.c1, count(case when t1.c1 > 100 then 10 end) from t1 group by t1.c1;"""

    /* ******** with one row "null" ******** */
    order_qt_basic_2_1 """select max(case when t2.c2 > 100 then 10 end) from t2;"""
    order_qt_basic_2_2 """select count(case when t2.c2 > 100 then 10 end) from t2;"""
    order_qt_basic_2_3 """select t2.c2, count(case when t2.c2 > 100 then 10 end) from t2 group by t2.c2;"""
    order_qt_basic_2_4 """select max(case when t2.c2 > 100 then 10 end) from t2;"""
    order_qt_basic_2_5 """select count(case when t2.c2 > 100 then 10 end) from t2;"""
    order_qt_basic_2_6 """select t2.c2, count(case when t2.c2 > 100 then 10 end) from t2 group by t2.c2;"""

    /* ******** with empty table ******** */
    order_qt_basic_3_1 """select max(case when t3.c3 > 100 then 10 end) from t3;"""
    order_qt_basic_3_2 """select count(case when t3.c3 > 100 then 10 end) from t3;"""
    order_qt_basic_3_3 """select t3.c3, count(case when t3.c3 > 100 then 10 end) from t3 group by t3.c3;"""
    order_qt_basic_3_4 """select max(case when t3.c3 > 100 then 10 end) from t3;"""
    order_qt_basic_3_5 """select count(case when t3.c3 > 100 then 10 end) from t3;"""
    order_qt_basic_3_6 """select t3.c3, count(case when t3.c3 > 100 then 10 end) from t3 group by t3.c3;"""

    /* ******** with different group table ******** */
    order_qt_basic_4_1 """select max(case when t4.c4 > 100 then 10 end) from t4;"""
    order_qt_basic_4_2 """select count(case when t4.c4 > 100 then 10 end) from t4;"""
    order_qt_basic_4_4 """select t4.c4, count(case when t4.c4 > 100 then 10 end) from t4 group by t4.c4;"""
    order_qt_basic_4_4 """select max(case when t4.c4 > 100 then 10 end) from t4;"""
    order_qt_basic_4_5 """select count(case when t4.c4 > 100 then 10 end) from t4;"""
    order_qt_basic_4_6 """select t4.c4, count(case when t4.c4 > 100 then 10 end) from t4 group by t4.c4;"""
}
