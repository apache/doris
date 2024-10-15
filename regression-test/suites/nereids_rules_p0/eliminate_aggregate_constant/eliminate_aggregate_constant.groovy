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

suite("eliminate_aggregate_constant") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql 'DROP DATABASE IF EXISTS test_aggregate_constant'
    sql 'CREATE DATABASE IF NOT EXISTS test_aggregate_constant'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql 'use test_aggregate_constant'

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
    qt_basic_1 """explain shape plan select max(1) from t1 group by c1;"""
    qt_basic_2 """explain shape plan select max(1) from t1;"""
    qt_basic_3 """explain shape plan select min(1) from t1 group by c1;"""
    qt_basic_4 """explain shape plan select min(1) from t1;"""
    qt_basic_5 """explain shape plan select sum(1) from t1 group by c1;"""
    qt_basic_6 """explain shape plan select sum(1) from t1;"""
    qt_basic_7 """explain shape plan select avg(1) from t1 group by c1;"""
    qt_basic_8 """explain shape plan select avg(1) from t1;"""

    /* ******** with one row "null" ******** */
    qt_basic_2_1 """explain shape plan select max(1) from t2 group by c2;"""
    qt_basic_2_2 """explain shape plan select max(1) from t2;"""
    qt_basic_2_3 """explain shape plan select min(1) from t2 group by c2;"""
    qt_basic_2_4 """explain shape plan select min(1) from t2;"""
    qt_basic_2_5 """explain shape plan select sum(1) from t2 group by c2;"""
    qt_basic_2_6 """explain shape plan select sum(1) from t2;"""
    qt_basic_2_7 """explain shape plan select avg(1) from t2 group by c2;"""
    qt_basic_2_8 """explain shape plan select avg(1) from t2;"""

    /* ******** with empty table ******** */
    qt_basic_3_1 """explain shape plan select max(1) from t3 group by c3;"""
    qt_basic_3_2 """explain shape plan select max(1) from t3;"""
    qt_basic_3_3 """explain shape plan select min(1) from t3 group by c3;"""
    qt_basic_3_4 """explain shape plan select min(1) from t3;"""
    qt_basic_3_5 """explain shape plan select sum(1) from t3 group by c3;"""
    qt_basic_3_6 """explain shape plan select sum(1) from t3;"""
    qt_basic_3_7 """explain shape plan select avg(1) from t3 group by c3;"""
    qt_basic_3_8 """explain shape plan select avg(1) from t3;"""

    /* ******** with different group table ******** */
    qt_basic_4_1 """explain shape plan select max(1) from t4 group by c4;"""
    qt_basic_4_2 """explain shape plan select max(1) from t4;"""
    qt_basic_4_3 """explain shape plan select min(1) from t4 group by c4;"""
    qt_basic_4_4 """explain shape plan select min(1) from t4;"""
    qt_basic_4_5 """explain shape plan select sum(1) from t4 group by c4;"""
    qt_basic_4_6 """explain shape plan select sum(1) from t4;"""
    qt_basic_4_7 """explain shape plan select avg(1) from t4 group by c4;"""
    qt_basic_4_8 """explain shape plan select avg(1) from t4;"""

    /* ******** with one row ******** */
    qt_basic_add_1 """explain shape plan select max(1) + 1 from t1 group by c1;"""
    qt_basic_add_2 """explain shape plan select max(1) + 1 from t1;"""
    qt_basic_add_3 """explain shape plan select min(1) + 1 from t1 group by c1;"""
    qt_basic_add_4 """explain shape plan select min(1) + 1 from t1;"""
    qt_basic_add_5 """explain shape plan select sum(1) + 1 from t1 group by c1;"""
    qt_basic_add_6 """explain shape plan select sum(1) + 1 from t1;"""
    qt_basic_add_7 """explain shape plan select avg(1) + 1 from t1 group by c1;"""
    qt_basic_add_8 """explain shape plan select avg(1) + 1 from t1;"""

    /* ******** with one row "null" ******** */
    qt_basic_add_2_1 """explain shape plan select max(1) + 1 from t2 group by c2;"""
    qt_basic_add_2_2 """explain shape plan select max(1) + 1 from t2;"""
    qt_basic_add_2_3 """explain shape plan select min(1) + 1 from t2 group by c2;"""
    qt_basic_add_2_4 """explain shape plan select min(1) + 1 from t2;"""
    qt_basic_add_2_5 """explain shape plan select sum(1) + 1 from t2 group by c2;"""
    qt_basic_add_2_6 """explain shape plan select sum(1) + 1 from t2;"""
    qt_basic_add_2_7 """explain shape plan select avg(1) + 1 from t2 group by c2;"""
    qt_basic_add_2_8 """explain shape plan select avg(1) + 1 from t2;"""

    /* ******** with empty table ******** */
    qt_basic_add_3_1 """explain shape plan select max(1) + 1 from t3 group by c3;"""
    qt_basic_add_3_2 """explain shape plan select max(1) + 1 from t3;"""
    qt_basic_add_3_3 """explain shape plan select min(1) + 1 from t3 group by c3;"""
    qt_basic_add_3_4 """explain shape plan select min(1) + 1 from t3;"""
    qt_basic_add_3_5 """explain shape plan select sum(1) + 1 from t3 group by c3;"""
    qt_basic_add_3_6 """explain shape plan select sum(1) + 1 from t3;"""
    qt_basic_add_3_7 """explain shape plan select avg(1) + 1 from t3 group by c3;"""
    qt_basic_add_3_8 """explain shape plan select avg(1) + 1 from t3;"""

    /* ******** with different group table ******** */
    qt_basic_add_4_1 """explain shape plan select max(1) + 1 from t4 group by c4;"""
    qt_basic_add_4_2 """explain shape plan select max(1) + 1 from t4;"""
    qt_basic_add_4_3 """explain shape plan select min(1) + 1 from t4 group by c4;"""
    qt_basic_add_4_4 """explain shape plan select min(1) + 1 from t4;"""
    qt_basic_add_4_5 """explain shape plan select sum(1) + 1 from t4 group by c4;"""
    qt_basic_add_4_6 """explain shape plan select sum(1) + 1 from t4;"""
    qt_basic_add_4_7 """explain shape plan select avg(1) + 1 from t4 group by c4;"""
    qt_basic_add_4_8 """explain shape plan select avg(1) + 1 from t4;"""

    /* ******** with one row ******** */
    qt_add_sum_1 """explain shape plan select max(1) + sum(2) from t1 group by c1;"""
    qt_add_sum_2 """explain shape plan select max(1) + sum(2) from t1;"""
    qt_add_sum_3 """explain shape plan select min(1) + sum(2) from t1 group by c1;"""
    qt_add_sum_4 """explain shape plan select min(1) + sum(2) from t1;"""
    qt_add_sum_5 """explain shape plan select sum(1) + sum(2) from t1 group by c1;"""
    qt_add_sum_6 """explain shape plan select sum(1) + sum(2) from t1;"""
    qt_add_sum_7 """explain shape plan select avg(1) + sum(2) from t1 group by c1;"""
    qt_add_sum_8 """explain shape plan select avg(1) + sum(2) from t1;"""

    /* ******** with one row "null" ******** */
    qt_add_sum_2_1 """explain shape plan select max(1) + sum(2) from t2 group by c2;"""
    qt_add_sum_2_2 """explain shape plan select max(1) + sum(2) from t2;"""
    qt_add_sum_2_3 """explain shape plan select min(1) + sum(2) from t2 group by c2;"""
    qt_add_sum_2_4 """explain shape plan select min(1) + sum(2) from t2;"""
    qt_add_sum_2_5 """explain shape plan select sum(1) + sum(2) from t2 group by c2;"""
    qt_add_sum_2_6 """explain shape plan select sum(1) + sum(2) from t2;"""
    qt_add_sum_2_7 """explain shape plan select avg(1) + sum(2) from t2 group by c2;"""
    qt_add_sum_2_8 """explain shape plan select avg(1) + sum(2) from t2;"""

    /* ******** with empty table ******** */
    qt_add_sum_3_1 """explain shape plan select max(1) + sum(2) from t3 group by c3;"""
    qt_add_sum_3_2 """explain shape plan select max(1) + sum(2) from t3;"""
    qt_add_sum_3_3 """explain shape plan select min(1) + sum(2) from t3 group by c3;"""
    qt_add_sum_3_4 """explain shape plan select min(1) + sum(2) from t3;"""
    qt_add_sum_3_5 """explain shape plan select sum(1) + sum(2) from t3 group by c3;"""
    qt_add_sum_3_6 """explain shape plan select sum(1) + sum(2) from t3;"""
    qt_add_sum_3_7 """explain shape plan select avg(1) + sum(2) from t3 group by c3;"""
    qt_add_sum_3_8 """explain shape plan select avg(1) + sum(2) from t3;"""

    /* ******** with different group table ******** */
    qt_add_sum_4_1 """explain shape plan select max(1) + sum(2) from t4 group by c4;"""
    qt_add_sum_4_2 """explain shape plan select max(1) + sum(2) from t4;"""
    qt_add_sum_4_3 """explain shape plan select min(1) + sum(2) from t4 group by c4;"""
    qt_add_sum_4_4 """explain shape plan select min(1) + sum(2) from t4;"""
    qt_add_sum_4_5 """explain shape plan select sum(1) + sum(2) from t4 group by c4;"""
    qt_add_sum_4_6 """explain shape plan select sum(1) + sum(2) from t4;"""
    qt_add_sum_4_7 """explain shape plan select avg(1) + sum(2) from t4 group by c4;"""
    qt_add_sum_4_8 """explain shape plan select avg(1) + sum(2) from t4;"""


    /* ******** Output ******** */

    /* ******** with one row ******** */
    order_qt_basic_1 """select max(1) from t1 group by c1;"""
    order_qt_basic_2 """select max(1) from t1;"""
    order_qt_basic_3 """select min(1) from t1 group by c1;"""
    order_qt_basic_4 """select min(1) from t1;"""
    order_qt_basic_5 """select sum(1) from t1 group by c1;"""
    order_qt_basic_6 """select sum(1) from t1;"""
    order_qt_basic_7 """select avg(1) from t1 group by c1;"""
    order_qt_basic_8 """select avg(1) from t1;"""

    /* ******** with one row "null" ******** */
    order_qt_basic_2_1 """select max(1) from t2 group by c2;"""
    order_qt_basic_2_2 """select max(1) from t2;"""
    order_qt_basic_2_3 """select min(1) from t2 group by c2;"""
    order_qt_basic_2_4 """select min(1) from t2;"""
    order_qt_basic_2_5 """select sum(1) from t2 group by c2;"""
    order_qt_basic_2_6 """select sum(1) from t2;"""
    order_qt_basic_2_7 """select avg(1) from t2 group by c2;"""
    order_qt_basic_2_8 """select avg(1) from t2;"""

    /* ******** with empty table ******** */
    order_qt_basic_3_1 """select max(1) from t3 group by c3;"""
    order_qt_basic_3_2 """select max(1) from t3;"""
    order_qt_basic_3_3 """select min(1) from t3 group by c3;"""
    order_qt_basic_3_4 """select min(1) from t3;"""
    order_qt_basic_3_5 """select sum(1) from t3 group by c3;"""
    order_qt_basic_3_6 """select sum(1) from t3;"""
    order_qt_basic_3_7 """select avg(1) from t3 group by c3;"""
    order_qt_basic_3_8 """select avg(1) from t3;"""

    /* ******** with different group table ******** */
    order_qt_basic_4_1 """select max(1) from t4 group by c4;"""
    order_qt_basic_4_2 """select max(1) from t4;"""
    order_qt_basic_4_3 """select min(1) from t4 group by c4;"""
    order_qt_basic_4_4 """select min(1) from t4;"""
    order_qt_basic_4_5 """select sum(1) from t4 group by c4;"""
    order_qt_basic_4_6 """select sum(1) from t4;"""
    order_qt_basic_4_7 """select avg(1) from t4 group by c4;"""
    order_qt_basic_4_8 """select avg(1) from t4;"""

    /* ******** with one row ******** */
    order_qt_basic_add_1 """select max(1) + 1 from t1 group by c1;"""
    order_qt_basic_add_2 """select max(1) + 1 from t1;"""
    order_qt_basic_add_3 """select min(1) + 1 from t1 group by c1;"""
    order_qt_basic_add_4 """select min(1) + 1 from t1;"""
    order_qt_basic_add_5 """select sum(1) + 1 from t1 group by c1;"""
    order_qt_basic_add_6 """select sum(1) + 1 from t1;"""
    order_qt_basic_add_7 """select avg(1) + 1 from t1 group by c1;"""
    order_qt_basic_add_8 """select avg(1) + 1 from t1;"""

    /* ******** with one row "null" ******** */
    order_qt_basic_add_2_1 """select max(1) + 1 from t2 group by c2;"""
    order_qt_basic_add_2_2 """select max(1) + 1 from t2;"""
    order_qt_basic_add_2_3 """select min(1) + 1 from t2 group by c2;"""
    order_qt_basic_add_2_4 """select min(1) + 1 from t2;"""
    order_qt_basic_add_2_5 """select sum(1) + 1 from t2 group by c2;"""
    order_qt_basic_add_2_6 """select sum(1) + 1 from t2;"""
    order_qt_basic_add_2_7 """select avg(1) + 1 from t2 group by c2;"""
    order_qt_basic_add_2_8 """select avg(1) + 1 from t2;"""

    /* ******** with empty table ******** */
    order_qt_basic_add_3_1 """select max(1) + 1 from t3 group by c3;"""
    order_qt_basic_add_3_2 """select max(1) + 1 from t3;"""
    order_qt_basic_add_3_3 """select min(1) + 1 from t3 group by c3;"""
    order_qt_basic_add_3_4 """select min(1) + 1 from t3;"""
    order_qt_basic_add_3_5 """select sum(1) + 1 from t3 group by c3;"""
    order_qt_basic_add_3_6 """select sum(1) + 1 from t3;"""
    order_qt_basic_add_3_7 """select avg(1) + 1 from t3 group by c3;"""
    order_qt_basic_add_3_8 """select avg(1) + 1 from t3;"""

    /* ******** with different group table ******** */
    order_qt_basic_add_4_1 """select max(1) + 1 from t4 group by c4;"""
    order_qt_basic_add_4_2 """select max(1) + 1 from t4;"""
    order_qt_basic_add_4_3 """select min(1) + 1 from t4 group by c4;"""
    order_qt_basic_add_4_4 """select min(1) + 1 from t4;"""
    order_qt_basic_add_4_5 """select sum(1) + 1 from t4 group by c4;"""
    order_qt_basic_add_4_6 """select sum(1) + 1 from t4;"""
    order_qt_basic_add_4_7 """select avg(1) + 1 from t4 group by c4;"""
    order_qt_basic_add_4_8 """select avg(1) + 1 from t4;"""

    /* ******** with one row ******** */
    order_qt_add_sum_1 """select max(1) + sum(2) from t1 group by c1;"""
    order_qt_add_sum_2 """select max(1) + sum(2) from t1;"""
    order_qt_add_sum_3 """select min(1) + sum(2) from t1 group by c1;"""
    order_qt_add_sum_4 """select min(1) + sum(2) from t1;"""
    order_qt_add_sum_5 """select sum(1) + sum(2) from t1 group by c1;"""
    order_qt_add_sum_6 """select sum(1) + sum(2) from t1;"""
    order_qt_add_sum_7 """select avg(1) + sum(2) from t1 group by c1;"""
    order_qt_add_sum_8 """select avg(1) + sum(2) from t1;"""

    /* ******** with one row "null" ******** */
    order_qt_add_sum_2_1 """select max(1) + sum(2) from t2 group by c2;"""
    order_qt_add_sum_2_2 """select max(1) + sum(2) from t2;"""
    order_qt_add_sum_2_3 """select min(1) + sum(2) from t2 group by c2;"""
    order_qt_add_sum_2_4 """select min(1) + sum(2) from t2;"""
    order_qt_add_sum_2_5 """select sum(1) + sum(2) from t2 group by c2;"""
    order_qt_add_sum_2_6 """select sum(1) + sum(2) from t2;"""
    order_qt_add_sum_2_7 """select avg(1) + sum(2) from t2 group by c2;"""
    order_qt_add_sum_2_8 """select avg(1) + sum(2) from t2;"""

    /* ******** with empty table ******** */
    order_qt_add_sum_3_1 """select max(1) + sum(2) from t3 group by c3;"""
    order_qt_add_sum_3_2 """select max(1) + sum(2) from t3;"""
    order_qt_add_sum_3_3 """select min(1) + sum(2) from t3 group by c3;"""
    order_qt_add_sum_3_4 """select min(1) + sum(2) from t3;"""
    order_qt_add_sum_3_5 """select sum(1) + sum(2) from t3 group by c3;"""
    order_qt_add_sum_3_6 """select sum(1) + sum(2) from t3;"""
    order_qt_add_sum_3_7 """select avg(1) + sum(2) from t3 group by c3;"""
    order_qt_add_sum_3_8 """select avg(1) + sum(2) from t3;"""

    /* ******** with different group table ******** */
    order_qt_add_sum_4_1 """select max(1) + sum(2) from t4 group by c4;"""
    order_qt_add_sum_4_2 """select max(1) + sum(2) from t4;"""
    order_qt_add_sum_4_3 """select min(1) + sum(2) from t4 group by c4;"""
    order_qt_add_sum_4_4 """select min(1) + sum(2) from t4;"""
    order_qt_add_sum_4_5 """select sum(1) + sum(2) from t4 group by c4;"""
    order_qt_add_sum_4_6 """select sum(1) + sum(2) from t4;"""
    order_qt_add_sum_4_7 """select avg(1) + sum(2) from t4 group by c4;"""
    order_qt_add_sum_4_8 """select avg(1) + sum(2) from t4;"""
}