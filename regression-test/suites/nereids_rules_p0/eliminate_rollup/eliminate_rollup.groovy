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

suite("eliminate_rollup") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql 'DROP DATABASE IF EXISTS test_eliminate_rollup'
    sql 'CREATE DATABASE IF NOT EXISTS test_eliminate_rollup'
    sql 'use test_eliminate_rollup'

    // create tables
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
    sql """drop table if exists t5;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""
    sql """create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');"""
    sql """create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');"""
    sql """create table t5 (c5 int not null, c55 int) distributed by hash(c5) buckets 3 properties('replication_num' = '1');"""

    sql "insert into t1 values (101, 101)"
    sql "insert into t2 values (null, null)"
    sql "insert into t4 values (102, 102)"
    sql "insert into t4 values (103, 103)"
    sql "insert into t5 values (105, 105)"

    /* ******** with one row ******** */
    qt_basic_1 """explain shape plan select c1 from t1 group by rollup(c1) having c1 > 1;"""

    /* ******** with one row "null" ******** */
    qt_basic_2 """explain shape plan select c2 from t2 group by rollup(c2) having c2 > 1;"""

    /* ******** with empty table ******** */
    qt_basic_3 """explain shape plan select c3 from t3 group by rollup(c3) having c3 > 1;"""

    /* ******** with different group table ******** */
    qt_basic_4 """explain shape plan select c4 from t4 group by rollup(c4) having c4 > 1 order by c4;"""

    /* ******** with one row ******** */
    qt_basic_1_1 """explain shape plan select c1 from t1 group by rollup(c1) order by c1;"""

    /* ******** with one row "null" ******** */
    qt_basic_1_2 """explain shape plan select c2 from t2 group by rollup(c2);"""

    /* ******** with empty table ******** */
    qt_basic_1_3 """explain shape plan select c3 from t3 group by rollup(c3);"""

    /* ******** with different group table ******** */
    qt_basic_1_4 """explain shape plan select c4 from t4 group by rollup(c4) order by c4;"""

    /* ******** with not nullable column ******** */
    qt_basic_1_5 """explain shape plan select c5 from t5 group by rollup(c5);"""
    

    /* ******** Output ******** */

    /* ******** with one row ******** */
    order_qt_basic_1 """select c1 from t1 group by rollup(c1) having c1 > 1;"""

    /* ******** with one row "null" ******** */
    order_qt_basic_2 """select c2 from t2 group by rollup(c2) having c2 > 1;"""

    /* ******** with empty table ******** */
    order_qt_basic_3 """select c3 from t3 group by rollup(c3) having c3 > 1;"""

    /* ******** with different group table ******** */
    order_qt_basic_4 """select c4 from t4 group by rollup(c4) having c4 > 1 order by c4;"""

    /* ******** with one row ******** */
    qt_order_basic_1_1 """select c1 from t1 group by rollup(c1) order by c1;"""

    /* ******** with one row "null" ******** */
    qt_order_basic_1_2 """select c2 from t2 group by rollup(c2);"""

    /* ******** with empty table ******** */
    qt_order_basic_1_3 """select c3 from t3 group by rollup(c3);"""

    /* ******** with different group table ******** */
    qt_order_basic_1_4 """select c4 from t4 group by rollup(c4) order by c4;"""

    /* ******** with not nullable column ******** */
    qt_order_basic_1_5 """select c5 from t5 group by rollup(c5) order by c5;"""
    
}
