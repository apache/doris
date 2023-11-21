/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_leading") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_leading'
    sql 'CREATE DATABASE IF NOT EXISTS test_leading'
    sql 'use test_leading'

    // setting planner to nereids
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // create tables
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""
    sql """create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');"""
    sql """create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');"""

    streamLoad {
        table "t1"
        db "test_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't1.csv'
        time 10000
    }

    streamLoad {
        table "t2"
        db "test_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't2.csv'
        time 10000
    }

    streamLoad {
        table "t3"
        db "test_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't3.csv'
        time 10000
    }

    streamLoad {
        table "t4"
        db "test_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't4.csv'
        time 10000
    }

//// check table count
    qt_select1_1 """select count(*) from t1;"""
    qt_select1_2 """select count(*) from t2;"""
    qt_select1_3 """select count(*) from t3;"""
    qt_select1_4 """select count(*) from t4;"""

//// test inner join with all edge and vertax is complete and equal predicates
    qt_select2_1 """select count(*) from t1 join t2 on c1 = c2;"""
    qt_select2_2 """select /*+ leading(t2 t1) */ count(*) from t1 join t2 on c1 = c2;"""
    qt_select2_3 """select /*+ leading(t1 t2) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select3_1 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_5 """select /*+ leading(t1 {t3 t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_6 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_7 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_8 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_9 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select4_1 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select4_2 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select4_3 """select /*+ leading({t1 t2} {t3 t4}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select4_4 """select /*+ leading(t1 {t2 t3 t4}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select4_5 """select /*+ leading(t1 {t2 {t3 t4}}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""

    qt_select5_1 """select count(*) from t1 join t2 on c1 > c2;"""
    qt_select5_2 """select /*+ leading(t2 t1) */ count(*) from t1 join t2 on c1 > c2;"""

    qt_select6_1 """select count(*) from t1 join t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""
    qt_select6_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""

   // (A leftjoin B on (Pab)) leftjoin C on (Pac) = (A leftjoin C on (Pac)) leftjoin B on (Pab)
    qt_select7_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;"""
    qt_select7_2 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;"""

    qt_select8_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c1 between 100 and 300;"""
    qt_select8_2 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c1 between 100 and 300;"""

    qt_select9_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c3 between 100 and 300;"""
    qt_select9_2 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c3 between 100 and 300;"""

    qt_select10_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""
    qt_select10_2 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""

    qt_select11_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 > c3 where c3 between 100 and 300;"""
    qt_select11_2 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 > c3 where c3 between 100 and 300;"""

    // (A leftjoin B on (Pab)) leftjoin C on (Pbc) = A leftjoin (B leftjoin C on (Pbc)) on (Pab)
    qt_select12_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select12_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""

    qt_select13_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 between 100 and 300;"""
    qt_select13_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 between 100 and 300;"""

    qt_select14_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c2 between 100 and 300;"""
    qt_select14_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c2 between 100 and 300;"""

    qt_select15_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c3 between 100 and 300;"""
    qt_select15_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c3 between 100 and 300;"""

    //// test outer join which can not swap
    // A leftjoin (B join C on (Pbc)) on (Pab) != (A leftjoin B on (Pab)) join C on (Pbc) output should be unused when explain
    // this can be done because left join can be eliminated to inner join
    qt_select16_1 """select count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select16_2 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3;"""


    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
}
