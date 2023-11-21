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

    // inner join + full outer join
    qt_select17_1 """select count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""

    // inner join + left outer join
    qt_select18_1 """select count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""

    // inner join + right outer join
    qt_select19_1 """select count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""

    // inner join + semi join
    qt_select20_1 """select count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""

    // inner join + anti join
    qt_select21_1 """select count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""

    // left join + left join
    qt_select22_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""

    // left join + right join
    qt_select23_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""

    // left join + semi join
    qt_select24_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""

    // left join + anti join
    qt_select25_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""

    // right join + semi join
    qt_select26_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""

    // right join + anti join
    qt_select27_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""

    // semi join + anti join
    qt_select28_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""

    // left join + two inner joins
    // right join + two inner joins
    // left join + right join + inner join
    // left join + semi join + inner join
    // left join + anti join + semi join
    // left join + full outer join + inner join
    //

    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
}
