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
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=1'
    sql 'set parallel_pipeline_task_num=1'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql "set ignore_shape_nodes='PhysicalProject'"
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set runtime_filter_mode=OFF'

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

    sql""" set BATCH_SIZE = 4064;"""

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

    // left join + left join + inner join
    qt_select32_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""

// left join + right join + inner join
    qt_select33_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""

// left join + semi join + inner join
    qt_select34_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""

// left join + anti join + inner join
    qt_select35_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""

// right join + semi join + inner join
    qt_select36_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""

// right join + anti join + inner join
    qt_select37_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""

// semi join + anti join + inner join
    qt_select38_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_2 """select /*+ leading(t1 t2 t3 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_3 """select /*+ leading(t1 {t2 t3} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_4 """select /*+ leading(t1 t3 t2 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_5 """select /*+ leading(t1 {t2 t2} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_6 """select /*+ leading(t2 t1 t3 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_7 """select /*+ leading(t2 {t1 t3} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_8 """select /*+ leading(t2 t3 t1 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_9 """select /*+ leading(t2 {t3 t1} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_10 """select /*+ leading(t3 t1 t2 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_11 """select /*+ leading(t3 {t1 t2} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_12 """select /*+ leading(t3 t2 t1 t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_13 """select /*+ leading(t3 {t2 t1} t4) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""

    // left join + left join + inner join
    qt_select42_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select42_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""

// left join + right join + inner join
    qt_select43_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select43_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""

// left join + semi join + inner join
    qt_select44_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select44_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""

// left join + anti join + inner join
    qt_select45_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select45_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""

// right join + semi join + inner join
    qt_select46_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select46_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""

// right join + anti join + inner join
    qt_select47_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select47_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""

// semi join + anti join + inner join
    qt_select48_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_2 """select /*+ leading(t1 t2 t4 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_3 """select /*+ leading(t1 {t2 t4} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_4 """select /*+ leading(t1 t4 t2 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_5 """select /*+ leading(t1 {t4 t2} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_6 """select /*+ leading(t2 t1 t4 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_7 """select /*+ leading(t2 {t1 t4} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_8 """select /*+ leading(t2 t4 t1 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_9 """select /*+ leading(t2 {t4 t1} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_10 """select /*+ leading(t4 t1 t2 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_11 """select /*+ leading(t4 {t1 t2} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_12 """select /*+ leading(t4 t2 t1 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select48_13 """select /*+ leading(t4 {t2 t1} t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""

    // left join + left join + inner join
    qt_select49_1 """select count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""
    qt_select49_2 """select /*+ leading(t1 t2 {t3 t4}) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""
    qt_select49_3 """select /*+ leading({t1 t2} t3 t4) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""
    qt_select49_4 """select /*+ leading({t1 t3} t2 t4) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""
    qt_select49_5 """select /*+ leading(t1 {t3 t4} t2) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""
    qt_select49_6 """select /*+ leading({t1 t3} t4 t2) */ count(*) from t1 left join t2 on c1 = c2 join t3 on c2 = c3 left join t4 on t3.c3 = t4.c4;"""

    // cte
    qt_select50_1 """with cte as (select * from t1 join t2 on c1 = c2) select count(*) from cte, t2;"""
    qt_select50_2 """with cte as (select /*+ leading(t2 t1) */* from t1 join t2 on c1 = c2) select /*+ leading(cte t3) */ count(*) from cte, t3;"""
    qt_select50_3 """with cte as (select /*+ leading(t2 t1) */* from t1 join t2 on c1 = c2) select count(*) from cte, t3;"""
    qt_select50_4 """with cte as (select * from t1 join t2 on c1 = c2) select /*+ leading(cte t3) */ count(*) from cte, t3;"""

    // in subquery +
// inner join + anti join
    qt_select51_1 """select count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select51_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// left join + left join
    qt_select52_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select52_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// left join + right join
    qt_select53_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select53_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// left join + semi join
    qt_select54_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select54_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// left join + anti join
    qt_select55_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select55_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// right join + semi join
    qt_select56_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select56_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// right join + anti join
    qt_select57_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""
    qt_select57_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where c1 in (select c4 from t4);"""

// semi join + anti join
    qt_select58_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""
    qt_select58_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where c1 in (select c4 from t4);"""

    // exists subquery +
// inner join + anti join
    qt_select61_1 """select count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select61_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + left join
    qt_select62_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select62_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + right join
    qt_select63_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select63_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + semi join
    qt_select64_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select64_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + anti join
    qt_select65_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select65_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// right join + semi join
    qt_select66_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select66_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// right join + anti join
    qt_select67_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select67_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// semi join + anti join
    qt_select68_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_3 """select /*+ leading(t1 {t2 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_5 """select /*+ leading(t1 {t2 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_6 """select /*+ leading(t2 t1 t3) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_7 """select /*+ leading(t2 {t1 t3}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_8 """select /*+ leading(t2 t3 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_9 """select /*+ leading(t2 {t3 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_12 """select /*+ leading(t3 t2 t1) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select68_13 """select /*+ leading(t3 {t2 t1}) */ count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

    // wrong table name
    qt_select70_1 """select /*+ leading(t1 t3) */ count(*) from t1 join t2 on c1 = c2;"""
    qt_select70_2 """select /*+ leading(t1 t5) */ count(*) from t1 join t2 on c1 = c2;"""

    // duplicate table name
    qt_select71_1 """select /*+ leading(t1 t1 t2) */ count(*) from t1 join t2 on c1 = c2;"""
    qt_select71_2 """select /*+ leading(t1 t2 t2) */ count(*) from t1 join t2 on c1 = c2;"""

    // different scope
    qt_select72_1 """select count(*) from t1 join t2 on c1 = c2 where c2 in (select /*+ leading(t4 t3) */ c3 from t3 join t4 on c3 = c4);"""
    qt_select72_2 """select /*+ leading(t1 t2) */ count(*) from t1 join t2 on c1 = c2 where c2 in (select c3 from t3 join t4 on c3 = c4);"""

    // multi leading hint
    qt_select73_1 """select /*+ leading(t1 t2) */ count(*) from t1 join t2 on c1 = c2 where c2 in (select /*+ leading(t4 t3) */ c3 from t3 join t4 on c3 = c4);"""

    // alias
// inner join + anti join
    qt_select81_1 """select count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select81_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + left join
    qt_select82_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select82_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + right join
    qt_select83_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select83_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + semi join
    qt_select84_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select84_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// left join + anti join
    qt_select85_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select85_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// right join + semi join
    qt_select86_1 """select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select86_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// right join + anti join
    qt_select87_1 """select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select87_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

// semi join + anti join
    qt_select88_1 """select count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_2 """select /*+ leading(t1 alias2 t3) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_3 """select /*+ leading(t1 {alias2 t3}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_4 """select /*+ leading(t1 t3 t2) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_5 """select /*+ leading(t1 {alias2 t2}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_6 """select /*+ leading(alias2 t1 t3) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_7 """select /*+ leading(alias2 {t1 t3}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_8 """select /*+ leading(alias2 t3 t1) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_9 """select /*+ leading(alias2 {t3 t1}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_10 """select /*+ leading(t3 t1 t2) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_11 """select /*+ leading(t3 {t1 t2}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_12 """select /*+ leading(t3 alias2 t1) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select88_13 """select /*+ leading(t3 {alias2 t1}) */ count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""

    // distribute hint + leading hint
// only distribute hint + single hint
    // used
    qt_select90_1 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2;"""
    // unused
    explain {
        sql """shape plan select count(*) from t1 right outer join [broadcast] t2 on c1 = c2;"""
        contains("UnUsed: [broadcast]_2")
    }

// only distribute hint + multi hints
    qt_select90_3 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
    explain {
        sql """shape plan select count(*) from t1 right outer join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
        contains("UnUsed: [broadcast]_2")
    }
    qt_select90_5 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 right outer join[shuffle] t3 on c2 = c3;"""
    explain {
        sql """shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 right outer join[broadcast] t3 on c2 = c3;"""
        contains("UnUsed: [broadcast]_3")
    }

// leading + distribute hint outside leading + single hint
    explain {
        sql """shape plan select /*+ leading(t1 t2 t3) */ count(*) from t1 join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
        contains("UnUsed: [broadcast]_2 [shuffle]_3")
    }
    explain {
        sql """shape plan select /*+ leading(t1 t2 t3) */ count(*) from t1 right outer join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
        contains("UnUsed: [broadcast]_2 [shuffle]_3")
    }
    explain {
        sql """shape plan select /*+ leading(t1 t2 t3) */ count(*) from t1 join [broadcast] t2 on c1 = c2 right outer join[shuffle] t3 on c2 = c3;"""
        contains("UnUsed: [broadcast]_2 [shuffle]_3")
    }
    explain {
        sql """shape plan select /*+ leading(t1 t2 t3) */ count(*) from t1 join [shuffle] t2 on c1 = c2 right outer join[broadcast] t3 on c2 = c3;"""
        contains("UnUsed: [shuffle]_2 [broadcast]_3")
    }

// leading + distribute hint inside leading + single hint
    // inner join
    qt_select92_1 """explain shape plan select /*+ leading(t1 shuffle t2 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select92_2 """explain shape plan select /*+ leading(t1 shuffle {t2 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select92_3 """explain shape plan select /*+ leading(t1 shuffle {t3 broadcast t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select92_4 """explain shape plan select /*+ leading(t2 shuffle t1 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select92_5 """explain shape plan select /*+ leading(t2 shuffle {t1 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select92_6 """explain shape plan select /*+ leading(t2 shuffle {t3 broadcast t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select93_1 """explain shape plan select /*+ leading(t1 t2 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select93_2 """explain shape plan select /*+ leading(t1 {t2 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select93_3 """explain shape plan select /*+ leading(t1 {t3 broadcast t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select93_4 """explain shape plan select /*+ leading(t2 t1 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select93_5 """explain shape plan select /*+ leading(t2 {t1 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select93_6 """explain shape plan select /*+ leading(t2 {t3 broadcast t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select94_2 """explain shape plan select /*+ leading(t1 shuffle t2 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select94_2 """explain shape plan select /*+ leading(t1 shuffle {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select94_2 """explain shape plan select /*+ leading(t1 shuffle {t3 t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select94_2 """explain shape plan select /*+ leading(t2 shuffle t1 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select94_2 """explain shape plan select /*+ leading(t2 shuffle {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select94_2 """explain shape plan select /*+ leading(t2 shuffle {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    // outer join
    qt_select95_1 """explain shape plan select /*+ leading(t1 broadcast t2 t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t1 broadcast {t2 t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 broadcast { t2 t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t1 broadcast {t3 t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 broadcast { t3 t2 })")
    }
    qt_select95_4 """explain shape plan select /*+ leading(t2 broadcast t1 t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t2 broadcast {t1 t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 broadcast { t1 t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t2 broadcast {t3 t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 broadcast { t3 t1 })")
    }
    explain {
        sql """shape plan select /*+ leading(t3 broadcast t1 t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t3 broadcast t1 t2)")
    }
    qt_select95_8 """explain shape plan select /*+ leading(t3 broadcast {t1 t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select95_9 """explain shape plan select /*+ leading(t3 broadcast {t2 t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select96_1 """explain shape plan select /*+ leading(t1 shuffle t2 broadcast t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t1 shuffle {t2 broadcast t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 shuffle { t2 broadcast t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t1 shuffle {t3 broadcast t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 shuffle { t3 broadcast t2 })")
    }
    qt_select96_4 """explain shape plan select /*+ leading(t2 shuffle t1 broadcast t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t2 shuffle {t1 broadcast t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 shuffle { t1 broadcast t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t2 shuffle {t3 broadcast t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 shuffle { t3 broadcast t1 })")
    }
    explain {
        sql """shape plan select /*+ leading(t3 shuffle t1 broadcast t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t3 shuffle t1 broadcast t2)")
    }
    qt_select96_8 """explain shape plan select /*+ leading(t3 shuffle {t1 broadcast t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select96_9 """explain shape plan select /*+ leading(t3 shuffle {t2 broadcast t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select97_1 """explain shape plan select /*+ leading(t1 broadcast t2 shuffle t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t1 broadcast {t2 shuffle t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 broadcast { t2 shuffle t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t1 broadcast {t3 shuffle t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t1 broadcast { t3 shuffle t2 })")
    }
    qt_select97_4 """explain shape plan select /*+ leading(t2 broadcast t1 shuffle t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    explain {
        sql """shape plan select /*+ leading(t2 broadcast {t1 shuffle t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 broadcast { t1 shuffle t3 })")
    }
    explain {
        sql """shape plan select /*+ leading(t2 broadcast {t3 shuffle t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t2 broadcast { t3 shuffle t1 })")
    }
    explain {
        sql """shape plan select /*+ leading(t3 broadcast t1 shuffle t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
        contains("UnUsed: leading(t3 broadcast t1 shuffle t2)")
    }
    qt_select97_8 """explain shape plan select /*+ leading(t3 broadcast {t1 shuffle t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select97_9 """explain shape plan select /*+ leading(t3 broadcast {t2 shuffle t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""

    // distribute hint + leading hint
// only distribute hint + single hint
    // used
    qt_select100_0 """select count(*) from t1 join t2 on c1 = c2;"""
    qt_select100_1 """select count(*) from t1 join [broadcast] t2 on c1 = c2;"""
    // unused
    qt_select100_2 """select count(*) from t1 right outer join [broadcast] t2 on c1 = c2;"""

// only distribute hint + multi hints
    qt_select100_3 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
    qt_select100_4 """select count(*) from t1 right outer join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
    qt_select100_5 """select count(*) from t1 join [broadcast] t2 on c1 = c2 right outer join[shuffle] t3 on c2 = c3;"""
    qt_select100_6 """select count(*) from t1 join [shuffle] t2 on c1 = c2 right outer join[broadcast] t3 on c2 = c3;"""

// leading + distribute hint outside leading + single hint
    qt_select101_0 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select101_1 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
    qt_select101_2 """select /*+ leading(t1 t2 t3) */ count(*) from t1 right outer join [broadcast] t2 on c1 = c2 join[shuffle] t3 on c2 = c3;"""
    qt_select101_3 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join [broadcast] t2 on c1 = c2 right outer join[shuffle] t3 on c2 = c3;"""
    qt_select101_4 """select /*+ leading(t1 t2 t3) */ count(*) from t1 join [shuffle] t2 on c1 = c2 right outer join[broadcast] t3 on c2 = c3;"""

// leading + distribute hint inside leading + single hint
    // inner join
    qt_select102_0 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_1 """select /*+ leading(t1 shuffle t2 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_2 """select /*+ leading(t1 shuffle {t2 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_3 """select /*+ leading(t1 shuffle {t3 broadcast t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_4 """select /*+ leading(t2 shuffle t1 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_5 """select /*+ leading(t2 shuffle {t1 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select102_6 """select /*+ leading(t2 shuffle {t3 broadcast t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select103_0 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_1 """select /*+ leading(t1 t2 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_2 """select /*+ leading(t1 {t2 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_3 """select /*+ leading(t1 {t3 broadcast t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_4 """select /*+ leading(t2 t1 broadcast t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_5 """select /*+ leading(t2 {t1 broadcast t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select103_6 """select /*+ leading(t2 {t3 broadcast t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select104_0 """select count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_1 """select /*+ leading(t1 shuffle t2 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_2 """select /*+ leading(t1 shuffle {t2 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_3 """select /*+ leading(t1 shuffle {t3 t2}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_4 """select /*+ leading(t2 shuffle t1 t3) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_5 """select /*+ leading(t2 shuffle {t1 t3}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select104_6 """select /*+ leading(t2 shuffle {t3 t1}) */ count(*) from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

    // outer join
    qt_select105_0 """select count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_1 """select /*+ leading(t1 broadcast t2 t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_2 """select /*+ leading(t1 broadcast {t2 t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_3 """select /*+ leading(t1 broadcast {t3 t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_4 """select /*+ leading(t2 broadcast t1 t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_5 """select /*+ leading(t2 broadcast {t1 t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_6 """select /*+ leading(t2 broadcast {t3 t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_7 """select /*+ leading(t3 broadcast t1 t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_8 """select /*+ leading(t3 broadcast {t1 t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select105_9 """select /*+ leading(t3 broadcast {t2 t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select106_0 """select count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_1 """select /*+ leading(t1 shuffle t2 broadcast t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_2 """select /*+ leading(t1 shuffle {t2 broadcast t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_3 """select /*+ leading(t1 shuffle {t3 broadcast t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_4 """select /*+ leading(t2 shuffle t1 broadcast t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_5 """select /*+ leading(t2 shuffle {t1 broadcast t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_6 """select /*+ leading(t2 shuffle {t3 broadcast t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_7 """select /*+ leading(t3 shuffle t1 broadcast t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_8 """select /*+ leading(t3 shuffle {t1 broadcast t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select106_9 """select /*+ leading(t3 shuffle {t2 broadcast t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""

    qt_select107_0 """select count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_1 """select /*+ leading(t1 broadcast t2 shuffle t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_2 """select /*+ leading(t1 broadcast {t2 shuffle t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_3 """select /*+ leading(t1 broadcast {t3 shuffle t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_4 """select /*+ leading(t2 broadcast t1 shuffle t3) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_5 """select /*+ leading(t2 broadcast {t1 shuffle t3}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_6 """select /*+ leading(t2 broadcast {t3 shuffle t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_7 """select /*+ leading(t3 broadcast t1 shuffle t2) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_8 """select /*+ leading(t3 broadcast {t1 shuffle t2}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select107_9 """select /*+ leading(t3 broadcast {t2 shuffle t1}) */ count(*) from t1 left outer join t2 on c1 = c2 join t3 on c2 = c3;"""
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
}
