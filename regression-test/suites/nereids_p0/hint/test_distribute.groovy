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

suite("test_distribute") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_distribute'
    sql 'CREATE DATABASE IF NOT EXISTS test_distribute'
    sql 'use test_distribute'

    // setting planner to nereids
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set runtime_filter_mode=OFF'
    sql 'set be_number_for_test=1'
    sql "set parallel_pipeline_task_num=1"
    
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
        db "test_distribute"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't1.csv'
        time 10000
    }

    streamLoad {
        table "t2"
        db "test_distribute"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't2.csv'
        time 10000
    }

    streamLoad {
        table "t3"
        db "test_distribute"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't3.csv'
        time 10000
    }

    streamLoad {
        table "t4"
        db "test_distribute"
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
    qt_select2_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2;"""
    qt_select2_2 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2;"""
    qt_select2_3 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2;"""

    qt_select3_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select3_2 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""
    qt_select3_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select3_5 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select3_6 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""
    qt_select3_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select3_8 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""

    qt_select4_1 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select4_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select4_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select4_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select4_5 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""

    qt_select5_1 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 > c2;"""
    qt_select5_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 > c2;"""

    qt_select6_1 """explain shape plan select /*+ leading(t3 {t1 t2}) */ count(*) from t1 join [shuffle] t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""
    qt_select6_2 """explain shape plan select count(*) from t1 join t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""

    // (A leftjoin B on (Pab)) leftjoin C on (Pac) = (A leftjoin C on (Pac)) leftjoin B on (Pab)
    qt_select7_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c1 = c3;"""
    qt_select7_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c1 = c3;"""

    qt_select8_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c1 = c3 where c1 between 100 and 300;"""
    qt_select8_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c1 = c3 where c1 between 100 and 300;"""

    qt_select9_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c1 = c3 where c3 between 100 and 300;"""
    qt_select9_2 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c1 = c3 where c3 between 100 and 300;"""

    qt_select10_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""
    qt_select10_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""

    qt_select11_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c1 > c3 where c3 between 100 and 300;"""
    qt_select11_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c1 > c3 where c3 between 100 and 300;"""

    // (A leftjoin B on (Pab)) leftjoin C on (Pbc) = A leftjoin (B leftjoin C on (Pbc)) on (Pab)
    qt_select12_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select12_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""

    qt_select13_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c1 between 100 and 300;"""
    qt_select13_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c1 between 100 and 300;"""

    qt_select14_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c2 between 100 and 300;"""
    qt_select14_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c2 between 100 and 300;"""

    qt_select15_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c3 between 100 and 300;"""
    qt_select15_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c3 between 100 and 300;"""

    //// test outer join which can not swap
    // A leftjoin (B join C on (Pbc)) on (Pab) != (A leftjoin B on (Pab)) join C on (Pbc) output should be unused when explain
    // this can be done because left join can be eliminated to inner join
    qt_select16_1 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select16_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""

    // inner join + full outer join
    qt_select17_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select17_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select17_5 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select17_6 """explain shape plan select count(*) from t1 join t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""
    qt_select17_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""
    qt_select17_8 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select17_9 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""

    // inner join + left outer join
    qt_select18_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select18_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select18_5 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select18_6 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select18_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select18_8 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select18_9 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""

    // inner join + right outer join
    qt_select19_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select19_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select19_5 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select19_6 """explain shape plan select count(*) from t1 join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select19_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select19_8 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select19_9 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""

    // inner join + semi join
    qt_select20_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select20_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select20_5 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select20_6 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select20_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select20_8 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select20_9 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

    // inner join + anti join
    qt_select21_1 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_2 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_3 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select21_4 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select21_5 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select21_6 """explain shape plan select count(*) from t1 join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select21_7 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select21_8 """explain shape plan select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select21_9 """explain shape plan select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

    // left join + left join
    qt_select22_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select22_4 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select22_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select22_6 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select22_7 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select22_8 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select22_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""

    // left join + right join
    qt_select23_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select23_4 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select23_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select23_6 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select23_7 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select23_8 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select23_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""

    // left join + semi join
    qt_select24_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select24_4 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select24_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select24_6 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select24_7 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select24_8 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select24_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

    // left join + anti join
    qt_select25_1 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_2 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select25_4 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select25_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select25_6 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select25_7 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select25_8 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select25_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

    // right join + semi join
    qt_select26_1 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_2 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_3 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select26_4 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select26_5 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select26_6 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select26_7 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select26_8 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select26_9 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

    // right join + anti join
    qt_select27_1 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_2 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_3 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select27_4 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select27_5 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select27_6 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select27_7 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select27_8 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select27_9 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

    // semi join + anti join
    qt_select28_1 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_2 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_3 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select28_4 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select28_5 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select28_6 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""
    qt_select28_7 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""
    qt_select28_8 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select28_9 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""

    // left join + left join + inner join
    qt_select32_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select32_4 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select32_6 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select32_7 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_8 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select32_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select32_10 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select32_11 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select32_12 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select32_13 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""

// left join + right join + inner join
    qt_select33_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select33_4 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select33_6 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select33_7 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_8 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select33_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select33_10 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select33_11 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select33_12 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select33_13 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""

// left join + semi join + inner join
    qt_select34_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select34_4 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select34_6 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select34_7 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_8 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select34_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select34_10 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select34_11 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select34_12 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select34_13 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// left join + anti join + inner join
    qt_select35_1 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_2 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_3 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select35_4 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_5 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select35_6 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select35_7 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_8 """explain shape plan select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select35_9 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select35_10 """explain shape plan select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select35_11 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select35_12 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select35_13 """explain shape plan select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// right join + semi join + inner join
    qt_select36_1 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_2 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_3 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select36_4 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_5 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select36_6 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select36_7 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_8 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select36_9 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select36_10 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select36_11 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select36_12 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select36_13 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// right join + anti join + inner join
    qt_select37_1 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_2 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_3 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select37_4 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_5 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select37_6 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select37_7 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_8 """explain shape plan select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select37_9 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select37_10 """explain shape plan select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select37_11 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select37_12 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select37_13 """explain shape plan select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// semi join + anti join + inner join
    qt_select38_1 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_2 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_3 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select38_4 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_5 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select38_6 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select38_7 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_8 """explain shape plan select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select38_9 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select38_10 """explain shape plan select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select38_11 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select38_12 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select38_13 """explain shape plan select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""

// alias
// inner join + anti join
    qt_select41_1 """explain shape plan select count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_2 """explain shape plan select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_3 """explain shape plan select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_4 """explain shape plan select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select41_5 """explain shape plan select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_6 """explain shape plan select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select41_7 """explain shape plan select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select41_8 """explain shape plan select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_9 """explain shape plan select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_10 """explain shape plan select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select41_11 """explain shape plan select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select41_12 """explain shape plan select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select41_13 """explain shape plan select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + left join
    qt_select42_1 """explain shape plan select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_2 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_3 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_4 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select42_5 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_6 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select42_7 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select42_8 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_9 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_10 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select42_11 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select42_12 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select42_13 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + right join
    qt_select43_1 """explain shape plan select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_2 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_3 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_4 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select43_5 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_6 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select43_7 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select43_8 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_9 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_10 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select43_11 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select43_12 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select43_13 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + semi join
    qt_select44_1 """explain shape plan select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_2 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_3 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_4 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select44_5 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_6 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select44_7 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select44_8 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_9 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_10 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select44_11 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select44_12 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select44_13 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + anti join
    qt_select45_1 """explain shape plan select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_2 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_3 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_4 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select45_5 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_6 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select45_7 """explain shape plan select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select45_8 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_9 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_10 """explain shape plan select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select45_11 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select45_12 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select45_13 """explain shape plan select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// right join + semi join
    qt_select46_1 """explain shape plan select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_2 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_3 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_4 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select46_5 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_6 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select46_7 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select46_8 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_9 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_10 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select46_11 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select46_12 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select46_13 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// right join + anti join
    qt_select47_1 """explain shape plan select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_2 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_3 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_4 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select47_5 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_6 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select47_7 """explain shape plan select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select47_8 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_9 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_10 """explain shape plan select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select47_11 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select47_12 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select47_13 """explain shape plan select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// semi join + anti join
    qt_select48_1 """explain shape plan select count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_2 """explain shape plan select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_3 """explain shape plan select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_4 """explain shape plan select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select48_5 """explain shape plan select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_6 """explain shape plan select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select48_7 """explain shape plan select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select48_8 """explain shape plan select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_9 """explain shape plan select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_10 """explain shape plan select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select48_11 """explain shape plan select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select48_12 """explain shape plan select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select48_13 """explain shape plan select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

    //// test inner join with all edge and vertax is complete and equal predicates
    qt_select_res2_1 """select count(*) from t1 join t2 on c1 = c2;"""
    qt_select_res2_2 """select count(*) from t1 join [shuffle] t2 on c1 = c2;"""
    qt_select_res2_3 """select count(*) from t1 join [broadcast] t2 on c1 = c2;"""

    qt_select_res3_1 """select count(*) from t1 join t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select_res3_2 """select count(*) from t1 join [shuffle] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select_res3_3 """select count(*) from t1 join t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""
    qt_select_res3_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select_res3_5 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select_res3_6 """select count(*) from t1 join [shuffle] t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""
    qt_select_res3_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 join [shuffle] t3 on c2 = c3;"""
    qt_select_res3_8 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""

    qt_select_res4_1 """select count(*) from t1 join [shuffle] t2 on c1 = c2 join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res4_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res4_3 """select count(*) from t1 join t2 on c1 = c2 join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res4_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res4_5 """select count(*) from t1 join [broadcast] t2 on c1 = c2 join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""

    qt_select_res5_1 """select count(*) from t1 join [shuffle] t2 on c1 > c2;"""
    qt_select_res5_2 """select count(*) from t1 join [broadcast] t2 on c1 > c2;"""

    qt_select_res6_1 """select count(*) from t1 join [shuffle] t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""
    qt_select_res6_2 """select count(*) from t1 join t2 on c1 > c2 join t3 on c2 > c3 where c1 < 100;"""

// (A leftjoin B on (Pab)) leftjoin C on (Pac) = (A leftjoin C on (Pac)) leftjoin B on (Pab)
    qt_select_res7_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c1 = c3;"""
    qt_select_res7_2 """select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c1 = c3;"""

    qt_select_res8_1 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c1 = c3 where c1 between 100 and 300;"""
    qt_select_res8_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c1 = c3 where c1 between 100 and 300;"""

    qt_select_res9_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c1 = c3 where c3 between 100 and 300;"""
    qt_select_res9_2 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c1 = c3 where c3 between 100 and 300;"""

    qt_select_res10_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""
    qt_select_res10_2 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c1 = c3 where c2 between 100 and 300;"""

    qt_select_res11_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c1 > c3 where c3 between 100 and 300;"""
    qt_select_res11_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c1 > c3 where c3 between 100 and 300;"""

// (A leftjoin B on (Pab)) leftjoin C on (Pbc) = A leftjoin (B leftjoin C on (Pbc)) on (Pab)
    qt_select_res12_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res12_2 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""

    qt_select_res13_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c1 between 100 and 300;"""
    qt_select_res13_2 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c1 between 100 and 300;"""

    qt_select_res14_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c2 between 100 and 300;"""
    qt_select_res14_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c2 between 100 and 300;"""

    qt_select_res15_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 where c3 between 100 and 300;"""
    qt_select_res15_2 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where c3 between 100 and 300;"""

//// test outer join which can not swap
// A leftjoin (B join C on (Pbc)) on (Pab) != (A leftjoin B on (Pab)) join C on (Pbc) output should be unused when explain
// this can be done because left join can be eliminated to inner join
    qt_select_res16_1 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select_res16_2 """select count(*) from t1 left join t2 on c1 = c2 join [broadcast] t3 on c2 = c3;"""

// inner join + full outer join
    qt_select_res17_1 """select count(*) from t1 join t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select_res17_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select_res17_3 """select count(*) from t1 join t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select_res17_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select_res17_5 """select count(*) from t1 join [shuffle] t2 on c1 = c2 full join t3 on c2 = c3;"""
    qt_select_res17_6 """select count(*) from t1 join t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""
    qt_select_res17_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""
    qt_select_res17_8 """select count(*) from t1 join [shuffle] t2 on c1 = c2 full join [broadcast] t3 on c2 = c3;"""
    qt_select_res17_9 """select count(*) from t1 join [broadcast] t2 on c1 = c2 full join [shuffle] t3 on c2 = c3;"""

// inner join + left outer join
    qt_select_res18_1 """select count(*) from t1 join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res18_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res18_3 """select count(*) from t1 join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res18_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res18_5 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res18_6 """select count(*) from t1 join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select_res18_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select_res18_8 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res18_9 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""

// inner join + right outer join
    qt_select_res19_1 """select count(*) from t1 join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res19_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res19_3 """select count(*) from t1 join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res19_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res19_5 """select count(*) from t1 join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res19_6 """select count(*) from t1 join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select_res19_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select_res19_8 """select count(*) from t1 join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res19_9 """select count(*) from t1 join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""

// inner join + semi join
    qt_select_res20_1 """select count(*) from t1 join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res20_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res20_3 """select count(*) from t1 join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res20_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res20_5 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res20_6 """select count(*) from t1 join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res20_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res20_8 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res20_9 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

// inner join + anti join
    qt_select_res21_1 """select count(*) from t1 join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res21_2 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res21_3 """select count(*) from t1 join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res21_4 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res21_5 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res21_6 """select count(*) from t1 join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res21_7 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res21_8 """select count(*) from t1 join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res21_9 """select count(*) from t1 join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

// left join + left join
    qt_select_res22_1 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res22_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res22_3 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res22_4 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res22_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3;"""
    qt_select_res22_6 """select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select_res22_7 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""
    qt_select_res22_8 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3;"""
    qt_select_res22_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3;"""

// left join + right join
    qt_select_res23_1 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res23_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res23_3 """select count(*) from t1 left join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res23_4 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res23_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3;"""
    qt_select_res23_6 """select count(*) from t1 left join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select_res23_7 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""
    qt_select_res23_8 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3;"""
    qt_select_res23_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3;"""

// left join + semi join
    qt_select_res24_1 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res24_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res24_3 """select count(*) from t1 left join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res24_4 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res24_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res24_6 """select count(*) from t1 left join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res24_7 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res24_8 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res24_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

// left join + anti join
    qt_select_res25_1 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res25_2 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res25_3 """select count(*) from t1 left join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res25_4 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res25_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res25_6 """select count(*) from t1 left join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res25_7 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res25_8 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res25_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

// right join + semi join
    qt_select_res26_1 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res26_2 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res26_3 """select count(*) from t1 right join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res26_4 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res26_5 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3;"""
    qt_select_res26_6 """select count(*) from t1 right join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res26_7 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""
    qt_select_res26_8 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3;"""
    qt_select_res26_9 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3;"""

// right join + anti join
    qt_select_res27_1 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res27_2 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res27_3 """select count(*) from t1 right join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res27_4 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res27_5 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3;"""
    qt_select_res27_6 """select count(*) from t1 right join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res27_7 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""
    qt_select_res27_8 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3;"""
    qt_select_res27_9 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3;"""

// semi join + anti join
    qt_select_res28_1 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select_res28_2 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select_res28_3 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select_res28_4 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select_res28_5 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3;"""
    qt_select_res28_6 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""
    qt_select_res28_7 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""
    qt_select_res28_8 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3;"""
    qt_select_res28_9 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3;"""

// left join + left join + inner join
    qt_select_res32_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res32_2 """select count(*) from t1 left join t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res32_3 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res32_4 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res32_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res32_6 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res32_7 """select count(*) from t1 left join t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res32_8 """select count(*) from t1 left join t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res32_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res32_10 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res32_11 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res32_12 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res32_13 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""

// left join + right join + inner join
    qt_select_res33_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res33_2 """select count(*) from t1 left join t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res33_3 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res33_4 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res33_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res33_6 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c3 = c4;"""
    qt_select_res33_7 """select count(*) from t1 left join t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res33_8 """select count(*) from t1 left join t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res33_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res33_10 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select_res33_11 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res33_12 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""
    qt_select_res33_13 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 right join t3 on c2 = c3 join [broadcast] t4 on c3 = c4;"""

// left join + semi join + inner join
    qt_select_res34_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res34_2 """select count(*) from t1 left join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res34_3 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res34_4 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res34_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res34_6 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res34_7 """select count(*) from t1 left join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res34_8 """select count(*) from t1 left join t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res34_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res34_10 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res34_11 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res34_12 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res34_13 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// left join + anti join + inner join
    qt_select_res35_1 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res35_2 """select count(*) from t1 left join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res35_3 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res35_4 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res35_5 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res35_6 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res35_7 """select count(*) from t1 left join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res35_8 """select count(*) from t1 left join t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res35_9 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res35_10 """select count(*) from t1 left join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res35_11 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res35_12 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res35_13 """select count(*) from t1 left join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// right join + semi join + inner join
    qt_select_res36_1 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res36_2 """select count(*) from t1 right join t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res36_3 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res36_4 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res36_5 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res36_6 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res36_7 """select count(*) from t1 right join t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res36_8 """select count(*) from t1 right join t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res36_9 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res36_10 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res36_11 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res36_12 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res36_13 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left semi join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// right join + anti join + inner join
    qt_select_res37_1 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res37_2 """select count(*) from t1 right join t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res37_3 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res37_4 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res37_5 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res37_6 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res37_7 """select count(*) from t1 right join t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res37_8 """select count(*) from t1 right join t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res37_9 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res37_10 """select count(*) from t1 right join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join t4 on c1 = c4;"""
    qt_select_res37_11 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res37_12 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res37_13 """select count(*) from t1 right join [shuffle] t2 on c1 = c2 left anti join t3 on c2 = c3 join [broadcast] t4 on c1 = c4;"""

// semi join + anti join + inner join
    qt_select_res38_1 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select_res38_2 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select_res38_3 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res38_4 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select_res38_5 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res38_6 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 join [shuffle] t4 on c1 = c4;"""
    qt_select_res38_7 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select_res38_8 """select count(*) from t1 left semi join t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res38_9 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res38_10 """select count(*) from t1 left semi join [broadcast] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join t4 on c1 = c4;"""
    qt_select_res38_11 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res38_12 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""
    qt_select_res38_13 """select count(*) from t1 left semi join [shuffle] t2 on c1 = c2 left anti join t3 on c1 = c3 join [broadcast] t4 on c1 = c4;"""

// alias
// inner join + anti join
    qt_select_res41_1 """select count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_2 """select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_3 """select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_4 """select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res41_5 """select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_6 """select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res41_7 """select  count(*) from t1 join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res41_8 """select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_9 """select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_10 """select  count(*) from t1 join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res41_11 """select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res41_12 """select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res41_13 """select  count(*) from t1 join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + left join
    qt_select_res42_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_2 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_3 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_4 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res42_5 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_6 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res42_7 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res42_8 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_9 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_10 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res42_11 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res42_12 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res42_13 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + right join
    qt_select_res43_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_2 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_3 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_4 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res43_5 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_6 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res43_7 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 right join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res43_8 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_9 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_10 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res43_11 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res43_12 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res43_13 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 right join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + semi join
    qt_select_res44_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_2 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_3 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_4 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res44_5 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_6 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res44_7 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res44_8 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_9 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_10 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res44_11 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res44_12 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res44_13 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// left join + anti join
    qt_select_res45_1 """select count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_2 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_3 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_4 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res45_5 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_6 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res45_7 """select  count(*) from t1 left join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res45_8 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_9 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_10 """select  count(*) from t1 left join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res45_11 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res45_12 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res45_13 """select  count(*) from t1 left join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// right join + semi join
    qt_select_res46_1 """select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_2 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_3 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_4 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res46_5 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_6 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res46_7 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left semi join [broadcast] t3 on c2 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res46_8 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_9 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_10 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res46_11 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res46_12 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res46_13 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left semi join [shuffle] t3 on c2 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// right join + anti join
    qt_select_res47_1 """select count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_2 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_3 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_4 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res47_5 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_6 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res47_7 """select  count(*) from t1 right join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res47_8 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_9 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_10 """select  count(*) from t1 right join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res47_11 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res47_12 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res47_13 """select  count(*) from t1 right join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""

// semi join + anti join
    qt_select_res48_1 """select count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_2 """select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_3 """select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_4 """select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res48_5 """select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_6 """select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res48_7 """select  count(*) from t1 left semi join [broadcast] (select c2 from t2) as alias2 on c1 = c2 left anti join [broadcast] t3 on c1 = c3 where exists (select 1 from t3 join [broadcast] t4 on c3 = c4);"""
    qt_select_res48_8 """select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_9 """select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_10 """select  count(*) from t1 left semi join (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res48_11 """select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join t4 on c3 = c4);"""
    qt_select_res48_12 """select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""
    qt_select_res48_13 """select  count(*) from t1 left semi join [shuffle] (select c2 from t2) as alias2 on c1 = c2 left anti join [shuffle] t3 on c1 = c3 where exists (select 1 from t3 join [shuffle] t4 on c3 = c4);"""


    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
}
