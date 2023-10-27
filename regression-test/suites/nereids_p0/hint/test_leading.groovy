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

    sql '''
    alter table t1 modify column c1 set stats ('ndv'='1', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='1', 'max_value'='1', 'row_count'='10000')
    '''
    sql '''
    alter table t2 modify column c2 set stats ('ndv'='1', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='1', 'max_value'='1', 'row_count'='10000')
    '''
    sql '''
    alter table t3 modify column c3 set stats ('ndv'='1', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='1', 'max_value'='1', 'row_count'='10000')
    '''
    sql '''
    alter table t4 modify column c4 set stats ('ndv'='1', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='1', 'max_value'='1', 'row_count'='10000')
    '''
//// test inner join with all edge and vertax is complete and equal predicates
    qt_select1 """explain shape plan select /*+ leading(t2 t1) */ * from t1 join t2 on c1 = c2;"""
    qt_select2 """explain shape plan select /*+ leading(t1 t2) */ * from t1 join t2 on c1 = c2;"""
    qt_select3 """explain shape plan select /*+ leading(t1 t2 t3) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select4 """explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    qt_select5 """explain shape plan select /*+ leading(t1 {t2 t3} t4) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""
    qt_select6 """explain shape plan select /*+ leading({t1 t2} {t3 t4}) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""

    // test inner join with part of edge and need cross join
    qt_select7 """explain shape plan select /*+ leading({t1 t2} {t3 t4}) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3 join t4 on c3 = c4;"""

//// test outer join which can swap
    // (A leftjoin B on (Pab)) innerjoin C on (Pac) = (A innerjoin C on (Pac)) leftjoin B on (Pab)
    qt_select8 """explain shape plan select * from t1 left join t2 on c1 = c2 join t3 on c1 = c3;"""
    qt_select9 """explain shape plan select /*+ leading(t1 t3 t2) */ * from t1 left join t2 on c1 = c2 join t3 on c1 = c3;"""
    
    // (A leftjoin B on (Pab)) leftjoin C on (Pac) = (A leftjoin C on (Pac)) leftjoin B on (Pab)
    qt_select10 """explain shape plan select * from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;"""
    qt_select11 """explain shape plan select /*+ leading(t1 t3 t2) */ * from t1 left join t2 on c1 = c2 left join t3 on c1 = c3;"""

    // (A leftjoin B on (Pab)) leftjoin C on (Pbc) = A leftjoin (B leftjoin C on (Pbc)) on (Pab)
    qt_select12 """explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 left join t3 on c2 = c3;"""
    
//// test outer join which can not swap
    // A leftjoin (B join C on (Pbc)) on (Pab) != (A leftjoin B on (Pab)) join C on (Pbc) output should be unused when explain
    // this can be done because left join can be eliminated to inner join
    qt_select13 """explain shape plan select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 join t3 on c2 = c3;"""

    // this can not be done, expect not success but return right deep tree
    qt_select14 """explain shape plan select * from t1 left join (select * from t2 join t3 on c2 = c3) as tmp on c1 = c2;"""
    qt_select15 """explain shape plan select /*+ leading(t1 t2 t3) */ * from t1 left join (select * from t2 join t3 on c2 = c3) as tmp on c1 = c2;"""

//// test semi join
    qt_select16 """explain shape plan select * from t1 where c1 in (select c2 from t2);"""
    qt_select17 """explain shape plan select /*+ leading(t2 t1) */ * from t1 where c1 in (select c2 from t2);"""

//// test anti join
    qt_select18 """explain shape plan select * from t1 where exists (select 1 from t2);"""
    qt_select19 """explain shape plan select /*+ leading (t2 t1) */ * from t1 where exists (select 1 from t2);"""

//// test cte
    // inline cte, change join order of tables inside cte
    qt_select20 """explain shape plan with cte as (select * from t1 join t2 on c1 = c2) select * from cte, t2;"""
    qt_select21 """explain shape plan with cte as (select * from t1 join t2 on c1 = c2) select /*+ leading(t2 t1 t3) */ * from cte, t3;"""
    // outside cte
    // inside and outside together (after unnest subquery)

//// test syntax error and unsupported feature
    // not exist tables in leading: syntax error
    qt_select22 """explain shape plan select /*+ leading(t66 t1) */ * from t1 join t2 on c1 = c2;"""
    qt_select23 """explain shape plan select /*+ leading(t3 t1) */ * from t1 join t2 on c1 = c2;"""
    // subquery alias as leading table
    qt_select24 """explain shape plan with cte as (select * from t1 join t2 on c1 = c2) select /*+ leading(t2 cte t1) */ * from cte, t2;"""
    // do not have all tables inside hint
    qt_select25 """explain shape plan select /*+ leading(t1 t2) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""
    // duplicated table
    qt_select26 """explain shape plan select /*+ leading(t1 t1 t2 t3) */ * from t1 join t2 on c1 = c2 join t3 on c2 = c3;"""

//// test table alias
    qt_select27 """explain shape plan select /*+ leading(t1 t_2) */ * from t1 join t2 t_2 on c1 = c2;"""
    qt_select28 """explain shape plan select /*+ leading(t1 t2) */ * from t1 join t2 t_2 on c1 = c2;"""
    qt_select29 """explain shape plan select /*+ leading(t1 t_1) */ * from t1 join t1 t_1 on t1.c1 = t_1.c1;"""

    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
}
