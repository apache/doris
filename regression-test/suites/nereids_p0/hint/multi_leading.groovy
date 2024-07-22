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

suite("multi_leading") {

    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_multi_leading'
    sql 'CREATE DATABASE IF NOT EXISTS test_multi_leading'
    sql 'use test_multi_leading'
    sql "set enable_parallel_result_sink=false;"

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
        db "test_multi_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't1.csv'
        time 10000
    }

    streamLoad {
        table "t2"
        db "test_multi_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't2.csv'
        time 10000
    }

    streamLoad {
        table "t3"
        db "test_multi_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't3.csv'
        time 10000
    }

    streamLoad {
        table "t4"
        db "test_multi_leading"
        set 'column_separator', '|'
        set 'format', 'csv'
        file 't4.csv'
        time 10000
    }

    // test cte inline
    qt_sql1_1 """explain shape plan with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_2 """explain shape plan with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_3 """explain shape plan with cte as (select /*+ leading(t1 t2) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_4 """explain shape plan with cte as (select /*+ leading(t1 t2) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""

    qt_sql1_res_1 """with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_res_2 """with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_res_3 """with cte as (select /*+ leading(t1 t2) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""
    qt_sql1_res_4 """with cte as (select /*+ leading(t1 t2) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t1 cte)*/ count(*) from cte,t1 where cte.c1 = t1.c1 and t1.c1 > 300;"""

    // test subquery alone
    qt_sql2_1 """explain shape plan select count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_2 """explain shape plan select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_3 """explain shape plan select count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_4 """explain shape plan select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""

    qt_sql2_res_1 """select count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_res_2 """select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_res_3 """select count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql2_res_4 """select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3;"""

    // test subquery + cte
    qt_sql3_1 """explain shape plan with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;"""
    qt_sql3_2 """explain shape plan with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t3 alias1 cte) */ count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""
    qt_sql3_3 """explain shape plan with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""
    qt_sql3_4 """explain shape plan with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t3 alias1 cte) */ count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""

    qt_sql3_res_1 """with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""
    qt_sql3_res_2 """with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t3 alias1 cte) */ count(*) from (select c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""
    qt_sql3_res_3 """with cte as (select c11, c1 from t1 join t2 on c1 = c2) select count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""
    qt_sql3_res_4 """with cte as (select /*+ leading(t2 t1) */ c11, c1 from t1 join t2 on c1 = c2) select /*+ leading(t3 alias1 cte) */ count(*) from (select /*+ leading(t2 t1) */ c1, c11 from t1 join t2 on c1 = c2) as alias1 join t3 on alias1.c1 = t3.c3 join cte on alias1.c1 = cte.c11;;"""

    // test multi level subqueries
    qt_sql4_0 """explain shape plan select count(*) from (select c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_1 """explain shape plan select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_2 """explain shape plan select count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_3 """explain shape plan select count(*) from (select c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_4 """explain shape plan select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    explain {
        sql """shape plan select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
        contains("SyntaxError: leading(t4 t2) Msg:one query block can only have one leading clause")
    }
    explain {
        sql """shape plan select count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
        contains("SyntaxError: leading(t4 t2) Msg:one query block can only have one leading clause")
    }
    explain {
        sql """shape plan select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
        contains("UnUsed: leading(alias2 t1)")
    }

    qt_sql4_res_0 """select count(*) from (select c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_1 """select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_2 """select count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_3 """select count(*) from (select c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_4 """select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_5 """select /*+ leading(t3 alias1) */ count(*) from (select c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_6 """select count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""
    qt_sql4_res_7 """select /*+ leading(t3 alias1) */ count(*) from (select /*+ leading(alias2 t1) */ c1, c11 from t1 join (select /*+ leading(t4 t2) */ c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;"""

    // use cte in scalar query
    qt_sql5_1 """explain shape plan with  cte as (select c11, c1 from t1)  SELECT c1 FROM cte group by c1 having sum(cte.c11) > (select 0.05 * avg(t1.c11) from t1 join cte on t1.c1 = cte.c11 )"""
    qt_sql5_2 """explain shape plan with  cte as (select c11, c1 from t1)  SELECT c1 FROM cte group by c1 having sum(cte.c11) > (select /*+ leading(cte t1) */ 0.05 * avg(t1.c11) from t1 join cte on t1.c1 = cte.c11 )"""
}
