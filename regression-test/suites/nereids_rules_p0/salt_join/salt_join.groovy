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

suite("salt_join") {
//    sql "set disable_nereids_rules=JOIN_SKEW_ADD_SALT"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules='prune_empty_partition'"
    sql "set runtime_filter_mode=OFF"
    sql "drop table if exists test_skew9;"
    sql """create table test_skew9(a int,c varchar(100), b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into test_skew9 values(1,'abc',9),(1,'abc',1),(1,'def',2),(null,'def',2),(2,'abc',2),(3,'abc',4),(5,'abc',6),(2,'def',2),(5,'abc',null),(3,'abc',null)"""
    sql "drop table if exists test_skew10;"
    sql """create table test_skew10(a int,c varchar(100), b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into test_skew10 values(1,'abc',9),(null,'abc1',3),(1,'def',2),(2,'def1',7),(2,'def',2),(3,'abc',4),(5,'def',8),(2,'def',2),(5,'def',null),(3,'abc',null)"""
    qt_simple """
    select * from test_skew9 tl inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b order by 1,2,3,4,5,6;
    """
    qt_left_simple """
    select * from test_skew9 tl left join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b order by 1,2,3,4,5,6;
    """
    qt_right_simple """
    select * from test_skew9 tl right join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b order by 1,2,3,4,5,6;
    """
    qt_skew_value_null """
    select * from test_skew9 tl inner join [shuffle[skew(tl.b(1,2,null))]] test_skew10 tr on tl.b = tr.b order by 1,2,3,4,5,6;
    """
    qt_skew_value_only_null """
    select * from test_skew9 tl inner join [shuffle[skew(tl.b(null))]] test_skew10 tr on tl.b = tr.b order by 1,2,3,4,5,6;
    """
    qt_leading """
    select /*+leading(tl shuffle [skew(tl.b(1,2))] tr) */ * from test_skew9 tl join test_skew10 tr on tl.b=tr.b order by 1,2,3,4,5,6;
    """
    qt_has_other_equal_condition """
    select /*+leading(tl shuffle [skew(tl.b(1,2))] tr) */ * from test_skew9 tl join test_skew10 tr on tl.b=tr.b and tl.a=tr.a order by 1,2,3,4,5,6;
    """
    qt_has_other_unequal_condition """
    select /*+leading(tl shuffle [skew(tl.b(1,2))] tr) */ * from test_skew9 tl join test_skew10 tr on tl.b=tr.b and tl.a<tr.a order by 1,2,3,4,5,6;
    """
    qt_test_varchar_skew_value """
    select /*+leading(tl shuffle [skew(tl.c("abc","def"))] tr) */ * from test_skew9 tl join test_skew10 tr on tl.c=tr.c and tl.a=tr.a order by 1,2,3,4,5,6;
    """
    qt_test_multi_join """
    select * from test_skew9 tl inner join [shuffle[skew(tl.b(1,2,null))]] test_skew10 tr on tl.b = tr.b left join  [shuffle[skew(tl.b(1,2,null))]] test_skew10 tt on tl.b=tt.b
    order by 1,2,3,4,5,6,7,8,9;
    """
    qt_test_multi_join_right_join """
    select * from test_skew9 tl inner join [shuffle[skew(tl.b(1,2,null))]] test_skew10 tr on tl.b = tr.b right join  [shuffle[skew(tl.b(1,2,null))]] test_skew10 tt on tl.b=tt.b
    order by 1,2,3,4,5,6,7,8,9;
    """
    qt_test_left_join_right_join """
    select * from test_skew9 tl left join [shuffle[skew(tl.b(1,2,null))]] test_skew10 tr on tl.b = tr.b right join  [shuffle[skew(tl.b(1,2,null))]] test_skew10 tt on tl.b=tt.b
    order by 1,2,3,4,5,6,7,8,9;
    """
    qt_leading_multi_join """
    select /*+leading(tl shuffle [skew(tl.c("abc","def"))] tr shuffle[skew(tl.c("abc","def"))] tt) */ * from 
    test_skew9 tl join test_skew10 tr on tl.c=tr.c and tl.a=tr.a inner join test_skew10 tt on tl.c = tt.c
    order by 1,2,3,4,5,6,7,8,9;
    """
    qt_leading_multi_join_bracket """
    select /*+leading(tl shuffle [skew(tl.c("abc","def"))] {tr shuffle[skew(tr.c("abc","def"))] tt}) */ * from 
    test_skew9 tl join test_skew10 tr on tl.c=tr.c and tl.a=tr.a inner join test_skew10 tt on tr.c = tt.c
    order by 1,2,3,4,5,6,7,8,9;
    """

    // has other operator
    // agg
    qt_agg """
    select tl.a, count(*) as cnt 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    group by tl.a 
    order by 1,2;
    """
    // window
    qt_window """
    select tl.a, tl.b, tr.b, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    order by tl.a, tl.b, tr.b, rn;
    """
    // filter
    qt_filter """
    select * 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    where tl.a > 1 
    order by 1,2,3,4,5,6;
    """
    // agg_filter
    qt_agg_filter """
    select tl.a, count(*) as cnt 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    where tl.a > 1 
    group by tl.a 
    order by tl.a, cnt;
    """
    // window_filter
    qt_window_filter """
    select tl.a, tl.b, tr.b, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    where tl.a > 1 
    order by tl.a, tl.b,tr.b,rn;
    """
    // agg_window_filter
    qt_agg_window_filter """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """

    // agg_filter_window_filter_other_equal_condition
    qt_agg_filter_window_filter_other_equal_condition """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b and tl.a = tr.a 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """
    // agg_filter_window_filter_other_unequal_condition
    qt_agg_filter_window_filter_other_equal_condition """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b and tl.a = tr.a 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """
    qt_complex_subquery_agg_window_filter """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from (select * from test_skew9 where a > 1) tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """
    qt_multi_join_agg_window_filter """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from test_skew9 tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    left join [shuffle[skew(tl.b(1,2))]] test_skew10 tt on tl.b = tt.b 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """
    qt_multi_join_subquery_agg_window_filter """
    select tl.a, count(*) as cnt, 
           row_number() over (partition by tl.a order by tl.b) as rn 
    from (select * from test_skew9 where a > 1) tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    left join [shuffle[skew(tl.b(1,2))]] test_skew10 tt on tl.b = tt.b 
    where tl.a > 1 
    group by tl.a, tl.b 
    order by 1,2,3;
    """
    qt_subquery """
    select * 
    from (select * from test_skew9 where a > 1) tl 
    inner join [shuffle[skew(tl.b(1,2))]] test_skew10 tr on tl.b = tr.b 
    order by 1,2,3,4,5,6;
    """

    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""
    sql """drop table if exists t3;"""
    sql """drop table if exists t4;"""
    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""
    sql """create table t3 (c3 int, c33 int) distributed by hash(c3) buckets 3 properties('replication_num' = '1');"""
    sql """create table t4 (c4 int, c44 int) distributed by hash(c4) buckets 3 properties('replication_num' = '1');"""
    qt_shape_leading_inner_subquery """
    explain shape plan
    select count(*) from (select /*+leading(alias2 shuffle[skew(alias2.c2(1,2))] t1) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;
    """
    qt_shape_leading_inner_subquery_switch """
    explain shape plan
    select count(*) from (select /*+leading(t1 shuffle[skew(t1.c1(1,2))] alias2) */ c1, c11 from t1 join (select c2, c22 from t2 join t4 on c2 = c4) as alias2 on c1 = alias2.c2) as alias1 join t3 on alias1.c1 = t3.c3;
    """

    // test null safe equal
    sql "drop table if exists test_null_safe1"
    sql "drop table if exists test_null_safe2"
    sql """create table test_null_safe1(a int, b int) distributed by hash(a)  properties("replication_num"="1");"""
    sql """create table test_null_safe2(a int, b int) distributed by hash(a)  properties("replication_num"="1");"""
    sql "insert into test_null_safe1 values(1,null);"
    sql "insert into test_null_safe2 values(1,null);"
    qt_inner_skew_value "select * from test_null_safe1 t1 inner join[shuffle[skew(t1.b(1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_inner_skew_value_only_null "select * from test_null_safe1 t1 inner join[shuffle[skew(t1.b(null))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_inner_skew_value_null_and_value "select * from test_null_safe1 t1 inner join[shuffle[skew(t1.b(null,1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_left_skew_value "select * from test_null_safe1 t1 left join[shuffle[skew(t1.b(1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_left_skew_value_only_null "select * from test_null_safe1 t1 left join[shuffle[skew(t1.b(null))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_left_skew_value_null_and_value "select * from test_null_safe1 t1 left join[shuffle[skew(t1.b(null,1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_right_skew_value "select * from test_null_safe1 t1 right join[shuffle[skew(t1.b(1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_right_skew_value_only_null "select * from test_null_safe1 t1 right join[shuffle[skew(t1.b(null))]] test_null_safe2 t2 on t1.b<=>t2.b;"
    qt_right_skew_value_null_and_value "select * from test_null_safe1 t1 right join[shuffle[skew(t1.b(null,1,2))]] test_null_safe2 t2 on t1.b<=>t2.b;"

    // test by Mind Map
    sql "drop table if exists test_null_safe3"
    sql "drop table if exists test_null_safe4"
    sql """create table test_null_safe3(a int, b int) distributed by hash(a)  properties("replication_num"="1");"""
    sql """create table test_null_safe4(a int, b int) distributed by hash(a)  properties("replication_num"="1");"""
    sql "insert into test_null_safe3 values(1,null),(2,3),(2,4),(2,1),(3,1),(4,2),(3,2),(4,22),(4,25);"
    sql "insert into test_null_safe4 values(1,null),(2,3),(2,4),(2,1),(3,1),(4,2),(2,2),(3,5),(4,24);"

    //1.equal
    //1.1 inner join null, no need to rewrite
    qt_equal_inner_only_null """select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b=t2.b order by 1,2,3,4;"""
    //1.2 left join null, no need to expand
    qt_equal_left_only_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b=t2.b order by 1,2,3,4;"
    //1.3 right join null, no need to expand
    qt_equal_right_only_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(null))]] test_null_safe4 t2 on t1.b=t2.b order by 1,2,3,4;"
    //1.4 no null in skew values
    qt_equal_inner_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b  order by 1,2,3,4;"
    qt_equal_left_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b  order by 1,2,3,4;"
    qt_equal_right_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b  order by 1,2,3,4;"
    //1.5 have null in skew values
    qt_equal_inner_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b  order by 1,2,3,4;"
    qt_equal_left_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b order by 1,2,3,4;"
    qt_equal_right_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b order by 1,2,3,4;"

    //2.null safe equal
    //2.1 inner join only null, need rewrite
    qt_null_safe_equal_inner_only_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    //2.2 left join null only, need expand
    qt_null_safe_equal_left_only_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    //2.3 right join null only, need expand
    qt_null_safe_equal_right_only_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    //2.4 no null
    qt_null_safe_equal_inner_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    qt_null_safe_equal_left_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    qt_null_safe_equal_right_no_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    //2.5 have null in skew values
    qt_null_safe_equal_inner_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    qt_null_safe_equal_left_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"
    qt_null_safe_equal_right_other_and_null "select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b order by 1,2,3,4;"

    // test by Mind Map shape
    qt_equal_inner_only_null_shape """explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b=t2.b;"""
    qt_equal_left_only_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_right_only_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(null))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_inner_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_left_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_right_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_inner_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_left_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_equal_right_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2,null))]] test_null_safe4 t2 on t1.b=t2.b;"
    qt_null_safe_equal_inner_only_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_left_only_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_right_only_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_inner_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b"
    qt_null_safe_equal_left_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_right_no_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_inner_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 inner join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_left_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 left join[shuffle[skew(t1.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
    qt_null_safe_equal_right_other_and_null_shape "explain shape plan select t1.a,t1.b,t2.a,t2.b from test_null_safe3 t1 right join[shuffle[skew(t2.b(1,2,null))]] test_null_safe4 t2 on t1.b<=>t2.b;"
}