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

suite("test_agg_skew_hint") {
    sql "drop table if exists test_skew_hint"
    sql "create table test_skew_hint (a int, b int, c int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test_skew_hint values(1,2,3),(1,2,4),(1,3,4),(2,3,5),(2,4,5),(3,4,5),(3,5,6),(3,6,7),(3,7,8),(3,8,9),(3,10,11);"
    qt_hint "select a , count(distinct [skew] b)from test_skew_hint group by a order by 1,2"
    qt_hint_other_agg_func "select a , count(distinct [skew] b), count(a) from test_skew_hint group by a order by 1,2"
    qt_hint_other_agg_func_expr "select a , count(distinct [skew] b+1) from test_skew_hint group by a order by 1,2"
    qt_hint_same_column_with_group_by "select b , count(distinct [skew] b) from test_skew_hint group by b order by 1,2"
    qt_hint_same_column_with_group_by_expr "select b , count(distinct [skew] b+1) from test_skew_hint group by b order by 1,2"
    qt_hint_grouping "select a , count(distinct [skew] b)from test_skew_hint group by grouping sets((a),(c),()) order by 1,2"
    qt_hint_other_agg_func_grouping "select a , count(distinct [skew] b), count(a) from test_skew_hint group by grouping sets((a),(c),()) order by 1,2"
    qt_hint_other_agg_func_expr_grouping "select a , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(c),()) order by 1,2"
    qt_hint_same_column_with_group_by_grouping "select b , count(distinct [skew] b) from test_skew_hint group by grouping sets((a),(b),()) order by 1,2"
    qt_hint_same_column_with_group_by_expr_grouping "select b , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(b),()) order by 1,2"
    qt_hint_multi_column "select a , count(distinct [skew] b,c)from test_skew_hint group by a order by 1,2"
    sql "set agg_distinct_skew_bucket_num = 65536"
    qt_hint_variable "select a , count(distinct [skew] b)from test_skew_hint group by a order by 1,2"

    qt_hint_agg_join "select t1.a , count(distinct [skew] t2.b) from test_skew_hint t1 left join test_skew_hint t2 on t1.a=t2.a group by t1.a order by 1,2"
    qt_hint_agg_agg "select a , count(distinct [skew] b)from (select a, count(distinct [skew] b) as b from test_skew_hint group by a) t group by a order by 1,2"
    qt_hint_multi_count_distinct "select a, count(distinct [skew] b), count(distinct [skew] c) as b from test_skew_hint group by a order by 1,2"
}