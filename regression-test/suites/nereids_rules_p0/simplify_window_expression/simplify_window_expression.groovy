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

suite("simplify_window_expression") {
    sql "set enable_parallel_result_sink=false;"
    sql """
          DROP TABLE IF EXISTS mal_test_simplify_window
         """

    sql """
         create table mal_test_simplify_window(a int, b int, c int) unique key(a,b) distributed by hash(a) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into mal_test_simplify_window values(6,null,6),(4,5,6),(1,1,4)
        ,(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8),(null,null,3);
      """

    sql "sync"

    qt_select_count_col """
        select a,count(a) over (partition by a,b) c1, count(a) over (partition by a,b order by a) c2 
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_rank """
        select a,rank() over (partition by a,b) c1, rank() over (partition by a,b order by b) c2 
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_dense_rank """
        select a,dense_rank() over (partition by a,b) c1, dense_rank() over (partition by a,b order by a,b) c1
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_row_number """
        select a,row_number() over (partition by a,b) c1, row_number() over (partition by a,b order by 1) c2 
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_first_value """
        select a,first_value(a) over (partition by a,b) c1, first_value(a) over (partition by a,b order by a) c2
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_last_value """
        select a,last_value(a) over (partition by a,b) c1,last_value(a) over (partition by a,b order by b) c2
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_min """
        select b,min(b) over (partition by a,b) c1, min(b) over (partition by a,b order by a,b) c2
         from mal_test_simplify_window order by 1,2,3;"""
    qt_select_max """
        select b,max(b) over (partition by a,b) c1,max(b) over (partition by a,b order by a,b) c2
         from mal_test_simplify_window order by 1,2,3;"""
    qt_select_sum """
        select a,sum(a) over (partition by a,b) c1, sum(a) over (partition by a,b order by a) c2
        from mal_test_simplify_window order by 1,2,3;"""
    qt_select_avg """
        select b, avg(b) over (partition by a,b) c1, avg(b) over (partition by a,b order by b) c2
        from mal_test_simplify_window order by 1,2,3;"""
    qt_more_than_pk """
        select b, avg(b) over (partition by a,b,c) c1, avg(b) over (partition by a,b,c order by b) c2
        from mal_test_simplify_window order by 1,2,3;"""

    qt_select_last_value_shape """explain shape plan 
        select a,last_value(a) over (partition by a,b) c1,last_value(a) over (partition by a,b order by b) c2
        from mal_test_simplify_window"""
    qt_select_min_shape """explain shape plan 
        select b,min(b) over (partition by a,b) c1, min(b) over (partition by a,b order by a,b) c2
         from mal_test_simplify_window"""
    qt_more_than_pk_shape """
        explain shape plan
        select b, avg(b) over (partition by a,b,c) c1, avg(b) over (partition by a,b,c order by b) c2
        from mal_test_simplify_window"""
    qt_select_count_star_col1 """
        select a,count() over (partition by a,b) c1, count() over (partition by a,b order by a) c2
        from mal_test_simplify_window order by 1,2,3;"""

    qt_select_upper_plan_use_all_rewrite """
        select b, c1 from (select b,avg(b) over (partition by a,b) c1 
        from mal_test_simplify_window) t order by 1,2"""
    qt_select_upper_plan_use_rewrite_and_not_rewrite """
        select b, c1, c2 from (select b,sum(b) over (partition by a,b) c1, max(b) over (partition by a order by a) c2
        from mal_test_simplify_window) t order by 1,2,3 """
    qt_select_upper_plan_use_all_not_rewrite """
        select b, c2 from (select b, max(b) over (partition by a order by a) c2
        from mal_test_simplify_window) t order by 1,2 """
    qt_select_upper_plan_use_all_rewrite_shape """
        explain shape plan select b, c1 from (select b,avg(b) over (partition by a,b) c1 
        from mal_test_simplify_window) t order by 1,2"""
    qt_select_upper_plan_use_rewrite_and_not_rewrite_shape """
        explain shape plan select b, c1, c2 from (select b,sum(b) over (partition by a,b) c1, max(b) over (partition by a order by a) c2
        from mal_test_simplify_window) t order by 1,2,3 """
    qt_select_upper_plan_use_all_not_rewrite_shape """
        explain shape plan select b, c2 from (select b, max(b) over (partition by a order by a) c2
        from mal_test_simplify_window) t order by 1,2 """

    qt_window_agg """
        select a, rank() over (partition by a order by sum(b) desc) as ranking
        from mal_test_simplify_window group by a order by 1,2;
    """
    qt_window_agg_shape """
        explain shape plan
        select a, rank() over (partition by a order by sum(b) desc) as ranking
        from mal_test_simplify_window group by a;
    """

    order_qt_check_output_type """
        select * from ( select a, rank() over (partition by a order by sum(b) desc) as ranking
        from mal_test_simplify_window group by a) t, (select 1 a) t2 where t.ranking = t2.a
    """
}