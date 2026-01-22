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

suite("test_view_var_persist") {
    // Enable enable256 and create view;
    sql "set enable_decimal256=true;"
    sql "drop view if exists v_test_decimal_mul_overflow1;"
    sql """create view v_test_decimal_mul_overflow1 as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    // Disable enable256 and perform query, expected result: multi exceeds 38 precision, multi+1 is still 38 precision
    sql "set enable_decimal256=false;"

    // expect column multi scale is 11:999999999999998246906000000000.76833464320 instead of 8: 999999999999998246906000000000.76833464
    qt_scale_is_11 "select multi from v_test_decimal_mul_overflow1;"
    // expect column c1 scale is 8: 999999999999998246906000000000.76833464
    qt_scale_is_8 "select multi+1 c1 from v_test_decimal_mul_overflow1;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_plus;
        create view v_plus as select a+b from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_plus "select * from v_plus;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_subtract;
        create view v_subtract as select a-b from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_subtract "select * from v_subtract;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_divide;
        create view v_divide as select a/b from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_divide "select * from v_divide;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_mod;
        create view v_mod as select mod(a,b) from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_mod "select * from v_mod;"

    // Test expressions inside view
    multi_sql """drop view if exists test_lower_project;
    set enable_decimal256=true;
    create view test_lower_project as select t1.c1 as col1, t2.c1 as col2 from 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t1 inner join (select f1*f2 as c1 from test_decimal_mul_overflow1) t2
    where t1.c1=t2.c1;"""
    sql "set enable_decimal256=false;"
    qt_view_inner_expression "select * from test_lower_project;"

    // Test nested views
    // case1
    // Outer view disables 256, but is direct select *
    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view;
    create view test_nested_view as select * from test_lower_project;"""
    sql "set enable_decimal256=true;"
    // Query nested view, expected to be 256 precision
    qt_nest_view "select * from test_nested_view;"

    // case2
    // Outer view disables 256, select col1+1 has expression
    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view_expr;
    create view test_nested_view_expr as select col1+1 from test_lower_project;"""
    // Query nested view, expected to be 128 precision
    sql "set enable_decimal256=true;"
    qt_nest_view_expr_256 "select * from test_nested_view_expr;"
    sql "set enable_decimal256=false;"
    qt_nest_view_expr_128 "select * from test_nested_view_expr;"

    // agg
    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_sum;
    create view v_test_sum as select f1, sum(f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg;
    create view v_test_avg as select f1, avg(f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    qt_sum1 "select col_sum from v_test_sum;"
    qt_avg1 "select col_avg from v_test_avg;"
    sql "set enable_decimal256=false;"
    qt_sum2 "select col_sum from v_test_sum;"
    qt_avg2 "select col_avg from v_test_avg;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_sum_expr;
    create view v_test_sum_expr as select f1, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg_expr;
    create view v_test_avg_expr as select f1, avg(f1*f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    qt_agg_expr_sum1 "select col_sum from v_test_sum_expr;"
    qt_agg_expr_avg1 "select col_avg from v_test_avg_expr;"
    sql "set enable_decimal256=false;"
    qt_agg_expr_sum2 "select col_sum from v_test_sum_expr;"
    qt_agg_expr_avg2 "select col_avg from v_test_avg_expr;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_distinct;
        create view v_test_agg_distinct as select f1, sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=false;
    """
    qt_agg_distinct "select col_sum from v_test_agg_distinct;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_func;
        create view v_test_agg_func as select f1, sum0(f1*f2) as col_sum0, avg(f1*f2) as col_avg,multi_distinct_sum(f1*f2) multi_sum, multi_distinct_sum0(f1*f2) multi_sum0 from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=false;
    """
    qt_agg_funcs "select * from v_test_agg_func;"

    // two phase agg
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_two_phase;
        create view v_test_agg_two_phase as select f2, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f2;
        set enable_decimal256=false;
    """
    qt_two_phase_agg "select * from v_test_agg_two_phase;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_without_gby;
        create view v_test_agg_without_gby as select sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    qt_agg_without_gby "select * from v_test_agg_without_gby;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_distinct_agg_rewrite;
    create view v_distinct_agg_rewrite as
    SELECT sum(f1*f2), count(distinct f1,f2) FROM test_decimal_mul_overflow1 GROUP BY f2;
    set enable_decimal256=false;"""
    qt_distinct_agg_rewrite "select * from v_distinct_agg_rewrite;"

    // window
    multi_sql """
        set enable_decimal256=true;
        drop view if exists test_window_sum;
        create view test_window_sum as select sum(f1) over() from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    qt_window "select * from test_window_sum;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists test_window_sum_expr;
        create view test_window_sum_expr as select sum(f1*f2) over() from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    qt_window_expr "select * from test_window_sum_expr;"

    // test if
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_if;
        create view v_test_if as select a,b, if(b>1,b,a) col_if from t_decimalv3 ;
        set enable_decimal256=false;
    """
    qt_if "select col_if from v_test_if;"

    // test casewhen
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_casewhen;
        create view v_test_casewhen as select a,b, case when b>1 then b when b<1 then b else a end col_cw from t_decimalv3 ;
        set enable_decimal256=false;
    """
    qt_casewhen "select col_cw from v_test_casewhen;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_if_condition;
        create view v_test_if_condition as select a,b, coalesce(b, null, a) col_coa, greatest(a,b) col_gre, least(a,b-1) col_lea, nvl(b,a) col_nvl, nullif(b,a) col_nif from t_decimalv3 ;
        set enable_decimal256=false;
    """
    qt_if_condition_func "select * from v_test_if_condition;"

    // array func
    multi_sql """
    set enable_decimal256=true;
    drop view if EXISTS v_test_array_func;
    create view v_test_array_func as
    select array_sum(array(a*b,a*b)), array_avg(array(a*b,a*b-1)),array_max(array(a*b,a*b+1)), array_min(array(a*b,a*b+1)),array_cum_sum(array(a*b,a*b)) from t_decimalv3;
    set enable_decimal256=false;"""
    qt_array_funcs "select * from v_test_array_func;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_persist_test_array_product;
    create view v_persist_test_array_product as
    select  array_product(array(f1,f2)) from test_decimal_mul_overflow1;
    set enable_decimal256=false;"""
    qt_array_product "select * from v_persist_test_array_product;"

    // compare expr
    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_compare;
        create view v_test_compare as select *,a=b equal, a <=> b nullsafe, a<b lessthan, b>a greaterthan from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    qt_compare_expr "select * from v_test_compare;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_in;
        create view v_test_in as select a in (b,123) in_col from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    qt_int "select * from v_test_in;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_union;
        create view v_test_union as select a from t_decimalv3_for_compare union all select b from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    qt_union "select * from v_test_union order by 1;"

    // ========== Reverse test cases: disable 256 to create view, enable 256 to query ==========

    // Disable enable256 and create view;
    sql "set enable_decimal256=false;"
    sql "drop view if exists v_test_decimal_mul_overflow1_reverse;"
    sql """create view v_test_decimal_mul_overflow1_reverse as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    // Enable enable256 and perform query
    sql "set enable_decimal256=true;"
    qt_scale_is_8_reverse "select multi from v_test_decimal_mul_overflow1_reverse;"
    qt_scale_is_8_reverse "select multi+1 c1 from v_test_decimal_mul_overflow1_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_plus_reverse;
        create view v_plus_reverse as select a+b from t_decimalv3;
        set enable_decimal256=true;
    """
    qt_plus_reverse "select * from v_plus_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_subtract_reverse;
        create view v_subtract_reverse as select a-b from t_decimalv3;
        set enable_decimal256=true;
    """
    qt_subtract_reverse "select * from v_subtract_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_divide_reverse;
        create view v_divide_reverse as select a/b from t_decimalv3;
        set enable_decimal256=true;
    """
    qt_divide_reverse "select * from v_divide_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_mod_reverse;
        create view v_mod_reverse as select mod(a,b) from t_decimalv3;
        set enable_decimal256=true;
    """
    qt_mod_reverse "select * from v_mod_reverse;"

    // Test expressions inside view - reverse
    multi_sql """drop view if exists test_lower_project_reverse;
    set enable_decimal256=false;
    create view test_lower_project_reverse as select t1.c1 as col1, t2.c1 as col2 from 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t1 inner join (select f1*f2 as c1 from test_decimal_mul_overflow1) t2
    where t1.c1=t2.c1;"""
    sql "set enable_decimal256=true;"
    qt_view_inner_expression_reverse "select * from test_lower_project_reverse;"

    // Test nested views - reverse
    // case1
    // Outer view enables 256, but is direct select *
    multi_sql """set enable_decimal256=true;
    drop view if exists test_nested_view_reverse;
    create view test_nested_view_reverse as select * from test_lower_project_reverse;"""
    sql "set enable_decimal256=false;"
    // Query nested view
    qt_nest_view_reverse "select * from test_nested_view_reverse;"

    // case2
    // Outer view enables 256, select col1+1 has expression
    multi_sql """set enable_decimal256=true;
    drop view if exists test_nested_view_expr_reverse;
    create view test_nested_view_expr_reverse as select col1+1 from test_lower_project_reverse;"""
    // Query nested view
    sql "set enable_decimal256=false;"
    qt_nest_view_expr_128_reverse "select * from test_nested_view_expr_reverse;"
    sql "set enable_decimal256=true;"
    qt_nest_view_expr_256_reverse "select * from test_nested_view_expr_reverse;"

    // agg - reverse
    multi_sql """set enable_decimal256=false;
    drop view if EXISTS v_test_sum_reverse;
    create view v_test_sum_reverse as select f1, sum(f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg_reverse;
    create view v_test_avg_reverse as select f1, avg(f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    qt_sum1_reverse "select col_sum from v_test_sum_reverse;"
    qt_avg1_reverse "select col_avg from v_test_avg_reverse;"
    sql "set enable_decimal256=true;"
    qt_sum2_reverse "select col_sum from v_test_sum_reverse;"
    qt_avg2_reverse "select col_avg from v_test_avg_reverse;"

    multi_sql """set enable_decimal256=false;
    drop view if EXISTS v_test_sum_expr_reverse;
    create view v_test_sum_expr_reverse as select f1, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg_expr_reverse;
    create view v_test_avg_expr_reverse as select f1, avg(f1*f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    qt_agg_expr_sum1_reverse "select col_sum from v_test_sum_expr_reverse;"
    qt_agg_expr_avg1_reverse "select col_avg from v_test_avg_expr_reverse;"
    sql "set enable_decimal256=true;"
    qt_agg_expr_sum2_reverse "select col_sum from v_test_sum_expr_reverse;"
    qt_agg_expr_avg2_reverse "select col_avg from v_test_avg_expr_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_agg_distinct_reverse;
        create view v_test_agg_distinct_reverse as select f1, sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=true;
    """
    qt_agg_distinct_reverse "select col_sum from v_test_agg_distinct_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_agg_func_reverse;
        create view v_test_agg_func_reverse as select f1, sum0(f1*f2) as col_sum0, avg(f1*f2) as col_avg,multi_distinct_sum(f1*f2) multi_sum, multi_distinct_sum0(f1*f2) multi_sum0 from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=true;
    """
    qt_agg_funcs_reverse "select * from v_test_agg_func_reverse;"

    // two phase agg - reverse
    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_agg_two_phase_reverse;
        create view v_test_agg_two_phase_reverse as select f2, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f2;
        set enable_decimal256=true;
    """
    qt_two_phase_agg_reverse "select * from v_test_agg_two_phase_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_agg_without_gby_reverse;
        create view v_test_agg_without_gby_reverse as select sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1;
        set enable_decimal256=true;
    """
    qt_agg_without_gby_reverse "select * from v_test_agg_without_gby_reverse;"

    multi_sql """set enable_decimal256=false;
    drop view if EXISTS v_distinct_agg_rewrite_reverse;
    create view v_distinct_agg_rewrite_reverse as
    SELECT sum(f1*f2), count(distinct f1,f2) FROM test_decimal_mul_overflow1 GROUP BY f2;
    set enable_decimal256=true;"""
    qt_distinct_agg_rewrite_reverse "select * from v_distinct_agg_rewrite_reverse;"

    // window - reverse
    multi_sql """
        set enable_decimal256=false;
        drop view if exists test_window_sum_reverse;
        create view test_window_sum_reverse as select sum(f1) over() from test_decimal_mul_overflow1;
        set enable_decimal256=true;
    """
    qt_window_reverse "select * from test_window_sum_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if exists test_window_sum_expr_reverse;
        create view test_window_sum_expr_reverse as select sum(f1*f2) over() from test_decimal_mul_overflow1;
        set enable_decimal256=true;
    """
    qt_window_expr_reverse "select * from test_window_sum_expr_reverse;"

    // test if - reverse
    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_if_reverse;
        create view v_test_if_reverse as select a,b, if(b>1,b,a) col_if from t_decimalv3 ;
        set enable_decimal256=true;
    """
    qt_if_reverse "select col_if from v_test_if_reverse;"

    // test casewhen - reverse
    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_casewhen_reverse;
        create view v_test_casewhen_reverse as select a,b, case when b>1 then b when b<1 then b else a end col_cw from t_decimalv3 ;
        set enable_decimal256=true;
    """
    qt_casewhen_reverse "select col_cw from v_test_casewhen_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_if_condition_reverse;
        create view v_test_if_condition_reverse as select a,b, coalesce(b, null, a) col_coa, greatest(a,b) col_gre, least(a,b-1) col_lea, nvl(b,a) col_nvl, nullif(b,a) col_nif from t_decimalv3 ;
        set enable_decimal256=true;
    """
    qt_if_condition_func_reverse "select * from v_test_if_condition_reverse;"

    // array func - reverse
    multi_sql """
    set enable_decimal256=false;
    drop view if EXISTS v_test_array_func_reverse;
    create view v_test_array_func_reverse as
    select array_sum(array(a*b,a*b)), array_avg(array(a*b,a*b-1)),array_max(array(a*b,a*b+1)), array_min(array(a*b,a*b+1)),array_cum_sum(array(a*b,a*b)) from t_decimalv3;
    set enable_decimal256=true;"""
    qt_array_funcs_reverse "select * from v_test_array_func_reverse;"

    // TODO: return type of array_product with decimal256 should be decimal256, currently it is decimal128, need to fix
    /*
    multi_sql """set enable_decimal256=false;
    drop view if EXISTS v_test_array_product_reverse;
    create view v_test_array_product_reverse as
    select  array_product(array(f1,f2)) from test_decimal_mul_overflow1;
    set enable_decimal256=true;"""
    qt_array_product_reverse "select * from v_test_array_product_reverse;"
    */

    // compare expr - reverse
    multi_sql """
        set enable_decimal256=false;
        drop view if exists v_test_compare_reverse;
        create view v_test_compare_reverse as select *,a=b equal, a <=> b nullsafe, a<b lessthan, b>a greaterthan from t_decimalv3_for_compare;
        set enable_decimal256=true;
    """
    qt_compare_expr_reverse "select * from v_test_compare_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if exists v_test_in_reverse;
        create view v_test_in_reverse as select a in (b,123) in_col from t_decimalv3_for_compare;
        set enable_decimal256=true;
    """
    qt_int_reverse "select * from v_test_in_reverse;"

    multi_sql """
        set enable_decimal256=false;
        drop view if exists v_test_union_reverse;
        create view v_test_union_reverse as select a from t_decimalv3_for_compare union all select b from t_decimalv3_for_compare;
        set enable_decimal256=true;
    """
    qt_union_reverse "select * from v_test_union_reverse;"

    // ========== Test scenarios with multiple variables persisted simultaneously ==========
    
    // Scenario 1: Set both enable_decimal256 and decimal_overflow_scale
    multi_sql """
        set enable_decimal256=true;
        set decimal_overflow_scale=10;
        drop view if exists v_test_multi_vars;
        create view v_test_multi_vars as select f1*f2 multi from test_decimal_mul_overflow1;
        set enable_decimal256=false;
        set decimal_overflow_scale=6;
    """
    qt_multi_vars "select multi from v_test_multi_vars;"

    // Scenario 2: Multiple variables in complex expressions
    multi_sql """
        set enable_decimal256=true;
        set decimal_overflow_scale=8;
        drop view if exists v_test_multi_vars_complex;
        create view v_test_multi_vars_complex as select (f1*f2)*f1 as expr1, f2/(f1*f2) as expr2 from test_decimal_mul_overflow1;
        set enable_decimal256=false;
        set decimal_overflow_scale=6;
    """
    qt_multi_vars_complex "select * from v_test_multi_vars_complex;"


    // Scenario 6: In nested views, outer view performs expression calculations on multiple columns
    multi_sql """drop view if exists v_inner_base;
    set enable_decimal256=true;
    set decimal_overflow_scale=10;
    create view v_inner_base as select f1*f2 as col1, f1+f2 as col2 from test_decimal_mul_overflow1;"""

    multi_sql """set enable_decimal256=false;
    set decimal_overflow_scale=8;
    drop view if exists v_outer_expr;
    create view v_outer_expr as select col1-col2 as expr2, col1/col2 as expr3 from v_inner_base;"""

    sql "set enable_decimal256=true;"
    sql "set decimal_overflow_scale=6;"
    qt_nested_multi_expr "select * from v_outer_expr;"

    sql """set enable_decimal256=true;
    drop view if exists test_sum_sum_expr;
    create view test_sum_sum_expr as select f1,sum(f1*f2) c1 from test_decimal_mul_overflow1 group by f1,f2;
    set enable_decimal256=false;
    drop view if exists test_sum_sum_expr_outer;
    create view test_sum_sum_expr_outer as select sum(c1) from test_sum_sum_expr group by f1;
    set enable_decimal256=true;"""
    qt_merge_agg "select * from test_sum_sum_expr_outer;"


    sql """set enable_decimal256=true;
    drop view if exists test_sum_sum_expr;
    create view test_sum_sum_expr as select f1,sum(f1) c1 from test_decimal_mul_overflow1 group by f1,f2;
    set enable_decimal256=false;
    drop view if exists test_sum_sum_expr_outer;
    create view test_sum_sum_expr_outer as select sum(c1) from test_sum_sum_expr group by f1;
    set enable_decimal256=true;"""
    qt_merge_agg_expr "select * from test_sum_sum_expr_outer;"
    sql "set enable_decimal256=false;"
    qt_merge_agg_expr_outer_no_guard "select * from test_sum_sum_expr_outer;"
}