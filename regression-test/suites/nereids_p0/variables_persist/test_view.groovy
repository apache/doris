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

suite("test_view") {
    // 这个结果是完整的结果，超出了38个精度
    // 999999999999998246906000000000.76833464320
    // 这个是截断到38个精度的结果
    // 999999999999998246906000000000.76833464

    // 打开enable256,创建视图;
    sql "set enable_decimal256=true;"
    sql "drop view if exists v_test_decimal_mul_overflow1;"
    sql """create view v_test_decimal_mul_overflow1 as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    // 关闭enable256,进行查询，预期结果是multi超出38个精度的结果, multi+1仍然是38个精度
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

    // view内部的表达式的测试
    multi_sql """drop view if exists test_lower_project;
    set enable_decimal256=true;
    create view test_lower_project as select t1.c1 as col1, t2.c1 as col2 from 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t1 inner join (select f1*f2 as c1 from test_decimal_mul_overflow1) t2
    where t1.c1=t2.c1;"""
    sql "set enable_decimal256=false;"
    qt_view_inner_expression "select * from test_lower_project;"

    // 测试嵌套视图
    // case1
    // 外层视图关闭256，但是是直接select *
    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view;
    create view test_nested_view as select * from test_lower_project;"""
    sql "set enable_decimal256=true;"
    // 查询嵌套视图，预期是256精度的
    qt_nest_view "select * from test_nested_view;"

    // case2
    // 外层视图关闭256，select col1+1 有表达式
    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view_expr;
    create view test_nested_view_expr as select col1+1 from test_lower_project;"""
    // 查询嵌套视图，预期是128精度的
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
    drop view if EXISTS v_test_array_product;
    create view v_test_array_product as
    select  array_product(array(f1,f2)) from test_decimal_mul_overflow1;
    set enable_decimal256=false;"""
    qt_array_product "select * from v_test_array_product;"

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

    // ========== 反向用例：关闭256创建视图，打开256查询 ==========

    // 关闭enable256,创建视图;
    sql "set enable_decimal256=false;"
    sql "drop view if exists v_test_decimal_mul_overflow1_reverse;"
    sql """create view v_test_decimal_mul_overflow1_reverse as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    // 打开enable256,进行查询
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

    // view内部的表达式的测试 - 反向
    multi_sql """drop view if exists test_lower_project_reverse;
    set enable_decimal256=false;
    create view test_lower_project_reverse as select t1.c1 as col1, t2.c1 as col2 from 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t1 inner join (select f1*f2 as c1 from test_decimal_mul_overflow1) t2
    where t1.c1=t2.c1;"""
    sql "set enable_decimal256=true;"
    qt_view_inner_expression_reverse "select * from test_lower_project_reverse;"

    // 测试嵌套视图 - 反向
    // case1
    // 外层视图打开256，但是是直接select *
    multi_sql """set enable_decimal256=true;
    drop view if exists test_nested_view_reverse;
    create view test_nested_view_reverse as select * from test_lower_project_reverse;"""
    sql "set enable_decimal256=false;"
    // 查询嵌套视图
    qt_nest_view_reverse "select * from test_nested_view_reverse;"

    // case2
    // 外层视图打开256，select col1+1 有表达式
    multi_sql """set enable_decimal256=true;
    drop view if exists test_nested_view_expr_reverse;
    create view test_nested_view_expr_reverse as select col1+1 from test_lower_project_reverse;"""
    // 查询嵌套视图
    sql "set enable_decimal256=false;"
    qt_nest_view_expr_128_reverse "select * from test_nested_view_expr_reverse;"
    sql "set enable_decimal256=true;"
    qt_nest_view_expr_256_reverse "select * from test_nested_view_expr_reverse;"

    // agg - 反向
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

    // two phase agg - 反向
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

    // window - 反向
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

    // test if - 反向
    multi_sql """
        set enable_decimal256=false;
        drop view if EXISTS v_test_if_reverse;
        create view v_test_if_reverse as select a,b, if(b>1,b,a) col_if from t_decimalv3 ;
        set enable_decimal256=true;
    """
    qt_if_reverse "select col_if from v_test_if_reverse;"

    // test casewhen - 反向
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

    // array func - 反向
    multi_sql """
    set enable_decimal256=false;
    drop view if EXISTS v_test_array_func_reverse;
    create view v_test_array_func_reverse as
    select array_sum(array(a*b,a*b)), array_avg(array(a*b,a*b-1)),array_max(array(a*b,a*b+1)), array_min(array(a*b,a*b+1)),array_cum_sum(array(a*b,a*b)) from t_decimalv3;
    set enable_decimal256=true;"""
    qt_array_funcs_reverse "select * from v_test_array_func_reverse;"

    multi_sql """set enable_decimal256=false;
    drop view if EXISTS v_test_array_product_reverse;
    create view v_test_array_product_reverse as
    select  array_product(array(f1,f2)) from test_decimal_mul_overflow1;
    set enable_decimal256=true;"""
    qt_array_product_reverse "select * from v_test_array_product_reverse;"

    // compare expr - 反向
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

    // ========== 测试多个变量同时持久化的场景 ==========
    
    // 场景1: 同时设置 enable_decimal256 和 decimal_overflow_scale
    multi_sql """
        set enable_decimal256=true;
        set decimal_overflow_scale=10;
        drop view if exists v_test_multi_vars;
        create view v_test_multi_vars as select f1*f2 multi from test_decimal_mul_overflow1;
        set enable_decimal256=false;
        set decimal_overflow_scale=6;
    """
    qt_multi_vars "select multi from v_test_multi_vars;"

    // 场景2: 多个变量在复杂表达式中
    multi_sql """
        set enable_decimal256=true;
        set decimal_overflow_scale=8;
        drop view if exists v_test_multi_vars_complex;
        create view v_test_multi_vars_complex as select (f1*f2)*f1 as expr1, f2/(f1*f2) as expr2 from test_decimal_mul_overflow1;
        set enable_decimal256=false;
        set decimal_overflow_scale=6;
    """
    qt_multi_vars_complex "select * from v_test_multi_vars_complex;"

    // 场景3: 多个变量在聚合函数中
    multi_sql """
        set enable_decimal256=true;
        set decimal_overflow_scale=12;
        drop view if exists v_test_multi_vars_agg;
        create view v_test_multi_vars_agg as select f1, sum(f1*f2) col_sum, avg(f1*f2) col_avg from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=false;
        set decimal_overflow_scale=6;
    """
    qt_multi_vars_agg "select * from v_test_multi_vars_agg;"

    // ========== 测试嵌套视图中多个guard的场景 ==========
    
    // 场景4: 内层视图使用 enable_decimal256=true，外层视图使用不同的变量组合
    multi_sql """drop view if exists v_inner_multi_vars;
    set enable_decimal256=true;
    set decimal_overflow_scale=10;
    create view v_inner_multi_vars as select f1*f2 as col1 from test_decimal_mul_overflow1;"""
    
    multi_sql """set enable_decimal256=false;
    set decimal_overflow_scale=8;
    drop view if exists v_outer_multi_vars;
    create view v_outer_multi_vars as select col1*2 as col2 from v_inner_multi_vars;"""
    
    sql "set enable_decimal256=true;"
    sql "set decimal_overflow_scale=6;"
    qt_nested_multi_vars "select * from v_outer_multi_vars;"

    // 场景5: 三层嵌套视图，每层使用不同的变量
    multi_sql """drop view if exists v_level1_multi;
    set enable_decimal256=true;
    set decimal_overflow_scale=10;
    create view v_level1_multi as select f1*f2 as col1 from test_decimal_mul_overflow1;"""
    
    multi_sql """set enable_decimal256=false;
    set decimal_overflow_scale=8;
    drop view if exists v_level2_multi;
    create view v_level2_multi as select col1+1 as col2 from v_level1_multi;"""
    
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=12;
    drop view if exists v_level3_multi;
    create view v_level3_multi as select col2*2 as col3 from v_level2_multi;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_three_level_nested "select * from v_level3_multi;"

    // 场景6: 嵌套视图中，外层视图对多个列进行表达式计算
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

    // ========== 测试连续多个guardExpr在同一个表达式中的场景 ==========
    
    // 场景7: 在JOIN中，左右表都使用不同的变量保护
    multi_sql """drop view if exists v_left_join;
    set enable_decimal256=true;
    set decimal_overflow_scale=10;
    create view v_left_join as 
    select t1.c1 as left_col, t2.c1 as right_col from 
    (select f1*f2 as c1 from test_decimal_mul_overflow1) t1 
    inner join 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t2 
    on t1.c1=t2.c1;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_join_multi_guards "select * from v_left_join;"

    // 场景8: UNION中，两个SELECT使用不同的变量保护
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_union_multi;
    create view v_union_multi as 
    select f1*f2 as col from test_decimal_mul_overflow1 
    union all 
    select f2*f1 as col from test_decimal_mul_overflow1;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_union_multi_guards "select * from v_union_multi order by 1;"

    // 场景9: 子查询中，外层和内层使用不同的变量
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_subquery_multi;
    create view v_subquery_multi as 
    select f1, (select sum(f1*f2) from test_decimal_mul_overflow1 t2 where t2.f1=t1.f1) as sub_sum 
    from test_decimal_mul_overflow1 t1;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_subquery_multi_guards "select * from v_subquery_multi;"

    // 场景10: CASE WHEN表达式中，不同分支可能需要不同的变量保护
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_case_multi;
    create view v_case_multi as 
    select a, b, 
    case when b>1 then a*b when b<1 then a/b else a+b end as case_col 
    from t_decimalv3;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_case_multi_guards "select * from v_case_multi;"

    // 场景11: 窗口函数中，窗口表达式和聚合表达式都需要变量保护
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_window_multi;
    create view v_window_multi as 
    select f1, f2, sum(f1*f2) over(partition by f1 order by f2) as win_sum,
    avg(f1*f2) over() as win_avg 
    from test_decimal_mul_overflow1;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_window_multi_guards "select * from v_window_multi;"

    // 场景12: 数组函数中，多个数组元素表达式都需要变量保护
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_array_multi;
    create view v_array_multi as 
    select array_sum(array(a*b, a*b+1, a*b-1)) as arr_sum,
    array_product(array(a*b, a*b)) as arr_prod 
    from t_decimalv3;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_array_multi_guards "select * from v_array_multi;"

    // ========== 反向测试：多个变量的反向场景 ==========
    
    // 场景13: 反向 - 关闭256和设置不同的overflow_scale创建视图，然后打开256查询
    multi_sql """set enable_decimal256=false;
    set decimal_overflow_scale=8;
    drop view if exists v_multi_vars_reverse;
    create view v_multi_vars_reverse as select f1*f2 multi from test_decimal_mul_overflow1;
    set enable_decimal256=true;
    set decimal_overflow_scale=6;"""
    qt_multi_vars_reverse "select multi from v_multi_vars_reverse;"

    // 场景14: 反向 - 嵌套视图反向测试
    multi_sql """drop view if exists v_inner_reverse;
    set enable_decimal256=false;
    set decimal_overflow_scale=8;
    create view v_inner_reverse as select f1*f2 as col1 from test_decimal_mul_overflow1;"""
    
    multi_sql """set enable_decimal256=true;
    set decimal_overflow_scale=10;
    drop view if exists v_outer_reverse;
    create view v_outer_reverse as select col1*2 as col2 from v_inner_reverse;"""
    
    sql "set enable_decimal256=false;"
    sql "set decimal_overflow_scale=6;"
    qt_nested_reverse_multi "select * from v_outer_reverse;"
}