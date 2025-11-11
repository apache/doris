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
        create view v_subtract select a-b from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_subtract "select * from v_subtract;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_divide;
        create view v_divide select a/b from t_decimalv3;
        set enable_decimal256=false;
    """
    qt_divide "select * from v_divide;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_mod;
        create view v_mod select mod(a,b) from t_decimalv3;
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
<<<<<<< HEAD
    qt_agg_expr "select col_sum from v_test_agg2;"

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
=======
    sql "explain verbose select col_sum from v_test_sum_expr;"
    qt_agg_expr_sum2 "select col_sum from v_test_sum_expr;"
    qt_agg_expr_avg2 "select col_avg from v_test_avg_expr;"
>>>>>>> 103db5d8db4 (add test case)
}