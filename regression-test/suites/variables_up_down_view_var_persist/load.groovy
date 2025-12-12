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

suite("variables_up_down_load_view_var_persist", "restart_fe") {

    multi_sql """
        drop table if exists test_decimal_mul_overflow1;
        CREATE TABLE `test_decimal_mul_overflow1` (
            `f1` decimal(20,5) NULL,
            `f2` decimal(21,6) NULL
        )DISTRIBUTED BY HASH(f1)
        PROPERTIES("replication_num" = "1");
        insert into test_decimal_mul_overflow1 values(999999999999999.12345,999999999999999.123456);
    """

    multi_sql """
        drop table if exists t_decimalv3;
        create table t_decimalv3(a decimal(38,9),b decimal(38,10))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into t_decimalv3 values(1.012345678,1.0123456789);
    """

    multi_sql """
        drop table if exists t_decimalv3_for_compare;
        create table t_decimalv3_for_compare(a decimal(38,9),b decimal(38,10))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into t_decimalv3_for_compare values(1.012345678,1.0123456781);
    """

    sql "set enable_decimal256=true;"
    sql "drop view if exists v_test_decimal_mul_overflow1;"
    sql """create view v_test_decimal_mul_overflow1 as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    sql "set enable_decimal256=false;"

    // expect column multi scale is 11:999999999999998246906000000000.76833464320 instead of 8: 999999999999998246906000000000.76833464
    order_qt_scale_is_11_master_sql "select multi from v_test_decimal_mul_overflow1;"
    // expect column c1 scale is 8: 999999999999998246906000000000.76833464
    order_qt_scale_is_8_master_sql "select multi+1 c1 from v_test_decimal_mul_overflow1;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_plus;
        create view v_plus as select a+b from t_decimalv3;
        set enable_decimal256=false;
    """
    order_qt_plus_master_sql "select * from v_plus;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_subtract;
        create view v_subtract select a-b from t_decimalv3;
        set enable_decimal256=false;
    """
    order_qt_subtract_master_sql "select * from v_subtract;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_divide;
        create view v_divide select a/b from t_decimalv3;
        set enable_decimal256=false;
    """
    order_qt_divide_master_sql "select * from v_divide;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_mod;
        create view v_mod select mod(a,b) from t_decimalv3;
        set enable_decimal256=false;
    """
    order_qt_mod_master_sql "select * from v_mod;"

    multi_sql """drop view if exists test_lower_project;
    set enable_decimal256=true;
    create view test_lower_project as select t1.c1 as col1, t2.c1 as col2 from 
    (select f2*f1 as c1 from test_decimal_mul_overflow1) t1 inner join (select f1*f2 as c1 from test_decimal_mul_overflow1) t2
    where t1.c1=t2.c1;"""
    sql "set enable_decimal256=false;"
    order_qt_view_inner_expression_master_sql "select * from test_lower_project;"

    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view;
    create view test_nested_view as select * from test_lower_project;"""
    sql "set enable_decimal256=true;"
    order_qt_nest_view_master_sql "select * from test_nested_view;"

    multi_sql """set enable_decimal256=false;
    drop view if exists test_nested_view_expr;
    create view test_nested_view_expr as select col1+1 from test_lower_project;"""

    sql "set enable_decimal256=true;"
    order_qt_nest_view_expr_256_master_sql "select * from test_nested_view_expr;"
    sql "set enable_decimal256=false;"
    order_qt_nest_view_expr_128_master_sql "select * from test_nested_view_expr;"

    // agg
    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_sum;
    create view v_test_sum as select f1, sum(f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg;
    create view v_test_avg as select f1, avg(f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    order_qt_sum1_master_sql "select col_sum from v_test_sum;"
    order_qt_avg1_master_sql "select col_avg from v_test_avg;"
    sql "set enable_decimal256=false;"
    order_qt_sum2_master_sql "select col_sum from v_test_sum;"
    order_qt_avg2_master_sql "select col_avg from v_test_avg;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_test_sum_expr;
    create view v_test_sum_expr as select f1, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
    drop view if EXISTS v_test_avg_expr;
    create view v_test_avg_expr as select f1, avg(f1*f2) col_avg from test_decimal_mul_overflow1 group by f1;"""
    order_qt_agg_expr_sum1_master_sql "select col_sum from v_test_sum_expr;"
    order_qt_agg_expr_avg1_master_sql "select col_avg from v_test_avg_expr;"
    sql "set enable_decimal256=false;"
    order_qt_agg_expr_sum2_master_sql "select col_sum from v_test_sum_expr;"
    order_qt_agg_expr_avg2_master_sql "select col_avg from v_test_avg_expr;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_distinct;
        create view v_test_agg_distinct as select f1, sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=false;
    """
    order_qt_agg_distinct_master_sql "select col_sum from v_test_agg_distinct;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_func;
        create view v_test_agg_func as select f1, sum0(f1*f2) as col_sum0, avg(f1*f2) as col_avg,multi_distinct_sum(f1*f2) multi_sum, multi_distinct_sum0(f1*f2) multi_sum0 from test_decimal_mul_overflow1 group by f1;
        set enable_decimal256=false;
    """
    order_qt_agg_funcs_master_sql "select * from v_test_agg_func;"

    // two phase agg
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_two_phase;
        create view v_test_agg_two_phase as select f2, sum(f1*f2) col_sum from test_decimal_mul_overflow1 group by f2;
        set enable_decimal256=false;
    """
    order_qt_two_phase_agg_master_sql "select * from v_test_agg_two_phase;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_agg_without_gby;
        create view v_test_agg_without_gby as select sum(distinct f1*f2) col_sum from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    order_qt_agg_without_gby_master_sql "select * from v_test_agg_without_gby;"

    multi_sql """set enable_decimal256=true;
    drop view if EXISTS v_distinct_agg_rewrite;
    create view v_distinct_agg_rewrite as
    SELECT sum(f1*f2), count(distinct f1,f2) FROM test_decimal_mul_overflow1 GROUP BY f2;
    set enable_decimal256=false;"""
    order_qt_distinct_agg_rewrite_master_sql "select * from v_distinct_agg_rewrite;"

    // window
    multi_sql """
        set enable_decimal256=true;
        drop view if exists test_window_sum;
        create view test_window_sum as select sum(f1) over() from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    order_qt_window_master_sql "select * from test_window_sum;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists test_window_sum_expr;
        create view test_window_sum_expr as select sum(f1*f2) over() from test_decimal_mul_overflow1;
        set enable_decimal256=false;
    """
    order_qt_window_expr_master_sql "select * from test_window_sum_expr;"

    // test if
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_if;
        create view v_test_if as select a,b, if(b>1,b,a) col_if from t_decimalv3 ;
        set enable_decimal256=false;
    """
    order_qt_if_master_sql "select col_if from v_test_if;"

    // test casewhen
    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_casewhen;
        create view v_test_casewhen as select a,b, case when b>1 then b when b<1 then b else a end col_cw from t_decimalv3 ;
        set enable_decimal256=false;
    """
    order_qt_casewhen_master_sql "select col_cw from v_test_casewhen;"

    multi_sql """
        set enable_decimal256=true;
        drop view if EXISTS v_test_if_condition;
        create view v_test_if_condition as select a,b, coalesce(b, null, a) col_coa, greatest(a,b) col_gre, least(a,b-1) col_lea, nvl(b,a) col_nvl, nullif(b,a) col_nif from t_decimalv3 ;
        set enable_decimal256=false;
    """
    order_qt_if_condition_func_master_sql "select * from v_test_if_condition;"

//    // array func
//    multi_sql """
//    set enable_decimal256=true;
//    drop view if EXISTS v_test_array_func;
//    create view v_test_array_func as
//    select array_sum(array(a*b,a*b)), array_avg(array(a*b,a*b-1)),array_max(array(a*b,a*b+1)), array_min(array(a*b,a*b+1)),array_cum_sum(array(a*b,a*b)) from t_decimalv3;
//    set enable_decimal256=false;"""
//    order_qt_array_funcs_master_sql "select * from v_test_array_func;"
//
//    multi_sql """set enable_decimal256=true;
//    drop view if EXISTS v_test_array_product;
//    create view v_test_array_product as
//    select  array_product(array(f1,f2)) from test_decimal_mul_overflow1;
//    set enable_decimal256=false;"""
//    order_qt_array_product_master_sql "select * from v_test_array_product;"

    // compare expr
    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_compare;
        create view v_test_compare as select *,a=b equal, a <=> b nullsafe, a<b lessthan, b>a greaterthan from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    order_qt_compare_expr_master_sql "select * from v_test_compare;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_in;
        create view v_test_in as select a in (b,123) in_col from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    order_qt_int_master_sql "select * from v_test_in;"

    multi_sql """
        set enable_decimal256=true;
        drop view if exists v_test_union;
        create view v_test_union as select a from t_decimalv3_for_compare union all select b from t_decimalv3_for_compare;
        set enable_decimal256=false;
    """
    order_qt_union_master_sql "select * from v_test_union;"
}
