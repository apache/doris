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

suite("variables_up_down_test_view_var_persist", "restart_fe") {

    sql "set enable_decimal256=false;"

    // expect column multi scale is 11:999999999999998246906000000000.76833464320 instead of 8: 999999999999998246906000000000.76833464
    order_qt_scale_is_11_master_sql "select multi from v_test_decimal_mul_overflow1;"
    // expect column c1 scale is 8: 999999999999998246906000000000.76833464
    order_qt_scale_is_8_master_sql "select multi+1 c1 from v_test_decimal_mul_overflow1;"

    order_qt_plus_master_sql "select * from v_plus;"

    order_qt_subtract_master_sql "select * from v_subtract;"

    order_qt_divide_master_sql "select * from v_divide;"

    order_qt_mod_master_sql "select * from v_mod;"

    order_qt_view_inner_expression_master_sql "select * from test_lower_project;"

    order_qt_nest_view_master_sql "select * from test_nested_view;"

    sql "set enable_decimal256=true;"
    order_qt_nest_view_expr_256_master_sql "select * from test_nested_view_expr;"
    sql "set enable_decimal256=false;"
    order_qt_nest_view_expr_128_master_sql "select * from test_nested_view_expr;"

    // agg
    sql "set enable_decimal256=true;"
    order_qt_sum1_master_sql "select col_sum from v_test_sum;"
    order_qt_avg1_master_sql "select col_avg from v_test_avg;"
    sql "set enable_decimal256=false;"
    order_qt_sum2_master_sql "select col_sum from v_test_sum;"
    order_qt_avg2_master_sql "select col_avg from v_test_avg;"

    sql "set enable_decimal256=true;"
    order_qt_agg_expr_sum1_master_sql "select col_sum from v_test_sum_expr;"
    order_qt_agg_expr_avg1_master_sql "select col_avg from v_test_avg_expr;"
    sql "set enable_decimal256=false;"
    order_qt_agg_expr_sum2_master_sql "select col_sum from v_test_sum_expr;"
    order_qt_agg_expr_avg2_master_sql "select col_avg from v_test_avg_expr;"

    order_qt_agg_distinct_master_sql "select col_sum from v_test_agg_distinct;"

    order_qt_agg_funcs_master_sql "select * from v_test_agg_func;"

    // two phase agg
    order_qt_two_phase_agg_master_sql "select * from v_test_agg_two_phase;"

    order_qt_agg_without_gby_master_sql "select * from v_test_agg_without_gby;"

    order_qt_distinct_agg_rewrite_master_sql "select * from v_distinct_agg_rewrite;"

    // window
    order_qt_window_master_sql "select * from test_window_sum;"

    order_qt_window_expr_master_sql "select * from test_window_sum_expr;"

    // test if
    order_qt_if_master_sql "select col_if from v_test_if;"

    // test casewhen
    order_qt_casewhen_master_sql "select col_cw from v_test_casewhen;"

    order_qt_if_condition_func_master_sql "select * from v_test_if_condition;"

    // array func
//    order_qt_array_funcs_master_sql "select * from v_test_array_func;"
//
//    order_qt_array_product_master_sql "select * from v_test_array_product;"

    // compare expr
    order_qt_compare_expr_master_sql "select * from v_test_compare;"

    order_qt_int_master_sql "select * from v_test_in;"

    order_qt_union_master_sql "select * from v_test_union;"

}
