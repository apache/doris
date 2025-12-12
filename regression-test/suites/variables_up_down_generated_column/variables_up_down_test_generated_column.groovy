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

suite("variables_up_down_test_generated_column", "restart_fe") {

    sql "set enable_decimal256=false;"

    order_qt_c_scale_is_128_master_sql "select * from t_gen_col_multi_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_c_scale_is_256_master_sql "select * from t_gen_col_multi_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_divide_scale_master_sql "select * from t_gen_col_divide_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_divide_scale_256_master_sql "select * from t_gen_col_divide_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_add_sub_mod1_master_sql "select * from t_gen_col_add_sub_mod_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_add_sub_mod2_master_sql "select * from t_gen_col_add_sub_mod_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_nested_cols1_master_sql "select * from t_gen_col_nested;"
    sql "set enable_decimal256=true;"
    order_qt_nested_cols2_master_sql "select * from t_gen_col_nested;"

    sql "set enable_decimal256=false;"
    order_qt_complex_expr1_master_sql "select * from t_gen_col_complex;"
    sql "set enable_decimal256=true;"
    order_qt_complex_expr2_master_sql "select * from t_gen_col_complex;"

    sql "set enable_decimal256=false;"
    order_qt_gen_col_case1_master_sql "select * from t_gen_col_case;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_case2_master_sql "select * from t_gen_col_case;"

    sql "set enable_decimal256=false;"
    order_qt_gen_col_if1_master_sql "select * from t_gen_col_if;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_if2_master_sql "select * from t_gen_col_if;"

    sql "set enable_decimal256=false;"
    order_qt_gen_col_funcs1_master_sql "select * from t_gen_col_funcs;"
    sql "set enable_decimal256=true;"
    order_qt_gen_col_funcs2_master_sql "select * from t_gen_col_funcs;"

}
