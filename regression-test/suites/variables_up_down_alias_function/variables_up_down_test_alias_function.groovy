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

suite("variables_up_down_test_alias_function", "restart_fe") {

    sql "set enable_decimal256=false;"
    order_qt_multiply_add_master_sql "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_multiply_add_master_sql "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"

    sql "set enable_decimal256=false;"
    order_qt_add_master_sql "select func_add(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_add_master_sql "select func_add(a,b) from t_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_subtract_master_sql "select func_subtract(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_subtract_master_sql "select func_subtract(a,b) from t_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_divide_master_sql "select func_divide(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_divide_master_sql "select func_divide(a,b) from t_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_mod_master_sql "select func_mod(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_mod_master_sql "select func_mod(a,b) from t_decimalv3;"

    sql "set enable_decimal256=false;"
    order_qt_nested_master_sql "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_nested_master_sql "select func_nested(f1,f2) from test_decimal_mul_overflow1;"

}
