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

suite("test_alias_function") {
    // 打开enable_decimal256创建函数
    multi_sql """
        set enable_decimal256=true;
        drop function if exists multiply_plus_1(decimalv3(20,5), decimalv3(20,6));
        CREATE ALIAS FUNCTION multiply_plus_1(decimalv3(20,5), decimalv3(20,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),1);
        set enable_decimal256=false;
    """
    // turn on/off 256, the result should be same
    qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    qt_multiply_add "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_add(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_add(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS add(a,b);
        set enable_decimal256=false;
    """
    qt_add "select func_add(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    qt_add "select func_add(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_subtract(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_subtract(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS subtract(a,b);
        set enable_decimal256=false;
    """
    qt_subtract "select func_subtract(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    qt_subtract "select func_subtract(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_divide(decimalv3(38,18), decimalv3(38,18));
        CREATE ALIAS FUNCTION func_divide(decimalv3(38,18), decimalv3(38,18)) WITH PARAMETER(a,b) AS divide(a,b);
        set enable_decimal256=false;
    """
    qt_divide "select func_divide(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    qt_divide "select func_divide(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_mod(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_mod(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS mod(a,b);
        set enable_decimal256=false;
    """
    qt_mod "select func_mod(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    qt_mod "select func_mod(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_nested(decimalv3(20,5), decimalv3(21,6));
        CREATE ALIAS FUNCTION func_nested(decimalv3(20,5), decimalv3(21,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),multiply(a,2));
        set enable_decimal256=false;
    """
    qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    qt_nested "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
}
