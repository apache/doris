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

suite("variables_up_down_load_alias_function", "restart_fe") {

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

    multi_sql """
        set enable_decimal256=true;
        drop function if exists multiply_plus_1(decimalv3(20,5), decimalv3(20,6));
        CREATE ALIAS FUNCTION multiply_plus_1(decimalv3(20,5), decimalv3(20,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),1);
        set enable_decimal256=false;
    """

    order_qt_multiply_add_master_sql "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_multiply_add_master_sql "select multiply_plus_1(f1,f2) from test_decimal_mul_overflow1;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_add(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_add(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS add(a,b);
        set enable_decimal256=false;
    """
    order_qt_add_master_sql "select func_add(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_add_master_sql "select func_add(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_subtract(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_subtract(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS subtract(a,b);
        set enable_decimal256=false;
    """
    order_qt_subtract_master_sql "select func_subtract(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_subtract_master_sql "select func_subtract(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_divide(decimalv3(38,18), decimalv3(38,18));
        CREATE ALIAS FUNCTION func_divide(decimalv3(38,18), decimalv3(38,18)) WITH PARAMETER(a,b) AS divide(a,b);
        set enable_decimal256=false;
    """
    order_qt_divide_master_sql "select func_divide(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_divide_master_sql "select func_divide(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_mod(decimalv3(38,9), decimalv3(38,10));
        CREATE ALIAS FUNCTION func_mod(decimalv3(38,9), decimalv3(38,10)) WITH PARAMETER(a,b) AS mod(a,b);
        set enable_decimal256=false;
    """
    order_qt_mod_master_sql "select func_mod(a,b) from t_decimalv3;"
    sql "set enable_decimal256=true;"
    order_qt_mod_master_sql "select func_mod(a,b) from t_decimalv3;"

    multi_sql """
        set enable_decimal256=true;
        drop function if exists func_nested(decimalv3(20,5), decimalv3(21,6));
        CREATE ALIAS FUNCTION func_nested(decimalv3(20,5), decimalv3(21,6)) WITH PARAMETER(a,b) AS add(multiply(a,b),multiply(a,2));
        set enable_decimal256=false;
    """
    order_qt_nested_master_sql "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
    sql "set enable_decimal256=true;"
    order_qt_nested_master_sql "select func_nested(f1,f2) from test_decimal_mul_overflow1;"
}
