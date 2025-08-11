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


suite("test_cast_to_decimal64_from_double_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_18_0_from_double_invalid_0;"
    sql "create table test_cast_to_decimal_18_0_from_double_invalid_0(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_0_from_double_invalid_0 values (0, "1.1e+18"),(1, "-1.1e+18"),(2, "inf"),(3, "-inf"),(4, "nan"),(5, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_0_from_double_invalid_0_data_start_index = 0
    def test_cast_to_decimal_18_0_from_double_invalid_0_data_end_index = 6
    for (int data_index = test_cast_to_decimal_18_0_from_double_invalid_0_data_start_index; data_index < test_cast_to_decimal_18_0_from_double_invalid_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal_18_0_from_double_invalid_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal_18_0_from_double_invalid_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_9_from_double_invalid_1;"
    sql "create table test_cast_to_decimal_18_9_from_double_invalid_1(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_9_from_double_invalid_1 values (0, "1099999999"),(1, "-1099999999"),(2, "inf"),(3, "-inf"),(4, "nan"),(5, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_9_from_double_invalid_1_data_start_index = 0
    def test_cast_to_decimal_18_9_from_double_invalid_1_data_end_index = 6
    for (int data_index = test_cast_to_decimal_18_9_from_double_invalid_1_data_start_index; data_index < test_cast_to_decimal_18_9_from_double_invalid_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal_18_9_from_double_invalid_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal_18_9_from_double_invalid_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_double_invalid_2;"
    sql "create table test_cast_to_decimal_18_17_from_double_invalid_2(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_double_invalid_2 values (0, "10"),(1, "-10"),(2, "inf"),(3, "-inf"),(4, "nan"),(5, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_17_from_double_invalid_2_data_start_index = 0
    def test_cast_to_decimal_18_17_from_double_invalid_2_data_end_index = 6
    for (int data_index = test_cast_to_decimal_18_17_from_double_invalid_2_data_start_index; data_index < test_cast_to_decimal_18_17_from_double_invalid_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_double_invalid_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_double_invalid_2 order by 1;'

}