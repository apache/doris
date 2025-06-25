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
    sql "drop table if exists test_cast_to_decimal64_18_0_from_double_overflow;"
    sql "create table test_cast_to_decimal64_18_0_from_double_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_double_overflow values (0, "1.1e+18"),(1, "-1.1e+18"),(2, "inf"),(3, "-inf"),(4, "nan"),
      (5, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_double_overflow_data_start_index = 0
    def test_cast_to_decimal64_18_0_from_double_overflow_data_end_index = 6
    for (int data_index = test_cast_to_decimal64_18_0_from_double_overflow_data_start_index; data_index < test_cast_to_decimal64_18_0_from_double_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_double_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_double_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_double_overflow;"
    sql "create table test_cast_to_decimal64_18_9_from_double_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_double_overflow values (6, "1099999999"),(7, "-1099999999"),(8, "inf"),(9, "-inf"),(10, "nan"),
      (11, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_double_overflow_data_start_index = 6
    def test_cast_to_decimal64_18_9_from_double_overflow_data_end_index = 12
    for (int data_index = test_cast_to_decimal64_18_9_from_double_overflow_data_start_index; data_index < test_cast_to_decimal64_18_9_from_double_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_double_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_double_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_double_overflow;"
    sql "create table test_cast_to_decimal64_18_17_from_double_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_double_overflow values (12, "10"),(13, "-10"),(14, "inf"),(15, "-inf"),(16, "nan"),
      (17, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_double_overflow_data_start_index = 12
    def test_cast_to_decimal64_18_17_from_double_overflow_data_end_index = 18
    for (int data_index = test_cast_to_decimal64_18_17_from_double_overflow_data_start_index; data_index < test_cast_to_decimal64_18_17_from_double_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_double_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_double_overflow order by 1;'

}