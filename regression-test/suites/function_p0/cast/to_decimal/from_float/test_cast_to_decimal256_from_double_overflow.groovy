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


suite("test_cast_to_decimal256_from_double_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_76_0_from_float64_overflow;"
    sql "create table test_cast_to_decimal256_76_0_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_float64_overflow values (0, "1e+76"),(1, "-1e+76"),(2, "1e+76"),(3, "-1e+76"),(4, "inf"),
      (5, "-inf"),(6, "nan"),(7, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_0_from_float64_overflow_data_start_index = 0
    def test_cast_to_decimal256_76_0_from_float64_overflow_data_end_index = 8
    for (int data_index = test_cast_to_decimal256_76_0_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal256_76_0_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_float64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_float64_overflow;"
    sql "create table test_cast_to_decimal256_76_38_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_float64_overflow values (8, "1e+38"),(9, "-1e+38"),(10, "1e+38"),(11, "-1e+38"),(12, "inf"),
      (13, "-inf"),(14, "nan"),(15, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_float64_overflow_data_start_index = 8
    def test_cast_to_decimal256_76_38_from_float64_overflow_data_end_index = 16
    for (int data_index = test_cast_to_decimal256_76_38_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal256_76_38_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_float64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_float64_overflow;"
    sql "create table test_cast_to_decimal256_76_75_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_float64_overflow values (16, "10"),(17, "-10"),(18, "11"),(19, "-11"),(20, "inf"),
      (21, "-inf"),(22, "nan"),(23, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_float64_overflow_data_start_index = 16
    def test_cast_to_decimal256_76_75_from_float64_overflow_data_end_index = 24
    for (int data_index = test_cast_to_decimal256_76_75_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal256_76_75_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_float64_overflow order by 1;'

}