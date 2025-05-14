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


suite("test_cast_to_decimal32_from_double_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_0_from_float64_overflow;"
    sql "create table test_cast_to_decimal32_9_0_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_float64_overflow values (0, "999999999.95"),(1, "-999999999.95"),(2, "1000000000.95"),(3, "-1000000000.95"),(4, "inf"),
      (5, "-inf"),(6, "nan"),(7, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_float64_overflow_data_start_index = 0
    def test_cast_to_decimal32_9_0_from_float64_overflow_data_end_index = 8
    for (int data_index = test_cast_to_decimal32_9_0_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal32_9_0_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_float64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_3_from_float64_overflow;"
    sql "create table test_cast_to_decimal32_9_3_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_3_from_float64_overflow values (8, "999999.9995"),(9, "-999999.9995"),(10, "1000000.9995"),(11, "-1000000.9995"),(12, "inf"),
      (13, "-inf"),(14, "nan"),(15, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_3_from_float64_overflow_data_start_index = 8
    def test_cast_to_decimal32_9_3_from_float64_overflow_data_end_index = 16
    for (int data_index = test_cast_to_decimal32_9_3_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal32_9_3_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 3)) from test_cast_to_decimal32_9_3_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(9, 3)) from test_cast_to_decimal32_9_3_from_float64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_float64_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_float64_overflow(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_float64_overflow values (16, "9.999999995"),(17, "-9.999999995"),(18, "10.999999995"),(19, "-10.999999995"),(20, "inf"),
      (21, "-inf"),(22, "nan"),(23, "-nan");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_float64_overflow_data_start_index = 16
    def test_cast_to_decimal32_9_8_from_float64_overflow_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_9_8_from_float64_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_float64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_float64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_float64_overflow order by 1;'

}