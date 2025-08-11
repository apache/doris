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


suite("test_cast_to_int_from_decimalv2_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_decimalv2_overflow_2;"
    sql "create table test_cast_to_int_from_decimalv2_overflow_2(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_overflow_2 values (0, "2147483648.000000000"),(1, "2147483648.000000001"),(2, "2147483648.000000009"),(3, "2147483648.999999999"),(4, "2147483648.999999998"),(5, "2147483648.099999999"),(6, "2147483648.900000000"),(7, "2147483648.900000001"),(8, "-2147483649.000000000"),(9, "-2147483649.000000001"),(10, "-2147483649.000000009"),(11, "-2147483649.999999999"),(12, "-2147483649.999999998"),(13, "-2147483649.099999999"),(14, "-2147483649.900000000"),(15, "-2147483649.900000001"),(16, "999999999999999999.000000000"),(17, "999999999999999999.000000001"),(18, "999999999999999999.000000009"),(19, "999999999999999999.999999999"),
      (20, "999999999999999999.999999998"),(21, "999999999999999999.099999999"),(22, "999999999999999999.900000000"),(23, "999999999999999999.900000001"),(24, "-999999999999999999.000000000"),(25, "-999999999999999999.000000001"),(26, "-999999999999999999.000000009"),(27, "-999999999999999999.999999999"),(28, "-999999999999999999.999999998"),(29, "-999999999999999999.099999999"),(30, "-999999999999999999.900000000"),(31, "-999999999999999999.900000001"),(32, "999999999999999998.000000000"),(33, "999999999999999998.000000001"),(34, "999999999999999998.000000009"),(35, "999999999999999998.999999999"),(36, "999999999999999998.999999998"),(37, "999999999999999998.099999999"),(38, "999999999999999998.900000000"),(39, "999999999999999998.900000001"),
      (40, "-999999999999999998.000000000"),(41, "-999999999999999998.000000001"),(42, "-999999999999999998.000000009"),(43, "-999999999999999998.999999999"),(44, "-999999999999999998.999999998"),(45, "-999999999999999998.099999999"),(46, "-999999999999999998.900000000"),(47, "-999999999999999998.900000001");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_int_from_decimalv2_overflow_2_data_start_index = 0
    def test_cast_to_int_from_decimalv2_overflow_2_data_end_index = 48
    for (int data_index = test_cast_to_int_from_decimalv2_overflow_2_data_start_index; data_index < test_cast_to_int_from_decimalv2_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_int_from_decimalv2_overflow_3;"
    sql "create table test_cast_to_int_from_decimalv2_overflow_3(f1 int, f2 decimalv2(19, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_overflow_3 values (0, "2147483648.000000"),(1, "2147483648.000000"),(2, "2147483648.000000"),(3, "2147483648.001000"),(4, "2147483648.001000"),(5, "2147483648.000100"),(6, "2147483648.000900"),(7, "2147483648.000900"),(8, "-2147483649.000000"),(9, "-2147483649.000000"),(10, "-2147483649.000000"),(11, "-2147483649.001000"),(12, "-2147483649.001000"),(13, "-2147483649.000100"),(14, "-2147483649.000900"),(15, "-2147483649.000900"),(16, "9999999999999.000000"),(17, "9999999999999.000000"),(18, "9999999999999.000000"),(19, "9999999999999.001000"),
      (20, "9999999999999.001000"),(21, "9999999999999.000100"),(22, "9999999999999.000900"),(23, "9999999999999.000900"),(24, "-9999999999999.000000"),(25, "-9999999999999.000000"),(26, "-9999999999999.000000"),(27, "-9999999999999.001000"),(28, "-9999999999999.001000"),(29, "-9999999999999.000100"),(30, "-9999999999999.000900"),(31, "-9999999999999.000900"),(32, "9999999999998.000000"),(33, "9999999999998.000000"),(34, "9999999999998.000000"),(35, "9999999999998.001000"),(36, "9999999999998.001000"),(37, "9999999999998.000100"),(38, "9999999999998.000900"),(39, "9999999999998.000900"),
      (40, "-9999999999998.000000"),(41, "-9999999999998.000000"),(42, "-9999999999998.000000"),(43, "-9999999999998.001000"),(44, "-9999999999998.001000"),(45, "-9999999999998.000100"),(46, "-9999999999998.000900"),(47, "-9999999999998.000900");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_int_from_decimalv2_overflow_3_data_start_index = 0
    def test_cast_to_int_from_decimalv2_overflow_3_data_end_index = 48
    for (int data_index = test_cast_to_int_from_decimalv2_overflow_3_data_start_index; data_index < test_cast_to_int_from_decimalv2_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_overflow_3 order by 1;'

}