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


suite("test_cast_to_tinyint_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal32_overflow_2;"
    sql "create table test_cast_to_tinyint_from_decimal32_overflow_2(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal32_overflow_2 values (0, "128"),(1, "-129"),(2, "999999999"),(3, "-999999999"),(4, "999999998"),(5, "-999999998");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal32_overflow_2_data_start_index = 0
    def test_cast_to_tinyint_from_decimal32_overflow_2_data_end_index = 6
    for (int data_index = test_cast_to_tinyint_from_decimal32_overflow_2_data_start_index; data_index < test_cast_to_tinyint_from_decimal32_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal32_overflow_3;"
    sql "create table test_cast_to_tinyint_from_decimal32_overflow_3(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal32_overflow_3 values (0, "128.0"),(1, "128.1"),(2, "128.9"),(3, "128.9"),(4, "128.8"),(5, "128.0"),(6, "128.9"),(7, "128.8"),(8, "-129.0"),(9, "-129.1"),(10, "-129.9"),(11, "-129.9"),(12, "-129.8"),(13, "-129.0"),(14, "-129.9"),(15, "-129.8"),(16, "99999999.0"),(17, "99999999.1"),(18, "99999999.9"),(19, "99999999.9"),
      (20, "99999999.8"),(21, "99999999.0"),(22, "99999999.9"),(23, "99999999.8"),(24, "-99999999.0"),(25, "-99999999.1"),(26, "-99999999.9"),(27, "-99999999.9"),(28, "-99999999.8"),(29, "-99999999.0"),(30, "-99999999.9"),(31, "-99999999.8"),(32, "99999998.0"),(33, "99999998.1"),(34, "99999998.9"),(35, "99999998.9"),(36, "99999998.8"),(37, "99999998.0"),(38, "99999998.9"),(39, "99999998.8"),
      (40, "-99999998.0"),(41, "-99999998.1"),(42, "-99999998.9"),(43, "-99999998.9"),(44, "-99999998.8"),(45, "-99999998.0"),(46, "-99999998.9"),(47, "-99999998.8");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal32_overflow_3_data_start_index = 0
    def test_cast_to_tinyint_from_decimal32_overflow_3_data_end_index = 48
    for (int data_index = test_cast_to_tinyint_from_decimal32_overflow_3_data_start_index; data_index < test_cast_to_tinyint_from_decimal32_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_3 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal32_overflow_4;"
    sql "create table test_cast_to_tinyint_from_decimal32_overflow_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal32_overflow_4 values (0, "128.0000"),(1, "128.0001"),(2, "128.0009"),(3, "128.9999"),(4, "128.9998"),(5, "128.0999"),(6, "128.9000"),(7, "128.9001"),(8, "-129.0000"),(9, "-129.0001"),(10, "-129.0009"),(11, "-129.9999"),(12, "-129.9998"),(13, "-129.0999"),(14, "-129.9000"),(15, "-129.9001"),(16, "99999.0000"),(17, "99999.0001"),(18, "99999.0009"),(19, "99999.9999"),
      (20, "99999.9998"),(21, "99999.0999"),(22, "99999.9000"),(23, "99999.9001"),(24, "-99999.0000"),(25, "-99999.0001"),(26, "-99999.0009"),(27, "-99999.9999"),(28, "-99999.9998"),(29, "-99999.0999"),(30, "-99999.9000"),(31, "-99999.9001"),(32, "99998.0000"),(33, "99998.0001"),(34, "99998.0009"),(35, "99998.9999"),(36, "99998.9998"),(37, "99998.0999"),(38, "99998.9000"),(39, "99998.9001"),
      (40, "-99998.0000"),(41, "-99998.0001"),(42, "-99998.0009"),(43, "-99998.9999"),(44, "-99998.9998"),(45, "-99998.0999"),(46, "-99998.9000"),(47, "-99998.9001");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal32_overflow_4_data_start_index = 0
    def test_cast_to_tinyint_from_decimal32_overflow_4_data_end_index = 48
    for (int data_index = test_cast_to_tinyint_from_decimal32_overflow_4_data_start_index; data_index < test_cast_to_tinyint_from_decimal32_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_overflow_4 order by 1;'

}