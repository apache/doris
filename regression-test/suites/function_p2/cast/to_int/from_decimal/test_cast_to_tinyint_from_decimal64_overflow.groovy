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


suite("test_cast_to_tinyint_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal64_overflow_0;"
    sql "create table test_cast_to_tinyint_from_decimal64_overflow_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_overflow_0 values (0, "128"),(1, "-129"),(2, "9999999999"),(3, "-9999999999"),(4, "9999999998"),(5, "-9999999998");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal64_overflow_0_data_start_index = 0
    def test_cast_to_tinyint_from_decimal64_overflow_0_data_end_index = 6
    for (int data_index = test_cast_to_tinyint_from_decimal64_overflow_0_data_start_index; data_index < test_cast_to_tinyint_from_decimal64_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_overflow_1;"
    sql "create table test_cast_to_tinyint_from_decimal64_overflow_1(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_overflow_1 values (0, "128.00000"),(1, "128.00001"),(2, "128.00009"),(3, "128.99999"),(4, "128.99998"),(5, "128.09999"),(6, "128.90000"),(7, "128.90001"),(8, "-129.00000"),(9, "-129.00001"),(10, "-129.00009"),(11, "-129.99999"),(12, "-129.99998"),(13, "-129.09999"),(14, "-129.90000"),(15, "-129.90001"),(16, "99999.00000"),(17, "99999.00001"),(18, "99999.00009"),(19, "99999.99999"),
      (20, "99999.99998"),(21, "99999.09999"),(22, "99999.90000"),(23, "99999.90001"),(24, "-99999.00000"),(25, "-99999.00001"),(26, "-99999.00009"),(27, "-99999.99999"),(28, "-99999.99998"),(29, "-99999.09999"),(30, "-99999.90000"),(31, "-99999.90001"),(32, "99998.00000"),(33, "99998.00001"),(34, "99998.00009"),(35, "99998.99999"),(36, "99998.99998"),(37, "99998.09999"),(38, "99998.90000"),(39, "99998.90001"),
      (40, "-99998.00000"),(41, "-99998.00001"),(42, "-99998.00009"),(43, "-99998.99999"),(44, "-99998.99998"),(45, "-99998.09999"),(46, "-99998.90000"),(47, "-99998.90001");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal64_overflow_1_data_start_index = 0
    def test_cast_to_tinyint_from_decimal64_overflow_1_data_end_index = 48
    for (int data_index = test_cast_to_tinyint_from_decimal64_overflow_1_data_start_index; data_index < test_cast_to_tinyint_from_decimal64_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_overflow_4;"
    sql "create table test_cast_to_tinyint_from_decimal64_overflow_4(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_overflow_4 values (0, "128"),(1, "-129"),(2, "999999999999999999"),(3, "-999999999999999999"),(4, "999999999999999998"),(5, "-999999999999999998");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal64_overflow_4_data_start_index = 0
    def test_cast_to_tinyint_from_decimal64_overflow_4_data_end_index = 6
    for (int data_index = test_cast_to_tinyint_from_decimal64_overflow_4_data_start_index; data_index < test_cast_to_tinyint_from_decimal64_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_overflow_5;"
    sql "create table test_cast_to_tinyint_from_decimal64_overflow_5(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_overflow_5 values (0, "128.0"),(1, "128.1"),(2, "128.9"),(3, "128.9"),(4, "128.8"),(5, "128.0"),(6, "128.9"),(7, "128.8"),(8, "-129.0"),(9, "-129.1"),(10, "-129.9"),(11, "-129.9"),(12, "-129.8"),(13, "-129.0"),(14, "-129.9"),(15, "-129.8"),(16, "99999999999999999.0"),(17, "99999999999999999.1"),(18, "99999999999999999.9"),(19, "99999999999999999.9"),
      (20, "99999999999999999.8"),(21, "99999999999999999.0"),(22, "99999999999999999.9"),(23, "99999999999999999.8"),(24, "-99999999999999999.0"),(25, "-99999999999999999.1"),(26, "-99999999999999999.9"),(27, "-99999999999999999.9"),(28, "-99999999999999999.8"),(29, "-99999999999999999.0"),(30, "-99999999999999999.9"),(31, "-99999999999999999.8"),(32, "99999999999999998.0"),(33, "99999999999999998.1"),(34, "99999999999999998.9"),(35, "99999999999999998.9"),(36, "99999999999999998.8"),(37, "99999999999999998.0"),(38, "99999999999999998.9"),(39, "99999999999999998.8"),
      (40, "-99999999999999998.0"),(41, "-99999999999999998.1"),(42, "-99999999999999998.9"),(43, "-99999999999999998.9"),(44, "-99999999999999998.8"),(45, "-99999999999999998.0"),(46, "-99999999999999998.9"),(47, "-99999999999999998.8");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal64_overflow_5_data_start_index = 0
    def test_cast_to_tinyint_from_decimal64_overflow_5_data_end_index = 48
    for (int data_index = test_cast_to_tinyint_from_decimal64_overflow_5_data_start_index; data_index < test_cast_to_tinyint_from_decimal64_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_overflow_6;"
    sql "create table test_cast_to_tinyint_from_decimal64_overflow_6(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_overflow_6 values (0, "128.000000000"),(1, "128.000000001"),(2, "128.000000009"),(3, "128.999999999"),(4, "128.999999998"),(5, "128.099999999"),(6, "128.900000000"),(7, "128.900000001"),(8, "-129.000000000"),(9, "-129.000000001"),(10, "-129.000000009"),(11, "-129.999999999"),(12, "-129.999999998"),(13, "-129.099999999"),(14, "-129.900000000"),(15, "-129.900000001"),(16, "999999999.000000000"),(17, "999999999.000000001"),(18, "999999999.000000009"),(19, "999999999.999999999"),
      (20, "999999999.999999998"),(21, "999999999.099999999"),(22, "999999999.900000000"),(23, "999999999.900000001"),(24, "-999999999.000000000"),(25, "-999999999.000000001"),(26, "-999999999.000000009"),(27, "-999999999.999999999"),(28, "-999999999.999999998"),(29, "-999999999.099999999"),(30, "-999999999.900000000"),(31, "-999999999.900000001"),(32, "999999998.000000000"),(33, "999999998.000000001"),(34, "999999998.000000009"),(35, "999999998.999999999"),(36, "999999998.999999998"),(37, "999999998.099999999"),(38, "999999998.900000000"),(39, "999999998.900000001"),
      (40, "-999999998.000000000"),(41, "-999999998.000000001"),(42, "-999999998.000000009"),(43, "-999999998.999999999"),(44, "-999999998.999999998"),(45, "-999999998.099999999"),(46, "-999999998.900000000"),(47, "-999999998.900000001");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_decimal64_overflow_6_data_start_index = 0
    def test_cast_to_tinyint_from_decimal64_overflow_6_data_end_index = 48
    for (int data_index = test_cast_to_tinyint_from_decimal64_overflow_6_data_start_index; data_index < test_cast_to_tinyint_from_decimal64_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_overflow_6 order by 1;'

}