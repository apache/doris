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


suite("test_cast_to_decimal64_from_str_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_0_18_0;"
    sql "create table test_cast_to_decimal64_from_str_overflow_0_18_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_0_18_0 values (0, "-1000000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-1999999999999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-9223372036854775808"),
      (5, "-999999999999999999.5"),(6, "-9999999999999999990"),(7, "-9999999999999999991"),(8, "-9999999999999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (10, "1000000000000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "1999999999999999999"),(13, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(14, "9223372036854775807"),
      (15, "999999999999999999.5"),(16, "9999999999999999990"),(17, "9999999999999999991"),(18, "9999999999999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_0_18_0_data_start_index = 0
    def test_cast_to_decimal64_from_str_overflow_0_18_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal64_from_str_overflow_0_18_0_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_0_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_from_str_overflow_0_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_from_str_overflow_0_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_1_18_1;"
    sql "create table test_cast_to_decimal64_from_str_overflow_1_18_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_1_18_1 values (20, "-100000000000000000"),(21, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(22, "-199999999999999999"),(23, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(24, "-9223372036854775808"),
      (25, "-99999999999999999.95"),(26, "-999999999999999990"),(27, "-999999999999999991"),(28, "-999999999999999999"),(29, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (30, "100000000000000000"),(31, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(32, "199999999999999999"),(33, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(34, "9223372036854775807"),
      (35, "99999999999999999.95"),(36, "999999999999999990"),(37, "999999999999999991"),(38, "999999999999999999"),(39, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_1_18_1_data_start_index = 20
    def test_cast_to_decimal64_from_str_overflow_1_18_1_data_end_index = 40
    for (int data_index = test_cast_to_decimal64_from_str_overflow_1_18_1_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_1_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_from_str_overflow_1_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_from_str_overflow_1_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_2_18_9;"
    sql "create table test_cast_to_decimal64_from_str_overflow_2_18_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_2_18_9 values (40, "-1000000000"),(41, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(42, "-1999999999"),(43, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(44, "-9223372036854775808"),
      (45, "-999999999.9999999995"),(46, "-9999999990"),(47, "-9999999991"),(48, "-9999999999"),(49, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (50, "1000000000"),(51, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(52, "1999999999"),(53, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(54, "9223372036854775807"),
      (55, "999999999.9999999995"),(56, "9999999990"),(57, "9999999991"),(58, "9999999999"),(59, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_2_18_9_data_start_index = 40
    def test_cast_to_decimal64_from_str_overflow_2_18_9_data_end_index = 60
    for (int data_index = test_cast_to_decimal64_from_str_overflow_2_18_9_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_2_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_from_str_overflow_2_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_from_str_overflow_2_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_3_18_18;"
    sql "create table test_cast_to_decimal64_from_str_overflow_3_18_18(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_3_18_18 values (60, "-.9999999999999999995"),(61, "-01"),(62, "-09"),(63, "-1"),(64, "-10"),
      (65, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(66, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(67, "-9223372036854775808"),(68, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(69, ".9999999999999999995"),
      (70, "01"),(71, "09"),(72, "1"),(73, "10"),(74, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),
      (75, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(76, "9223372036854775807"),(77, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_3_18_18_data_start_index = 60
    def test_cast_to_decimal64_from_str_overflow_3_18_18_data_end_index = 78
    for (int data_index = test_cast_to_decimal64_from_str_overflow_3_18_18_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_3_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_from_str_overflow_3_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_from_str_overflow_3_18_18 order by 1;'

}