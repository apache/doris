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


suite("test_cast_to_decimal32_from_str_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_0_1_0;"
    sql "create table test_cast_to_decimal32_from_str_overflow_0_1_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_0_1_0 values (0, "-10"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-19"),(3, "-2147483648"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (5, "-9.5"),(6, "-90"),(7, "-91"),(8, "-99"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (10, "10"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "19"),(13, "2147483647"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (15, "9.5"),(16, "90"),(17, "91"),(18, "99"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_0_1_0_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_0_1_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_from_str_overflow_0_1_0_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_0_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_str_overflow_0_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_str_overflow_0_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_1_1_1;"
    sql "create table test_cast_to_decimal32_from_str_overflow_1_1_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_1_1_1 values (20, "-.95"),(21, "-01"),(22, "-09"),(23, "-1"),(24, "-10"),
      (25, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(26, "-2147483648"),(27, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(28, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(29, ".95"),
      (30, "01"),(31, "09"),(32, "1"),(33, "10"),(34, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),
      (35, "2147483647"),(36, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(37, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_1_1_1_data_start_index = 20
    def test_cast_to_decimal32_from_str_overflow_1_1_1_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_from_str_overflow_1_1_1_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_1_1_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_from_str_overflow_1_1_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_from_str_overflow_1_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_2_9_0;"
    sql "create table test_cast_to_decimal32_from_str_overflow_2_9_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_2_9_0 values (38, "-1000000000"),(39, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(40, "-1999999999"),(41, "-2147483648"),(42, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (43, "-999999999.5"),(44, "-9999999990"),(45, "-9999999991"),(46, "-9999999999"),(47, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (48, "1000000000"),(49, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(50, "1999999999"),(51, "2147483647"),(52, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (53, "999999999.5"),(54, "9999999990"),(55, "9999999991"),(56, "9999999999"),(57, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_2_9_0_data_start_index = 38
    def test_cast_to_decimal32_from_str_overflow_2_9_0_data_end_index = 58
    for (int data_index = test_cast_to_decimal32_from_str_overflow_2_9_0_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_2_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_str_overflow_2_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_str_overflow_2_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_3_9_1;"
    sql "create table test_cast_to_decimal32_from_str_overflow_3_9_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_3_9_1 values (58, "-100000000"),(59, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(60, "-199999999"),(61, "-2147483648"),(62, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (63, "-99999999.95"),(64, "-999999990"),(65, "-999999991"),(66, "-999999999"),(67, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (68, "100000000"),(69, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(70, "199999999"),(71, "2147483647"),(72, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (73, "99999999.95"),(74, "999999990"),(75, "999999991"),(76, "999999999"),(77, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_3_9_1_data_start_index = 58
    def test_cast_to_decimal32_from_str_overflow_3_9_1_data_end_index = 78
    for (int data_index = test_cast_to_decimal32_from_str_overflow_3_9_1_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_3_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_from_str_overflow_3_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_from_str_overflow_3_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_4_9_4;"
    sql "create table test_cast_to_decimal32_from_str_overflow_4_9_4(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_4_9_4 values (78, "-100000"),(79, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(80, "-199999"),(81, "-2147483648"),(82, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (83, "-99999.99995"),(84, "-999990"),(85, "-999991"),(86, "-999999"),(87, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (88, "100000"),(89, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(90, "199999"),(91, "2147483647"),(92, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (93, "99999.99995"),(94, "999990"),(95, "999991"),(96, "999999"),(97, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_4_9_4_data_start_index = 78
    def test_cast_to_decimal32_from_str_overflow_4_9_4_data_end_index = 98
    for (int data_index = test_cast_to_decimal32_from_str_overflow_4_9_4_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_4_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_str_overflow_4_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_str_overflow_4_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_5_9_9;"
    sql "create table test_cast_to_decimal32_from_str_overflow_5_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_5_9_9 values (98, "-.9999999995"),(99, "-01"),(100, "-09"),(101, "-1"),(102, "-10"),
      (103, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(104, "-2147483648"),(105, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(106, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(107, ".9999999995"),
      (108, "01"),(109, "09"),(110, "1"),(111, "10"),(112, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),
      (113, "2147483647"),(114, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(115, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_5_9_9_data_start_index = 98
    def test_cast_to_decimal32_from_str_overflow_5_9_9_data_end_index = 116
    for (int data_index = test_cast_to_decimal32_from_str_overflow_5_9_9_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_5_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_from_str_overflow_5_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_from_str_overflow_5_9_9 order by 1;'

}