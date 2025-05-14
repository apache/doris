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


suite("test_cast_to_decimal256_from_str_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_0_76_0;"
    sql "create table test_cast_to_decimal256_from_str_overflow_0_76_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_0_76_0 values (0, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "-19999999999999999999999999999999999999999999999999999999999999999999999999999"),(2, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(3, "-9999999999999999999999999999999999999999999999999999999999999999999999999999.5"),(4, "-99999999999999999999999999999999999999999999999999999999999999999999999999990"),
      (5, "-99999999999999999999999999999999999999999999999999999999999999999999999999991"),(6, "-99999999999999999999999999999999999999999999999999999999999999999999999999999"),(7, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(8, "19999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (10, "9999999999999999999999999999999999999999999999999999999999999999999999999999.5"),(11, "99999999999999999999999999999999999999999999999999999999999999999999999999990"),(12, "99999999999999999999999999999999999999999999999999999999999999999999999999991"),(13, "99999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_0_76_0_data_start_index = 0
    def test_cast_to_decimal256_from_str_overflow_0_76_0_data_end_index = 14
    for (int data_index = test_cast_to_decimal256_from_str_overflow_0_76_0_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_0_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_str_overflow_0_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_str_overflow_0_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_1_76_1;"
    sql "create table test_cast_to_decimal256_from_str_overflow_1_76_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_1_76_1 values (14, "-1000000000000000000000000000000000000000000000000000000000000000000000000000"),(15, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(16, "-1999999999999999999999999999999999999999999999999999999999999999999999999999"),(17, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(18, "-999999999999999999999999999999999999999999999999999999999999999999999999999.95"),
      (19, "-9999999999999999999999999999999999999999999999999999999999999999999999999990"),(20, "-9999999999999999999999999999999999999999999999999999999999999999999999999991"),(21, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(22, "1000000000000000000000000000000000000000000000000000000000000000000000000000"),(23, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),
      (24, "1999999999999999999999999999999999999999999999999999999999999999999999999999"),(25, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(26, "999999999999999999999999999999999999999999999999999999999999999999999999999.95"),(27, "9999999999999999999999999999999999999999999999999999999999999999999999999990"),(28, "9999999999999999999999999999999999999999999999999999999999999999999999999991"),
      (29, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_1_76_1_data_start_index = 14
    def test_cast_to_decimal256_from_str_overflow_1_76_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal256_from_str_overflow_1_76_1_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_1_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_from_str_overflow_1_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_from_str_overflow_1_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_2_76_38;"
    sql "create table test_cast_to_decimal256_from_str_overflow_2_76_38(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_2_76_38 values (30, "-100000000000000000000000000000000000000"),(31, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(32, "-199999999999999999999999999999999999999"),(33, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(34, "-99999999999999999999999999999999999999.999999999999999999999999999999999999995"),
      (35, "-999999999999999999999999999999999999990"),(36, "-999999999999999999999999999999999999991"),(37, "-999999999999999999999999999999999999999"),(38, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(39, "100000000000000000000000000000000000000"),
      (40, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(41, "199999999999999999999999999999999999999"),(42, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(43, "99999999999999999999999999999999999999.999999999999999999999999999999999999995"),(44, "999999999999999999999999999999999999990"),
      (45, "999999999999999999999999999999999999991"),(46, "999999999999999999999999999999999999999"),(47, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_2_76_38_data_start_index = 30
    def test_cast_to_decimal256_from_str_overflow_2_76_38_data_end_index = 48
    for (int data_index = test_cast_to_decimal256_from_str_overflow_2_76_38_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_2_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_str_overflow_2_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_str_overflow_2_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_3_76_76;"
    sql "create table test_cast_to_decimal256_from_str_overflow_3_76_76(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_3_76_76 values (48, "-.99999999999999999999999999999999999999999999999999999999999999999999999999995"),(49, "-01"),(50, "-09"),(51, "-1"),(52, "-10"),
      (53, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(54, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(55, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(56, ".99999999999999999999999999999999999999999999999999999999999999999999999999995"),(57, "01"),
      (58, "09"),(59, "1"),(60, "10"),(61, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(62, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (63, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_3_76_76_data_start_index = 48
    def test_cast_to_decimal256_from_str_overflow_3_76_76_data_end_index = 64
    for (int data_index = test_cast_to_decimal256_from_str_overflow_3_76_76_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_3_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_from_str_overflow_3_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_from_str_overflow_3_76_76 order by 1;'

}