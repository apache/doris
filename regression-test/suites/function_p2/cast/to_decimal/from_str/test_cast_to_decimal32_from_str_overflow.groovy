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
    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_0;"
    sql "create table test_cast_to_decimal32_from_str_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_0 values (0, "-10"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-19"),(3, "-2147483648"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(5, "-9.5"),(6, "-90"),(7, "-91"),(8, "-99"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "10"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "19"),(13, "2147483647"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(15, "9.5"),(16, "90"),(17, "91"),(18, "99"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_0_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_from_str_overflow_0_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_str_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_str_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_1;"
    sql "create table test_cast_to_decimal32_from_str_overflow_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_1 values (0, "-.95"),(1, "-01"),(2, "-09"),(3, "-1"),(4, "-10"),(5, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(6, "-2147483648"),(7, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(8, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, ".95"),(10, "01"),(11, "09"),(12, "1"),(13, "10"),(14, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(15, "2147483647"),(16, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(17, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_1_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_1_data_end_index = 18
    for (int data_index = test_cast_to_decimal32_from_str_overflow_1_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_from_str_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_from_str_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_2;"
    sql "create table test_cast_to_decimal32_from_str_overflow_2(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_2 values (0, "-1000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-1999999999"),(3, "-2147483648"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(5, "-999999999.5"),(6, "-9999999990"),(7, "-9999999991"),(8, "-9999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "1000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "1999999999"),(13, "2147483647"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(15, "999999999.5"),(16, "9999999990"),(17, "9999999991"),(18, "9999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_2_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_2_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_from_str_overflow_2_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_str_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_str_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_3;"
    sql "create table test_cast_to_decimal32_from_str_overflow_3(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_3 values (0, "-100000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-199999999"),(3, "-2147483648"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(5, "-99999999.95"),(6, "-999999990"),(7, "-999999991"),(8, "-999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "100000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "199999999"),(13, "2147483647"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(15, "99999999.95"),(16, "999999990"),(17, "999999991"),(18, "999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_3_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_3_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_from_str_overflow_3_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_from_str_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_from_str_overflow_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_4;"
    sql "create table test_cast_to_decimal32_from_str_overflow_4(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_4 values (0, "-100000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-199999"),(3, "-2147483648"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(5, "-99999.99995"),(6, "-999990"),(7, "-999991"),(8, "-999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "100000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "199999"),(13, "2147483647"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(15, "99999.99995"),(16, "999990"),(17, "999991"),(18, "999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_4_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_4_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_from_str_overflow_4_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_str_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_str_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_str_overflow_5;"
    sql "create table test_cast_to_decimal32_from_str_overflow_5(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_str_overflow_5 values (0, "-.9999999995"),(1, "-01"),(2, "-09"),(3, "-1"),(4, "-10"),(5, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(6, "-2147483648"),(7, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(8, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, ".9999999995"),(10, "01"),(11, "09"),(12, "1"),(13, "10"),(14, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(15, "2147483647"),(16, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(17, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_str_overflow_5_data_start_index = 0
    def test_cast_to_decimal32_from_str_overflow_5_data_end_index = 18
    for (int data_index = test_cast_to_decimal32_from_str_overflow_5_data_start_index; data_index < test_cast_to_decimal32_from_str_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_from_str_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_from_str_overflow_5 order by 1;'

}