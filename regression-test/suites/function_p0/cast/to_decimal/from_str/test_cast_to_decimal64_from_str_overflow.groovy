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
    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_0;"
    sql "create table test_cast_to_decimal64_from_str_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_0 values (0, "-1000000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-1999999999999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-9223372036854775808"),(5, "-999999999999999999.5"),(6, "-9999999999999999990"),(7, "-9999999999999999991"),(8, "-9999999999999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "1000000000000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "1999999999999999999"),(13, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(14, "9223372036854775807"),(15, "999999999999999999.5"),(16, "9999999999999999990"),(17, "9999999999999999991"),(18, "9999999999999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_0_data_start_index = 0
    def test_cast_to_decimal64_from_str_overflow_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal64_from_str_overflow_0_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_from_str_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_from_str_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_1;"
    sql "create table test_cast_to_decimal64_from_str_overflow_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_1 values (0, "-100000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-199999999999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-9223372036854775808"),(5, "-99999999999999999.95"),(6, "-999999999999999990"),(7, "-999999999999999991"),(8, "-999999999999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "100000000000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "199999999999999999"),(13, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(14, "9223372036854775807"),(15, "99999999999999999.95"),(16, "999999999999999990"),(17, "999999999999999991"),(18, "999999999999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_1_data_start_index = 0
    def test_cast_to_decimal64_from_str_overflow_1_data_end_index = 20
    for (int data_index = test_cast_to_decimal64_from_str_overflow_1_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_from_str_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_from_str_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_2;"
    sql "create table test_cast_to_decimal64_from_str_overflow_2(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_2 values (0, "-1000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-1999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-9223372036854775808"),(5, "-999999999.9999999995"),(6, "-9999999990"),(7, "-9999999991"),(8, "-9999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(10, "1000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "1999999999"),(13, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(14, "9223372036854775807"),(15, "999999999.9999999995"),(16, "9999999990"),(17, "9999999991"),(18, "9999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_2_data_start_index = 0
    def test_cast_to_decimal64_from_str_overflow_2_data_end_index = 20
    for (int data_index = test_cast_to_decimal64_from_str_overflow_2_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_from_str_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_from_str_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_from_str_overflow_3;"
    sql "create table test_cast_to_decimal64_from_str_overflow_3(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_from_str_overflow_3 values (0, "-.9999999999999999995"),(1, "-01"),(2, "-09"),(3, "-1"),(4, "-10"),(5, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(6, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(7, "-9223372036854775808"),(8, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, ".9999999999999999995"),(10, "01"),(11, "09"),(12, "1"),(13, "10"),(14, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(15, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(16, "9223372036854775807"),(17, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_from_str_overflow_3_data_start_index = 0
    def test_cast_to_decimal64_from_str_overflow_3_data_end_index = 18
    for (int data_index = test_cast_to_decimal64_from_str_overflow_3_data_start_index; data_index < test_cast_to_decimal64_from_str_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_from_str_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_from_str_overflow_3 order by 1;'

}