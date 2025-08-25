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
    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_0;"
    sql "create table test_cast_to_decimal256_from_str_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_0 values (0, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "-19999999999999999999999999999999999999999999999999999999999999999999999999999"),(2, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(3, "-9999999999999999999999999999999999999999999999999999999999999999999999999999.5"),(4, "-99999999999999999999999999999999999999999999999999999999999999999999999999990"),(5, "-99999999999999999999999999999999999999999999999999999999999999999999999999991"),(6, "-99999999999999999999999999999999999999999999999999999999999999999999999999999"),(7, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(8, "19999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(10, "9999999999999999999999999999999999999999999999999999999999999999999999999999.5"),(11, "99999999999999999999999999999999999999999999999999999999999999999999999999990"),(12, "99999999999999999999999999999999999999999999999999999999999999999999999999991"),(13, "99999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_0_data_start_index = 0
    def test_cast_to_decimal256_from_str_overflow_0_data_end_index = 14
    for (int data_index = test_cast_to_decimal256_from_str_overflow_0_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_str_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_str_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_1;"
    sql "create table test_cast_to_decimal256_from_str_overflow_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_1 values (0, "-1000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-1999999999999999999999999999999999999999999999999999999999999999999999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-999999999999999999999999999999999999999999999999999999999999999999999999999.95"),(5, "-9999999999999999999999999999999999999999999999999999999999999999999999999990"),(6, "-9999999999999999999999999999999999999999999999999999999999999999999999999991"),(7, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(8, "1000000000000000000000000000000000000000000000000000000000000000000000000000"),(9, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(10, "1999999999999999999999999999999999999999999999999999999999999999999999999999"),(11, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(12, "999999999999999999999999999999999999999999999999999999999999999999999999999.95"),(13, "9999999999999999999999999999999999999999999999999999999999999999999999999990"),(14, "9999999999999999999999999999999999999999999999999999999999999999999999999991"),(15, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_1_data_start_index = 0
    def test_cast_to_decimal256_from_str_overflow_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal256_from_str_overflow_1_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_from_str_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_from_str_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_2;"
    sql "create table test_cast_to_decimal256_from_str_overflow_2(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_2 values (0, "-100000000000000000000000000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-199999999999999999999999999999999999999"),(3, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(4, "-99999999999999999999999999999999999999.999999999999999999999999999999999999995"),(5, "-999999999999999999999999999999999999990"),(6, "-999999999999999999999999999999999999991"),(7, "-999999999999999999999999999999999999999"),(8, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(9, "100000000000000000000000000000000000000"),(10, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(11, "199999999999999999999999999999999999999"),(12, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(13, "99999999999999999999999999999999999999.999999999999999999999999999999999999995"),(14, "999999999999999999999999999999999999990"),(15, "999999999999999999999999999999999999991"),(16, "999999999999999999999999999999999999999"),(17, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_2_data_start_index = 0
    def test_cast_to_decimal256_from_str_overflow_2_data_end_index = 18
    for (int data_index = test_cast_to_decimal256_from_str_overflow_2_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_str_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_str_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_str_overflow_3;"
    sql "create table test_cast_to_decimal256_from_str_overflow_3(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_str_overflow_3 values (0, "-.99999999999999999999999999999999999999999999999999999999999999999999999999995"),(1, "-01"),(2, "-09"),(3, "-1"),(4, "-10"),(5, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(6, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(7, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(8, ".99999999999999999999999999999999999999999999999999999999999999999999999999995"),(9, "01"),(10, "09"),(11, "1"),(12, "10"),(13, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(15, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_str_overflow_3_data_start_index = 0
    def test_cast_to_decimal256_from_str_overflow_3_data_end_index = 16
    for (int data_index = test_cast_to_decimal256_from_str_overflow_3_data_start_index; data_index < test_cast_to_decimal256_from_str_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_from_str_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_from_str_overflow_3 order by 1;'

}