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


suite("test_cast_to_decimal128v3_from_str_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_from_str_overflow_0_38_0;"
    sql "create table test_cast_to_decimal128v3_from_str_overflow_0_38_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_from_str_overflow_0_38_0 values (0, "-100000000000000000000000000000000000000"),(1, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(2, "-170141183460469231731687303715884105728"),(3, "-199999999999999999999999999999999999999"),(4, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (5, "-99999999999999999999999999999999999999.5"),(6, "-999999999999999999999999999999999999990"),(7, "-999999999999999999999999999999999999991"),(8, "-999999999999999999999999999999999999999"),(9, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (10, "100000000000000000000000000000000000000"),(11, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(12, "170141183460469231731687303715884105727"),(13, "199999999999999999999999999999999999999"),(14, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (15, "99999999999999999999999999999999999999.5"),(16, "999999999999999999999999999999999999990"),(17, "999999999999999999999999999999999999991"),(18, "999999999999999999999999999999999999999"),(19, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_from_str_overflow_0_38_0_data_start_index = 0
    def test_cast_to_decimal128v3_from_str_overflow_0_38_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal128v3_from_str_overflow_0_38_0_data_start_index; data_index < test_cast_to_decimal128v3_from_str_overflow_0_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_from_str_overflow_0_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_from_str_overflow_0_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_from_str_overflow_1_38_1;"
    sql "create table test_cast_to_decimal128v3_from_str_overflow_1_38_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_from_str_overflow_1_38_1 values (20, "-10000000000000000000000000000000000000"),(21, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(22, "-170141183460469231731687303715884105728"),(23, "-19999999999999999999999999999999999999"),(24, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (25, "-9999999999999999999999999999999999999.95"),(26, "-99999999999999999999999999999999999990"),(27, "-99999999999999999999999999999999999991"),(28, "-99999999999999999999999999999999999999"),(29, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (30, "10000000000000000000000000000000000000"),(31, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(32, "170141183460469231731687303715884105727"),(33, "19999999999999999999999999999999999999"),(34, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (35, "9999999999999999999999999999999999999.95"),(36, "99999999999999999999999999999999999990"),(37, "99999999999999999999999999999999999991"),(38, "99999999999999999999999999999999999999"),(39, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_from_str_overflow_1_38_1_data_start_index = 20
    def test_cast_to_decimal128v3_from_str_overflow_1_38_1_data_end_index = 40
    for (int data_index = test_cast_to_decimal128v3_from_str_overflow_1_38_1_data_start_index; data_index < test_cast_to_decimal128v3_from_str_overflow_1_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128v3_from_str_overflow_1_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128v3_from_str_overflow_1_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_from_str_overflow_2_38_19;"
    sql "create table test_cast_to_decimal128v3_from_str_overflow_2_38_19(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_from_str_overflow_2_38_19 values (40, "-10000000000000000000"),(41, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(42, "-170141183460469231731687303715884105728"),(43, "-19999999999999999999"),(44, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
      (45, "-9999999999999999999.99999999999999999995"),(46, "-99999999999999999990"),(47, "-99999999999999999991"),(48, "-99999999999999999999"),(49, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),
      (50, "10000000000000000000"),(51, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),(52, "170141183460469231731687303715884105727"),(53, "19999999999999999999"),(54, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),
      (55, "9999999999999999999.99999999999999999995"),(56, "99999999999999999990"),(57, "99999999999999999991"),(58, "99999999999999999999"),(59, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_from_str_overflow_2_38_19_data_start_index = 40
    def test_cast_to_decimal128v3_from_str_overflow_2_38_19_data_end_index = 60
    for (int data_index = test_cast_to_decimal128v3_from_str_overflow_2_38_19_data_start_index; data_index < test_cast_to_decimal128v3_from_str_overflow_2_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_from_str_overflow_2_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_from_str_overflow_2_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_from_str_overflow_3_38_38;"
    sql "create table test_cast_to_decimal128v3_from_str_overflow_3_38_38(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_from_str_overflow_3_38_38 values (60, "-.999999999999999999999999999999999999995"),(61, "-01"),(62, "-09"),(63, "-1"),(64, "-10"),
      (65, "-10000000000000000000000000000000000000000000000000000000000000000000000000000"),(66, "-170141183460469231731687303715884105728"),(67, "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),(68, "-9999999999999999999999999999999999999999999999999999999999999999999999999999"),(69, ".999999999999999999999999999999999999995"),
      (70, "01"),(71, "09"),(72, "1"),(73, "10"),(74, "10000000000000000000000000000000000000000000000000000000000000000000000000000"),
      (75, "170141183460469231731687303715884105727"),(76, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(77, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_from_str_overflow_3_38_38_data_start_index = 60
    def test_cast_to_decimal128v3_from_str_overflow_3_38_38_data_end_index = 78
    for (int data_index = test_cast_to_decimal128v3_from_str_overflow_3_38_38_data_start_index; data_index < test_cast_to_decimal128v3_from_str_overflow_3_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128v3_from_str_overflow_3_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128v3_from_str_overflow_3_38_38 order by 1;'

}