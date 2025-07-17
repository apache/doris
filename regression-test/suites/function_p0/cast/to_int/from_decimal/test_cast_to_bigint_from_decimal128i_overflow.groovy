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


suite("test_cast_to_bigint_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_bigint_from_decimal128i_overflow_0;"
    sql "create table test_cast_to_bigint_from_decimal128i_overflow_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_overflow_0 values (0, "9223372036854775808"),(1, "-9223372036854775809"),(2, "9999999999999999999"),(3, "-9999999999999999999"),(4, "9999999999999999998"),(5, "-9999999999999999998");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_bigint_from_decimal128i_overflow_0_data_start_index = 0
    def test_cast_to_bigint_from_decimal128i_overflow_0_data_end_index = 6
    for (int data_index = test_cast_to_bigint_from_decimal128i_overflow_0_data_start_index; data_index < test_cast_to_bigint_from_decimal128i_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_decimal128i_overflow_4;"
    sql "create table test_cast_to_bigint_from_decimal128i_overflow_4(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_overflow_4 values (0, "9223372036854775808"),(1, "-9223372036854775809"),(2, "99999999999999999999999999999999999999"),(3, "-99999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998"),(5, "-99999999999999999999999999999999999998");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_bigint_from_decimal128i_overflow_4_data_start_index = 0
    def test_cast_to_bigint_from_decimal128i_overflow_4_data_end_index = 6
    for (int data_index = test_cast_to_bigint_from_decimal128i_overflow_4_data_start_index; data_index < test_cast_to_bigint_from_decimal128i_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_decimal128i_overflow_5;"
    sql "create table test_cast_to_bigint_from_decimal128i_overflow_5(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_overflow_5 values (0, "9223372036854775808.0"),(1, "9223372036854775808.1"),(2, "9223372036854775808.9"),(3, "9223372036854775808.9"),(4, "9223372036854775808.8"),(5, "9223372036854775808.0"),(6, "9223372036854775808.9"),(7, "9223372036854775808.8"),(8, "-9223372036854775809.0"),(9, "-9223372036854775809.1"),(10, "-9223372036854775809.9"),(11, "-9223372036854775809.9"),(12, "-9223372036854775809.8"),(13, "-9223372036854775809.0"),(14, "-9223372036854775809.9"),(15, "-9223372036854775809.8"),(16, "9999999999999999999999999999999999999.0"),(17, "9999999999999999999999999999999999999.1"),(18, "9999999999999999999999999999999999999.9"),(19, "9999999999999999999999999999999999999.9"),
      (20, "9999999999999999999999999999999999999.8"),(21, "9999999999999999999999999999999999999.0"),(22, "9999999999999999999999999999999999999.9"),(23, "9999999999999999999999999999999999999.8"),(24, "-9999999999999999999999999999999999999.0"),(25, "-9999999999999999999999999999999999999.1"),(26, "-9999999999999999999999999999999999999.9"),(27, "-9999999999999999999999999999999999999.9"),(28, "-9999999999999999999999999999999999999.8"),(29, "-9999999999999999999999999999999999999.0"),(30, "-9999999999999999999999999999999999999.9"),(31, "-9999999999999999999999999999999999999.8"),(32, "9999999999999999999999999999999999998.0"),(33, "9999999999999999999999999999999999998.1"),(34, "9999999999999999999999999999999999998.9"),(35, "9999999999999999999999999999999999998.9"),(36, "9999999999999999999999999999999999998.8"),(37, "9999999999999999999999999999999999998.0"),(38, "9999999999999999999999999999999999998.9"),(39, "9999999999999999999999999999999999998.8"),
      (40, "-9999999999999999999999999999999999998.0"),(41, "-9999999999999999999999999999999999998.1"),(42, "-9999999999999999999999999999999999998.9"),(43, "-9999999999999999999999999999999999998.9"),(44, "-9999999999999999999999999999999999998.8"),(45, "-9999999999999999999999999999999999998.0"),(46, "-9999999999999999999999999999999999998.9"),(47, "-9999999999999999999999999999999999998.8");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_bigint_from_decimal128i_overflow_5_data_start_index = 0
    def test_cast_to_bigint_from_decimal128i_overflow_5_data_end_index = 48
    for (int data_index = test_cast_to_bigint_from_decimal128i_overflow_5_data_start_index; data_index < test_cast_to_bigint_from_decimal128i_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_decimal128i_overflow_6;"
    sql "create table test_cast_to_bigint_from_decimal128i_overflow_6(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_overflow_6 values (0, "9223372036854775808.0000000000000000000"),(1, "9223372036854775808.0000000000000000001"),(2, "9223372036854775808.0000000000000000009"),(3, "9223372036854775808.9999999999999999999"),(4, "9223372036854775808.9999999999999999998"),(5, "9223372036854775808.0999999999999999999"),(6, "9223372036854775808.9000000000000000000"),(7, "9223372036854775808.9000000000000000001"),(8, "-9223372036854775809.0000000000000000000"),(9, "-9223372036854775809.0000000000000000001"),(10, "-9223372036854775809.0000000000000000009"),(11, "-9223372036854775809.9999999999999999999"),(12, "-9223372036854775809.9999999999999999998"),(13, "-9223372036854775809.0999999999999999999"),(14, "-9223372036854775809.9000000000000000000"),(15, "-9223372036854775809.9000000000000000001"),(16, "9999999999999999999.0000000000000000000"),(17, "9999999999999999999.0000000000000000001"),(18, "9999999999999999999.0000000000000000009"),(19, "9999999999999999999.9999999999999999999"),
      (20, "9999999999999999999.9999999999999999998"),(21, "9999999999999999999.0999999999999999999"),(22, "9999999999999999999.9000000000000000000"),(23, "9999999999999999999.9000000000000000001"),(24, "-9999999999999999999.0000000000000000000"),(25, "-9999999999999999999.0000000000000000001"),(26, "-9999999999999999999.0000000000000000009"),(27, "-9999999999999999999.9999999999999999999"),(28, "-9999999999999999999.9999999999999999998"),(29, "-9999999999999999999.0999999999999999999"),(30, "-9999999999999999999.9000000000000000000"),(31, "-9999999999999999999.9000000000000000001"),(32, "9999999999999999998.0000000000000000000"),(33, "9999999999999999998.0000000000000000001"),(34, "9999999999999999998.0000000000000000009"),(35, "9999999999999999998.9999999999999999999"),(36, "9999999999999999998.9999999999999999998"),(37, "9999999999999999998.0999999999999999999"),(38, "9999999999999999998.9000000000000000000"),(39, "9999999999999999998.9000000000000000001"),
      (40, "-9999999999999999998.0000000000000000000"),(41, "-9999999999999999998.0000000000000000001"),(42, "-9999999999999999998.0000000000000000009"),(43, "-9999999999999999998.9999999999999999999"),(44, "-9999999999999999998.9999999999999999998"),(45, "-9999999999999999998.0999999999999999999"),(46, "-9999999999999999998.9000000000000000000"),(47, "-9999999999999999998.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_bigint_from_decimal128i_overflow_6_data_start_index = 0
    def test_cast_to_bigint_from_decimal128i_overflow_6_data_end_index = 48
    for (int data_index = test_cast_to_bigint_from_decimal128i_overflow_6_data_start_index; data_index < test_cast_to_bigint_from_decimal128i_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_overflow_6 order by 1;'

}