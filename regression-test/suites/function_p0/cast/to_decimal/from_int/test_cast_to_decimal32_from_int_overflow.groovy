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


suite("test_cast_to_decimal32_from_int_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_tinyint_overflow;"
    sql "create table test_cast_to_decimal32_1_0_from_tinyint_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_tinyint_overflow values (0, -128),(1, -10),(2, 10),(3, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_tinyint_overflow_data_start_index = 0
    def test_cast_to_decimal32_1_0_from_tinyint_overflow_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_1_0_from_tinyint_overflow_data_start_index; data_index < test_cast_to_decimal32_1_0_from_tinyint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_tinyint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_tinyint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_tinyint_overflow;"
    sql "create table test_cast_to_decimal32_4_2_from_tinyint_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_tinyint_overflow values (4, -128),(5, -100),(6, 100),(7, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_tinyint_overflow_data_start_index = 4
    def test_cast_to_decimal32_4_2_from_tinyint_overflow_data_end_index = 8
    for (int data_index = test_cast_to_decimal32_4_2_from_tinyint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_2_from_tinyint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_tinyint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_tinyint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_tinyint_overflow;"
    sql "create table test_cast_to_decimal32_4_3_from_tinyint_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_tinyint_overflow values (8, -128),(9, -10),(10, 10),(11, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_tinyint_overflow_data_start_index = 8
    def test_cast_to_decimal32_4_3_from_tinyint_overflow_data_end_index = 12
    for (int data_index = test_cast_to_decimal32_4_3_from_tinyint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_3_from_tinyint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_tinyint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_tinyint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_tinyint_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_tinyint_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_tinyint_overflow values (12, -128),(13, -10),(14, 10),(15, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_tinyint_overflow_data_start_index = 12
    def test_cast_to_decimal32_9_8_from_tinyint_overflow_data_end_index = 16
    for (int data_index = test_cast_to_decimal32_9_8_from_tinyint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_tinyint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_tinyint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_tinyint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_smallint_overflow;"
    sql "create table test_cast_to_decimal32_1_0_from_smallint_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_smallint_overflow values (16, -32768),(17, -10),(18, 10),(19, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_smallint_overflow_data_start_index = 16
    def test_cast_to_decimal32_1_0_from_smallint_overflow_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_1_0_from_smallint_overflow_data_start_index; data_index < test_cast_to_decimal32_1_0_from_smallint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_smallint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_smallint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_smallint_overflow;"
    sql "create table test_cast_to_decimal32_4_0_from_smallint_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_smallint_overflow values (20, -32768),(21, -10000),(22, 10000),(23, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_smallint_overflow_data_start_index = 20
    def test_cast_to_decimal32_4_0_from_smallint_overflow_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_4_0_from_smallint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_0_from_smallint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_smallint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_smallint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_smallint_overflow;"
    sql "create table test_cast_to_decimal32_4_2_from_smallint_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_smallint_overflow values (24, -32768),(25, -100),(26, 100),(27, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_smallint_overflow_data_start_index = 24
    def test_cast_to_decimal32_4_2_from_smallint_overflow_data_end_index = 28
    for (int data_index = test_cast_to_decimal32_4_2_from_smallint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_2_from_smallint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_smallint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_smallint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_smallint_overflow;"
    sql "create table test_cast_to_decimal32_4_3_from_smallint_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_smallint_overflow values (28, -32768),(29, -10),(30, 10),(31, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_smallint_overflow_data_start_index = 28
    def test_cast_to_decimal32_4_3_from_smallint_overflow_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_4_3_from_smallint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_3_from_smallint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_smallint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_smallint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_smallint_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_smallint_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_smallint_overflow values (32, -32768),(33, -10),(34, 10),(35, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_smallint_overflow_data_start_index = 32
    def test_cast_to_decimal32_9_8_from_smallint_overflow_data_end_index = 36
    for (int data_index = test_cast_to_decimal32_9_8_from_smallint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_smallint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_smallint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_smallint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_int_overflow;"
    sql "create table test_cast_to_decimal32_1_0_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_int_overflow values (36, -2147483648),(37, -10),(38, 10),(39, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_int_overflow_data_start_index = 36
    def test_cast_to_decimal32_1_0_from_int_overflow_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_1_0_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_1_0_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_int_overflow;"
    sql "create table test_cast_to_decimal32_4_0_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_int_overflow values (40, -2147483648),(41, -10000),(42, 10000),(43, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_int_overflow_data_start_index = 40
    def test_cast_to_decimal32_4_0_from_int_overflow_data_end_index = 44
    for (int data_index = test_cast_to_decimal32_4_0_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_4_0_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_int_overflow;"
    sql "create table test_cast_to_decimal32_4_2_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_int_overflow values (44, -2147483648),(45, -100),(46, 100),(47, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_int_overflow_data_start_index = 44
    def test_cast_to_decimal32_4_2_from_int_overflow_data_end_index = 48
    for (int data_index = test_cast_to_decimal32_4_2_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_4_2_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_int_overflow;"
    sql "create table test_cast_to_decimal32_4_3_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_int_overflow values (48, -2147483648),(49, -10),(50, 10),(51, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_int_overflow_data_start_index = 48
    def test_cast_to_decimal32_4_3_from_int_overflow_data_end_index = 52
    for (int data_index = test_cast_to_decimal32_4_3_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_4_3_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_int_overflow;"
    sql "create table test_cast_to_decimal32_9_0_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_int_overflow values (52, -2147483648),(53, -1000000000),(54, 1000000000),(55, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_int_overflow_data_start_index = 52
    def test_cast_to_decimal32_9_0_from_int_overflow_data_end_index = 56
    for (int data_index = test_cast_to_decimal32_9_0_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_9_0_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_int_overflow;"
    sql "create table test_cast_to_decimal32_9_4_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_int_overflow values (56, -2147483648),(57, -100000),(58, 100000),(59, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_int_overflow_data_start_index = 56
    def test_cast_to_decimal32_9_4_from_int_overflow_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_9_4_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_9_4_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_int_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_int_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_int_overflow values (60, -2147483648),(61, -10),(62, 10),(63, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_int_overflow_data_start_index = 60
    def test_cast_to_decimal32_9_8_from_int_overflow_data_end_index = 64
    for (int data_index = test_cast_to_decimal32_9_8_from_int_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_int_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_int_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_int_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_1_0_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_bigint_overflow values (64, -9223372036854775808),(65, -10),(66, 10),(67, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_bigint_overflow_data_start_index = 64
    def test_cast_to_decimal32_1_0_from_bigint_overflow_data_end_index = 68
    for (int data_index = test_cast_to_decimal32_1_0_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_1_0_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_4_0_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_bigint_overflow values (68, -9223372036854775808),(69, -10000),(70, 10000),(71, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_bigint_overflow_data_start_index = 68
    def test_cast_to_decimal32_4_0_from_bigint_overflow_data_end_index = 72
    for (int data_index = test_cast_to_decimal32_4_0_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_0_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_4_2_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_bigint_overflow values (72, -9223372036854775808),(73, -100),(74, 100),(75, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_bigint_overflow_data_start_index = 72
    def test_cast_to_decimal32_4_2_from_bigint_overflow_data_end_index = 76
    for (int data_index = test_cast_to_decimal32_4_2_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_2_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_4_3_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_bigint_overflow values (76, -9223372036854775808),(77, -10),(78, 10),(79, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_bigint_overflow_data_start_index = 76
    def test_cast_to_decimal32_4_3_from_bigint_overflow_data_end_index = 80
    for (int data_index = test_cast_to_decimal32_4_3_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_3_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_9_0_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_bigint_overflow values (80, -9223372036854775808),(81, -1000000000),(82, 1000000000),(83, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_bigint_overflow_data_start_index = 80
    def test_cast_to_decimal32_9_0_from_bigint_overflow_data_end_index = 84
    for (int data_index = test_cast_to_decimal32_9_0_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_0_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_9_4_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_bigint_overflow values (84, -9223372036854775808),(85, -100000),(86, 100000),(87, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_bigint_overflow_data_start_index = 84
    def test_cast_to_decimal32_9_4_from_bigint_overflow_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_9_4_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_4_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_bigint_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_bigint_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_bigint_overflow values (88, -9223372036854775808),(89, -10),(90, 10),(91, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_bigint_overflow_data_start_index = 88
    def test_cast_to_decimal32_9_8_from_bigint_overflow_data_end_index = 92
    for (int data_index = test_cast_to_decimal32_9_8_from_bigint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_bigint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_bigint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_bigint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_1_0_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_largeint_overflow values (92, -170141183460469231731687303715884105728),(93, -10),(94, 10),(95, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_largeint_overflow_data_start_index = 92
    def test_cast_to_decimal32_1_0_from_largeint_overflow_data_end_index = 96
    for (int data_index = test_cast_to_decimal32_1_0_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_1_0_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_4_0_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_largeint_overflow values (96, -170141183460469231731687303715884105728),(97, -10000),(98, 10000),(99, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_largeint_overflow_data_start_index = 96
    def test_cast_to_decimal32_4_0_from_largeint_overflow_data_end_index = 100
    for (int data_index = test_cast_to_decimal32_4_0_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_0_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_4_2_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_largeint_overflow values (100, -170141183460469231731687303715884105728),(101, -100),(102, 100),(103, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_largeint_overflow_data_start_index = 100
    def test_cast_to_decimal32_4_2_from_largeint_overflow_data_end_index = 104
    for (int data_index = test_cast_to_decimal32_4_2_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_2_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_4_3_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_largeint_overflow values (104, -170141183460469231731687303715884105728),(105, -10),(106, 10),(107, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_largeint_overflow_data_start_index = 104
    def test_cast_to_decimal32_4_3_from_largeint_overflow_data_end_index = 108
    for (int data_index = test_cast_to_decimal32_4_3_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_4_3_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_9_0_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_largeint_overflow values (108, -170141183460469231731687303715884105728),(109, -1000000000),(110, 1000000000),(111, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_largeint_overflow_data_start_index = 108
    def test_cast_to_decimal32_9_0_from_largeint_overflow_data_end_index = 112
    for (int data_index = test_cast_to_decimal32_9_0_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_0_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_9_4_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_largeint_overflow values (112, -170141183460469231731687303715884105728),(113, -100000),(114, 100000),(115, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_largeint_overflow_data_start_index = 112
    def test_cast_to_decimal32_9_4_from_largeint_overflow_data_end_index = 116
    for (int data_index = test_cast_to_decimal32_9_4_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_4_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_largeint_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_largeint_overflow;"
    sql "create table test_cast_to_decimal32_9_8_from_largeint_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_largeint_overflow values (116, -170141183460469231731687303715884105728),(117, -10),(118, 10),(119, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_largeint_overflow_data_start_index = 116
    def test_cast_to_decimal32_9_8_from_largeint_overflow_data_end_index = 120
    for (int data_index = test_cast_to_decimal32_9_8_from_largeint_overflow_data_start_index; data_index < test_cast_to_decimal32_9_8_from_largeint_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_largeint_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_largeint_overflow order by 1;'

}