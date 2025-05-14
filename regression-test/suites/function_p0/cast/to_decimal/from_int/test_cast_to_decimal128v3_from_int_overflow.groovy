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


suite("test_cast_to_decimal128v3_from_int_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_1_0_from_int8_overflow;"
    sql "create table test_cast_to_decimal128v3_1_0_from_int8_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_1_0_from_int8_overflow values (0, -128),(1, -10),(2, 10),(3, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_1_0_from_int8_overflow_data_start_index = 0
    def test_cast_to_decimal128v3_1_0_from_int8_overflow_data_end_index = 4
    for (int data_index = test_cast_to_decimal128v3_1_0_from_int8_overflow_data_start_index; data_index < test_cast_to_decimal128v3_1_0_from_int8_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int8_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int8_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_int8_overflow;"
    sql "create table test_cast_to_decimal128v3_19_18_from_int8_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_int8_overflow values (4, -128),(5, -10),(6, 10),(7, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_18_from_int8_overflow_data_start_index = 4
    def test_cast_to_decimal128v3_19_18_from_int8_overflow_data_end_index = 8
    for (int data_index = test_cast_to_decimal128v3_19_18_from_int8_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_18_from_int8_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int8_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int8_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_int8_overflow;"
    sql "create table test_cast_to_decimal128v3_38_37_from_int8_overflow(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_int8_overflow values (8, -128),(9, -10),(10, 10),(11, 127);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_37_from_int8_overflow_data_start_index = 8
    def test_cast_to_decimal128v3_38_37_from_int8_overflow_data_end_index = 12
    for (int data_index = test_cast_to_decimal128v3_38_37_from_int8_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_37_from_int8_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int8_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int8_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_1_0_from_int16_overflow;"
    sql "create table test_cast_to_decimal128v3_1_0_from_int16_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_1_0_from_int16_overflow values (12, -32768),(13, -10),(14, 10),(15, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_1_0_from_int16_overflow_data_start_index = 12
    def test_cast_to_decimal128v3_1_0_from_int16_overflow_data_end_index = 16
    for (int data_index = test_cast_to_decimal128v3_1_0_from_int16_overflow_data_start_index; data_index < test_cast_to_decimal128v3_1_0_from_int16_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int16_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int16_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_int16_overflow;"
    sql "create table test_cast_to_decimal128v3_19_18_from_int16_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_int16_overflow values (16, -32768),(17, -10),(18, 10),(19, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_18_from_int16_overflow_data_start_index = 16
    def test_cast_to_decimal128v3_19_18_from_int16_overflow_data_end_index = 20
    for (int data_index = test_cast_to_decimal128v3_19_18_from_int16_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_18_from_int16_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int16_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int16_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_int16_overflow;"
    sql "create table test_cast_to_decimal128v3_38_37_from_int16_overflow(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_int16_overflow values (20, -32768),(21, -10),(22, 10),(23, 32767);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_37_from_int16_overflow_data_start_index = 20
    def test_cast_to_decimal128v3_38_37_from_int16_overflow_data_end_index = 24
    for (int data_index = test_cast_to_decimal128v3_38_37_from_int16_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_37_from_int16_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int16_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int16_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_1_0_from_int32_overflow;"
    sql "create table test_cast_to_decimal128v3_1_0_from_int32_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_1_0_from_int32_overflow values (24, -2147483648),(25, -10),(26, 10),(27, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_1_0_from_int32_overflow_data_start_index = 24
    def test_cast_to_decimal128v3_1_0_from_int32_overflow_data_end_index = 28
    for (int data_index = test_cast_to_decimal128v3_1_0_from_int32_overflow_data_start_index; data_index < test_cast_to_decimal128v3_1_0_from_int32_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int32_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int32_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_int32_overflow;"
    sql "create table test_cast_to_decimal128v3_19_18_from_int32_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_int32_overflow values (28, -2147483648),(29, -10),(30, 10),(31, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_18_from_int32_overflow_data_start_index = 28
    def test_cast_to_decimal128v3_19_18_from_int32_overflow_data_end_index = 32
    for (int data_index = test_cast_to_decimal128v3_19_18_from_int32_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_18_from_int32_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int32_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int32_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_int32_overflow;"
    sql "create table test_cast_to_decimal128v3_38_37_from_int32_overflow(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_int32_overflow values (32, -2147483648),(33, -10),(34, 10),(35, 2147483647);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_37_from_int32_overflow_data_start_index = 32
    def test_cast_to_decimal128v3_38_37_from_int32_overflow_data_end_index = 36
    for (int data_index = test_cast_to_decimal128v3_38_37_from_int32_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_37_from_int32_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int32_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int32_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_1_0_from_int64_overflow;"
    sql "create table test_cast_to_decimal128v3_1_0_from_int64_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_1_0_from_int64_overflow values (36, -9223372036854775808),(37, -10),(38, 10),(39, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_1_0_from_int64_overflow_data_start_index = 36
    def test_cast_to_decimal128v3_1_0_from_int64_overflow_data_end_index = 40
    for (int data_index = test_cast_to_decimal128v3_1_0_from_int64_overflow_data_start_index; data_index < test_cast_to_decimal128v3_1_0_from_int64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_9_from_int64_overflow;"
    sql "create table test_cast_to_decimal128v3_19_9_from_int64_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_9_from_int64_overflow values (40, -9223372036854775808),(41, -10000000000),(42, 10000000000),(43, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_9_from_int64_overflow_data_start_index = 40
    def test_cast_to_decimal128v3_19_9_from_int64_overflow_data_end_index = 44
    for (int data_index = test_cast_to_decimal128v3_19_9_from_int64_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_9_from_int64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128v3_19_9_from_int64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128v3_19_9_from_int64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_int64_overflow;"
    sql "create table test_cast_to_decimal128v3_19_18_from_int64_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_int64_overflow values (44, -9223372036854775808),(45, -10),(46, 10),(47, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_18_from_int64_overflow_data_start_index = 44
    def test_cast_to_decimal128v3_19_18_from_int64_overflow_data_end_index = 48
    for (int data_index = test_cast_to_decimal128v3_19_18_from_int64_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_18_from_int64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_int64_overflow;"
    sql "create table test_cast_to_decimal128v3_38_37_from_int64_overflow(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_int64_overflow values (48, -9223372036854775808),(49, -10),(50, 10),(51, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_37_from_int64_overflow_data_start_index = 48
    def test_cast_to_decimal128v3_38_37_from_int64_overflow_data_end_index = 52
    for (int data_index = test_cast_to_decimal128v3_38_37_from_int64_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_37_from_int64_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int64_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int64_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_1_0_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_1_0_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_1_0_from_int128_overflow values (52, -170141183460469231731687303715884105728),(53, -10),(54, 10),(55, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_1_0_from_int128_overflow_data_start_index = 52
    def test_cast_to_decimal128v3_1_0_from_int128_overflow_data_end_index = 56
    for (int data_index = test_cast_to_decimal128v3_1_0_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_1_0_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v3_1_0_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_0_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_19_0_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_0_from_int128_overflow values (56, -170141183460469231731687303715884105728),(57, -10000000000000000000),(58, 10000000000000000000),(59, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_0_from_int128_overflow_data_start_index = 56
    def test_cast_to_decimal128v3_19_0_from_int128_overflow_data_end_index = 60
    for (int data_index = test_cast_to_decimal128v3_19_0_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_0_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128v3_19_0_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128v3_19_0_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_9_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_19_9_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_9_from_int128_overflow values (60, -170141183460469231731687303715884105728),(61, -10000000000),(62, 10000000000),(63, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_9_from_int128_overflow_data_start_index = 60
    def test_cast_to_decimal128v3_19_9_from_int128_overflow_data_end_index = 64
    for (int data_index = test_cast_to_decimal128v3_19_9_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_9_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128v3_19_9_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128v3_19_9_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_19_18_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_int128_overflow values (64, -170141183460469231731687303715884105728),(65, -10),(66, 10),(67, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_19_18_from_int128_overflow_data_start_index = 64
    def test_cast_to_decimal128v3_19_18_from_int128_overflow_data_end_index = 68
    for (int data_index = test_cast_to_decimal128v3_19_18_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_19_18_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_0_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_38_0_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_0_from_int128_overflow values (68, -170141183460469231731687303715884105728),(69, -100000000000000000000000000000000000000),(70, 100000000000000000000000000000000000000),(71, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_0_from_int128_overflow_data_start_index = 68
    def test_cast_to_decimal128v3_38_0_from_int128_overflow_data_end_index = 72
    for (int data_index = test_cast_to_decimal128v3_38_0_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_0_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_38_0_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_38_0_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_19_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_38_19_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_19_from_int128_overflow values (72, -170141183460469231731687303715884105728),(73, -10000000000000000000),(74, 10000000000000000000),(75, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_19_from_int128_overflow_data_start_index = 72
    def test_cast_to_decimal128v3_38_19_from_int128_overflow_data_end_index = 76
    for (int data_index = test_cast_to_decimal128v3_38_19_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_19_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_38_19_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_38_19_from_int128_overflow order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_int128_overflow;"
    sql "create table test_cast_to_decimal128v3_38_37_from_int128_overflow(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_int128_overflow values (76, -170141183460469231731687303715884105728),(77, -10),(78, 10),(79, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128v3_38_37_from_int128_overflow_data_start_index = 76
    def test_cast_to_decimal128v3_38_37_from_int128_overflow_data_end_index = 80
    for (int data_index = test_cast_to_decimal128v3_38_37_from_int128_overflow_data_start_index; data_index < test_cast_to_decimal128v3_38_37_from_int128_overflow_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int128_overflow where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_int128_overflow order by 1;'

}