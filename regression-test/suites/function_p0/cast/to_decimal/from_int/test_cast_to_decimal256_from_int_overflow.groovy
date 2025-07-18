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


suite("test_cast_to_decimal256_from_int_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_0;"
    sql "create table test_cast_to_decimal256_from_int_overflow_0(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_0 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_0_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_0_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_0_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_3;"
    sql "create table test_cast_to_decimal256_from_int_overflow_3(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_3 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_3_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_3_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_3_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_6;"
    sql "create table test_cast_to_decimal256_from_int_overflow_6(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_6 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_6_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_6_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_6_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_7;"
    sql "create table test_cast_to_decimal256_from_int_overflow_7(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_7 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_7_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_7_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_7_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_10;"
    sql "create table test_cast_to_decimal256_from_int_overflow_10(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_10 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_10_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_10_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_10_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_13;"
    sql "create table test_cast_to_decimal256_from_int_overflow_13(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_13 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_13_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_13_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_13_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_13_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_13 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_13 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_14;"
    sql "create table test_cast_to_decimal256_from_int_overflow_14(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_14 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_14_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_14_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_14_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_14_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_14 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_14 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_17;"
    sql "create table test_cast_to_decimal256_from_int_overflow_17(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_17 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_17_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_17_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_17_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_20;"
    sql "create table test_cast_to_decimal256_from_int_overflow_20(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_20 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_20_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_20_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_20_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_21;"
    sql "create table test_cast_to_decimal256_from_int_overflow_21(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_21 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_21_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_21_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_21_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_21_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_21 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_21 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_24;"
    sql "create table test_cast_to_decimal256_from_int_overflow_24(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_24 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_24_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_24_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_24_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_24_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_24 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_24 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_27;"
    sql "create table test_cast_to_decimal256_from_int_overflow_27(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_27 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_27_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_27_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_27_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_27_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_27 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_27 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_28;"
    sql "create table test_cast_to_decimal256_from_int_overflow_28(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_28 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_28_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_28_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_28_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_28_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_28 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_overflow_28 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_29;"
    sql "create table test_cast_to_decimal256_from_int_overflow_29(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_29 values (0, "-170141183460469231731687303715884105728"),(1, "-100000000000000000000000000000000000000"),(2, "100000000000000000000000000000000000000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_29_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_29_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_29_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_29_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_overflow_29 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_overflow_29 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_30;"
    sql "create table test_cast_to_decimal256_from_int_overflow_30(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_30 values (0, "-170141183460469231731687303715884105728"),(1, "-10000000000000000000"),(2, "10000000000000000000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_30_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_30_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_30_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_31;"
    sql "create table test_cast_to_decimal256_from_int_overflow_31(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_31 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_31_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_31_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_31_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_overflow_31 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_33;"
    sql "create table test_cast_to_decimal256_from_int_overflow_33(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_33 values (0, "-170141183460469231731687303715884105728"),(1, "-100000000000000000000000000000000000000"),(2, "100000000000000000000000000000000000000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_33_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_33_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_33_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_33_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_overflow_33 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_overflow_33 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_overflow_34;"
    sql "create table test_cast_to_decimal256_from_int_overflow_34(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_overflow_34 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_from_int_overflow_34_data_start_index = 0
    def test_cast_to_decimal256_from_int_overflow_34_data_end_index = 4
    for (int data_index = test_cast_to_decimal256_from_int_overflow_34_data_start_index; data_index < test_cast_to_decimal256_from_int_overflow_34_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_34 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_overflow_34 order by 1;'

}