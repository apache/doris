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
    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_0;"
    sql "create table test_cast_to_decimal32_from_int_overflow_0(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_0 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_0_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_0_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_0_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_2;"
    sql "create table test_cast_to_decimal32_from_int_overflow_2(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_2 values (0, "-128"),(1, "-100"),(2, "100"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_2_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_2_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_2_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_3;"
    sql "create table test_cast_to_decimal32_from_int_overflow_3(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_3 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_3_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_3_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_3_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_6;"
    sql "create table test_cast_to_decimal32_from_int_overflow_6(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_6 values (0, "-128"),(1, "-10"),(2, "10"),(3, "127");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_6_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_6_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_6_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_7;"
    sql "create table test_cast_to_decimal32_from_int_overflow_7(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_7 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_7_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_7_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_7_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_8;"
    sql "create table test_cast_to_decimal32_from_int_overflow_8(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_8 values (0, "-32768"),(1, "-10000"),(2, "10000"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_8_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_8_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_8_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_9;"
    sql "create table test_cast_to_decimal32_from_int_overflow_9(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_9 values (0, "-32768"),(1, "-100"),(2, "100"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_9_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_9_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_9_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_10;"
    sql "create table test_cast_to_decimal32_from_int_overflow_10(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_10 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_10_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_10_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_10_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_13;"
    sql "create table test_cast_to_decimal32_from_int_overflow_13(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_13 values (0, "-32768"),(1, "-10"),(2, "10"),(3, "32767");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_13_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_13_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_13_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_13_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_13 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_13 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_14;"
    sql "create table test_cast_to_decimal32_from_int_overflow_14(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_14 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_14_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_14_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_14_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_14_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_14 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_14 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_15;"
    sql "create table test_cast_to_decimal32_from_int_overflow_15(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_15 values (0, "-2147483648"),(1, "-10000"),(2, "10000"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_15_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_15_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_15_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_15_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_15 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_15 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_16;"
    sql "create table test_cast_to_decimal32_from_int_overflow_16(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_16 values (0, "-2147483648"),(1, "-100"),(2, "100"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_16_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_16_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_16_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_17;"
    sql "create table test_cast_to_decimal32_from_int_overflow_17(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_17 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_17_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_17_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_17_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_18;"
    sql "create table test_cast_to_decimal32_from_int_overflow_18(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_18 values (0, "-2147483648"),(1, "-1000000000"),(2, "1000000000"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_18_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_18_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_18_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_19;"
    sql "create table test_cast_to_decimal32_from_int_overflow_19(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_19 values (0, "-2147483648"),(1, "-100000"),(2, "100000"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_19_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_19_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_19_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_20;"
    sql "create table test_cast_to_decimal32_from_int_overflow_20(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_20 values (0, "-2147483648"),(1, "-10"),(2, "10"),(3, "2147483647");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_20_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_20_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_20_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_21;"
    sql "create table test_cast_to_decimal32_from_int_overflow_21(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_21 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_21_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_21_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_21_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_21_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_21 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_21 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_22;"
    sql "create table test_cast_to_decimal32_from_int_overflow_22(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_22 values (0, "-9223372036854775808"),(1, "-10000"),(2, "10000"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_22_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_22_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_22_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_22_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_22 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_22 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_23;"
    sql "create table test_cast_to_decimal32_from_int_overflow_23(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_23 values (0, "-9223372036854775808"),(1, "-100"),(2, "100"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_23_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_23_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_23_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_23_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_23 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_23 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_24;"
    sql "create table test_cast_to_decimal32_from_int_overflow_24(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_24 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_24_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_24_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_24_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_24_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_24 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_24 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_25;"
    sql "create table test_cast_to_decimal32_from_int_overflow_25(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_25 values (0, "-9223372036854775808"),(1, "-1000000000"),(2, "1000000000"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_25_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_25_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_25_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_26;"
    sql "create table test_cast_to_decimal32_from_int_overflow_26(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_26 values (0, "-9223372036854775808"),(1, "-100000"),(2, "100000"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_26_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_26_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_26_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_27;"
    sql "create table test_cast_to_decimal32_from_int_overflow_27(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_27 values (0, "-9223372036854775808"),(1, "-10"),(2, "10"),(3, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_27_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_27_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_27_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_27_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_27 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_27 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_28;"
    sql "create table test_cast_to_decimal32_from_int_overflow_28(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_28 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_28_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_28_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_28_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_28_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_28 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_from_int_overflow_28 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_29;"
    sql "create table test_cast_to_decimal32_from_int_overflow_29(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_29 values (0, "-170141183460469231731687303715884105728"),(1, "-10000"),(2, "10000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_29_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_29_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_29_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_29_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_29 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_from_int_overflow_29 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_30;"
    sql "create table test_cast_to_decimal32_from_int_overflow_30(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_30 values (0, "-170141183460469231731687303715884105728"),(1, "-100"),(2, "100"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_30_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_30_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_30_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_from_int_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_31;"
    sql "create table test_cast_to_decimal32_from_int_overflow_31(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_31 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_31_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_31_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_31_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_from_int_overflow_31 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_32;"
    sql "create table test_cast_to_decimal32_from_int_overflow_32(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_32 values (0, "-170141183460469231731687303715884105728"),(1, "-1000000000"),(2, "1000000000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_32_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_32_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_32_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_32_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_32 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_from_int_overflow_32 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_33;"
    sql "create table test_cast_to_decimal32_from_int_overflow_33(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_33 values (0, "-170141183460469231731687303715884105728"),(1, "-100000"),(2, "100000"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_33_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_33_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_33_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_33_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_33 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_from_int_overflow_33 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_from_int_overflow_34;"
    sql "create table test_cast_to_decimal32_from_int_overflow_34(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_from_int_overflow_34 values (0, "-170141183460469231731687303715884105728"),(1, "-10"),(2, "10"),(3, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_from_int_overflow_34_data_start_index = 0
    def test_cast_to_decimal32_from_int_overflow_34_data_end_index = 4
    for (int data_index = test_cast_to_decimal32_from_int_overflow_34_data_start_index; data_index < test_cast_to_decimal32_from_int_overflow_34_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_34 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_from_int_overflow_34 order by 1;'

}