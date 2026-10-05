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


suite("test_cast_to_decimal32_9_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0;"
    sql "create table test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0 values (0, "1000000000"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0_data_start_index = 0
    def test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0_data_start_index; data_index < test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_10_0_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1;"
    sql "create table test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1 values (0, "999999999.9"),(1, "999999999.5");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1_data_start_index = 0
    def test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1_data_end_index = 2
    for (int data_index = test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1_data_start_index; data_index < test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_10_1_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4;"
    sql "create table test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4 values (0, "1000000000"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4_data_start_index = 0
    def test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4_data_start_index; data_index < test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_18_0_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5;"
    sql "create table test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5 values (0, "999999999.9"),(1, "999999999.5"),(2, "1000000000.9"),(3, "1000000000.5"),(4, "99999999999999998.9"),(5, "99999999999999998.5"),(6, "99999999999999999.9"),(7, "99999999999999999.5");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5_data_start_index = 0
    def test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5_data_end_index = 8
    for (int data_index = test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5_data_start_index; data_index < test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimal_18_1_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8;"
    sql "create table test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8 values (0, "100000000"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8_data_start_index = 0
    def test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8_data_start_index; data_index < test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_10_0_overflow_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9;"
    sql "create table test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9 values (0, "100000000.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9_data_start_index = 0
    def test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9_data_start_index; data_index < test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_10_1_overflow_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12;"
    sql "create table test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12 values (0, "100000000"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12_data_start_index = 0
    def test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12_data_start_index; data_index < test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_18_0_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13;"
    sql "create table test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13 values (0, "100000000.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13_data_start_index = 0
    def test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13_data_start_index; data_index < test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal_9_1_from_decimal_18_1_overflow_13 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16 values (0, "10"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_0_overflow_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17 values (0, "10.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_1_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18 values (0, "9.999999999"),(1, "9.999999995");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18_data_end_index = 2
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_10_9_overflow_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20 values (0, "10"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_0_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21 values (0, "10.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_1_overflow_21 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22;"
    sql "create table test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22 values (0, "9.99999999999999999"),(1, "9.99999999500000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22_data_start_index = 0
    def test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22_data_end_index = 2
    for (int data_index = test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22_data_start_index; data_index < test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal_9_8_from_decimal_18_17_overflow_22 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24 values (0, "1"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_0_overflow_24 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25 values (0, "1.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_1_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26 values (0, "1.999999999"),(1, "8.999999999"),(2, "9.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_9_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27 values (0, "0.9999999999"),(1, "0.9999999995");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27_data_end_index = 2
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_10_10_overflow_27 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28 values (0, "1"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_0_overflow_28 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29 values (0, "1.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29_data_end_index = 3
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_1_overflow_29 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30 values (0, "0.99999999999999999"),(1, "0.99999999950000000"),(2, "1.99999999999999999"),(3, "1.99999999950000000"),(4, "8.99999999999999999"),(5, "8.99999999950000000"),(6, "9.99999999999999999"),(7, "9.99999999950000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30_data_end_index = 8
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_17_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31;"
    sql "create table test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31 values (0, "0.999999999999999999"),(1, "0.999999999500000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31_data_start_index = 0
    def test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31_data_end_index = 2
    for (int data_index = test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31_data_start_index; data_index < test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal_9_9_from_decimal_18_18_overflow_31 order by 1;'

}