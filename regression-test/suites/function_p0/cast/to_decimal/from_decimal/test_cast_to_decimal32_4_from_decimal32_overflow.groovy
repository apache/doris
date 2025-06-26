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


suite("test_cast_to_decimal32_4_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7 values (0, "10000"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_overflow_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8 values (0, "9999.9"),(1, "9999.9"),(2, "10000.9"),(3, "10000.9"),(4, "9999998.9"),(5, "9999998.9"),(6, "9999999.9"),(7, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_overflow_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9 values (0, "9999.9999"),(1, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_overflow_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12 values (0, "10000"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13 values (0, "9999.9"),(1, "9999.9"),(2, "10000.9"),(3, "10000.9"),(4, "99999998.9"),(5, "99999998.9"),(6, "99999999.9"),(7, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_overflow_13 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14 values (0, "9999.9999"),(1, "9999.9999"),(2, "10000.9999"),(3, "10000.9999"),(4, "99998.9999"),(5, "99998.9999"),(6, "99999.9999"),(7, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_overflow_14 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19 values (0, "1000"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_4_0_overflow_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24 values (0, "1000"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_0_overflow_24 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25 values (0, "1000.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_1_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26 values (0, "999.9999"),(1, "999.9999"),(2, "1000.9999"),(3, "1000.9999"),(4, "9998.9999"),(5, "9998.9999"),(6, "9999.9999"),(7, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_8_4_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29 values (0, "1000"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_0_overflow_29 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30 values (0, "1000.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_1_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31 values (0, "999.9999"),(1, "999.9999"),(2, "1000.9999"),(3, "1000.9999"),(4, "99998.9999"),(5, "99998.9999"),(6, "99999.9999"),(7, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_9_4_overflow_31 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36 values (0, "100"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_4_0_overflow_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37 values (0, "100.9"),(1, "998.9"),(2, "999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_4_1_overflow_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41 values (0, "100"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_0_overflow_41 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42 values (0, "100.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_1_overflow_42 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43 values (0, "99.9999"),(1, "99.9999"),(2, "100.9999"),(3, "100.9999"),(4, "9998.9999"),(5, "9998.9999"),(6, "9999.9999"),(7, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_43_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_8_4_overflow_43 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46 values (0, "100"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_0_overflow_46 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47 values (0, "100.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_1_overflow_47 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48 values (0, "99.9999"),(1, "99.9999"),(2, "100.9999"),(3, "100.9999"),(4, "99998.9999"),(5, "99998.9999"),(6, "99999.9999"),(7, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_9_4_overflow_48 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53 values (0, "10"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_0_overflow_53 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54 values (0, "10.9"),(1, "998.9"),(2, "999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_1_overflow_54 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55 values (0, "10.99"),(1, "98.99"),(2, "99.99");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_4_2_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58 values (0, "10"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_0_overflow_58 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59 values (0, "10.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_1_overflow_59 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60 values (0, "9.9999"),(1, "9.9999"),(2, "10.9999"),(3, "10.9999"),(4, "9998.9999"),(5, "9998.9999"),(6, "9999.9999"),(7, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_4_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61 values (0, "9.9999999"),(1, "9.9999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_8_7_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63 values (0, "10"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_0_overflow_63 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64 values (0, "10.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_1_overflow_64 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65 values (0, "9.9999"),(1, "9.9999"),(2, "10.9999"),(3, "10.9999"),(4, "99998.9999"),(5, "99998.9999"),(6, "99999.9999"),(7, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_4_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66 values (0, "9.99999999"),(1, "9.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_9_8_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68 values (0, "1"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_1_0_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70 values (0, "1"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71 values (0, "1.9"),(1, "998.9"),(2, "999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72 values (0, "1.99"),(1, "98.99"),(2, "99.99");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_2_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73 values (0, "1.999"),(1, "8.999"),(2, "9.999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_4_3_overflow_73 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75 values (0, "1"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_0_overflow_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76 values (0, "1.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_1_overflow_76 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77 values (0, "1.9999"),(1, "9998.9999"),(2, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_4_overflow_77 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78 values (0, "0.9999999"),(1, "0.9999999"),(2, "1.9999999"),(3, "1.9999999"),(4, "8.9999999"),(5, "8.9999999"),(6, "9.9999999"),(7, "9.9999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_7_overflow_78 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79 values (0, "0.99999999"),(1, "0.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_79_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_8_8_overflow_79 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80 values (0, "1"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_0_overflow_80 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81 values (0, "1.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_1_overflow_81 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82 values (0, "1.9999"),(1, "99998.9999"),(2, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_4_overflow_82 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83 values (0, "0.99999999"),(1, "0.99999999"),(2, "1.99999999"),(3, "1.99999999"),(4, "8.99999999"),(5, "8.99999999"),(6, "9.99999999"),(7, "9.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_8_overflow_83 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84 values (0, "0.999999999"),(1, "0.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_9_9_overflow_84 order by 1;'

}