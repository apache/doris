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


suite("test_cast_to_decimal128i_38_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5 values (0, "100000000000000000000000000000000000000"),(1, "999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_39_0_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6 values (0, "99999999999999999999999999999999999999.9"),(1, "99999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_39_1_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10 values (0, "100000000000000000000000000000000000000"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_0_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11 values (0, "99999999999999999999999999999999999999.9"),(1, "99999999999999999999999999999999999999.9"),(2, "100000000000000000000000000000000000000.9"),(3, "100000000000000000000000000000000000000.9"),(4, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(5, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(6, "99999999999999999999999999999999999999999999999999999999999999999999999999.9"),(7, "99999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_1_overflow_11 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12 values (0, "99999999999999999999999999999999999999.9999999999999999999999999999999999999"),(1, "99999999999999999999999999999999999999.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_75_37_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15 values (0, "100000000000000000000000000000000000000"),(1, "9999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_0_overflow_15 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16 values (0, "99999999999999999999999999999999999999.9"),(1, "99999999999999999999999999999999999999.9"),(2, "100000000000000000000000000000000000000.9"),(3, "100000000000000000000000000000000000000.9"),(4, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(5, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(6, "999999999999999999999999999999999999999999999999999999999999999999999999999.9"),(7, "999999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_1_overflow_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17;"
    sql "create table test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17 values (0, "99999999999999999999999999999999999999.99999999999999999999999999999999999999"),(1, "99999999999999999999999999999999999999.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17_data_start_index = 0
    def test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17_data_start_index; data_index < test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal_38_0_from_decimal_76_38_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20 values (0, "10000000000000000000000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_38_0_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25 values (0, "10000000000000000000000000000000000000"),(1, "999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_39_0_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26 values (0, "10000000000000000000000000000000000000.9"),(1, "99999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_39_1_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30 values (0, "10000000000000000000000000000000000000"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_0_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31 values (0, "10000000000000000000000000000000000000.9"),(1, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_1_overflow_31 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32 values (0, "9999999999999999999999999999999999999.9999999999999999999999999999999999999"),(1, "9999999999999999999999999999999999999.9999999999999999999999999999999999999"),(2, "10000000000000000000000000000000000000.9999999999999999999999999999999999999"),(3, "10000000000000000000000000000000000000.9999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(5, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(6, "99999999999999999999999999999999999999.9999999999999999999999999999999999999"),(7, "99999999999999999999999999999999999999.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_75_37_overflow_32 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35 values (0, "10000000000000000000000000000000000000"),(1, "9999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_0_overflow_35 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36 values (0, "10000000000000000000000000000000000000.9"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_1_overflow_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37 values (0, "9999999999999999999999999999999999999.99999999999999999999999999999999999999"),(1, "9999999999999999999999999999999999999.99999999999999999999999999999999999999"),(2, "10000000000000000000000000000000000000.99999999999999999999999999999999999999"),(3, "10000000000000000000000000000000000000.99999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(5, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(6, "99999999999999999999999999999999999999.99999999999999999999999999999999999999"),(7, "99999999999999999999999999999999999999.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_76_38_overflow_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40 values (0, "10000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_38_0_overflow_40 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41 values (0, "10000000000000000000.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_38_1_overflow_41 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45 values (0, "10000000000000000000"),(1, "999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_0_overflow_45 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46 values (0, "10000000000000000000.9"),(1, "99999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_1_overflow_46 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47 values (0, "10000000000000000000.9999999999999999999"),(1, "99999999999999999998.9999999999999999999"),(2, "99999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_39_19_overflow_47 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50 values (0, "10000000000000000000"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_0_overflow_50 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51 values (0, "10000000000000000000.9"),(1, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_1_overflow_51 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52 values (0, "9999999999999999999.9999999999999999999999999999999999999"),(1, "9999999999999999999.9999999999999999999999999999999999999"),(2, "10000000000000000000.9999999999999999999999999999999999999"),(3, "10000000000000000000.9999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(5, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(6, "99999999999999999999999999999999999999.9999999999999999999999999999999999999"),(7, "99999999999999999999999999999999999999.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_75_37_overflow_52 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55 values (0, "10000000000000000000"),(1, "9999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_0_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56 values (0, "10000000000000000000.9"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_1_overflow_56 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57;"
    sql "create table test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57 values (0, "9999999999999999999.99999999999999999999999999999999999999"),(1, "9999999999999999999.99999999999999999999999999999999999999"),(2, "10000000000000000000.99999999999999999999999999999999999999"),(3, "10000000000000000000.99999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(5, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(6, "99999999999999999999999999999999999999.99999999999999999999999999999999999999"),(7, "99999999999999999999999999999999999999.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57_data_start_index = 0
    def test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57_data_start_index; data_index < test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal_38_19_from_decimal_76_38_overflow_57 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60 values (0, "10"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_0_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61 values (0, "10.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_1_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62 values (0, "10.9999999999999999999"),(1, "9999999999999999998.9999999999999999999"),(2, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_19_overflow_62 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65 values (0, "10"),(1, "999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_0_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66 values (0, "10.9"),(1, "99999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_1_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67 values (0, "10.9999999999999999999"),(1, "99999999999999999998.9999999999999999999"),(2, "99999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_19_overflow_67 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68 values (0, "9.99999999999999999999999999999999999999"),(1, "9.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_39_38_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70 values (0, "10"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71 values (0, "10.9"),(1, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72 values (0, "10.9999999999999999999999999999999999999"),(1, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(2, "99999999999999999999999999999999999999.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_37_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73 values (0, "9.99999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "9.99999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_75_74_overflow_73 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75 values (0, "10"),(1, "9999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_0_overflow_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76 values (0, "10.9"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_1_overflow_76 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77 values (0, "9.99999999999999999999999999999999999999"),(1, "9.99999999999999999999999999999999999999"),(2, "10.99999999999999999999999999999999999999"),(3, "10.99999999999999999999999999999999999999"),(4, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(5, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(6, "99999999999999999999999999999999999999.99999999999999999999999999999999999999"),(7, "99999999999999999999999999999999999999.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_38_overflow_77 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78 values (0, "9.999999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "9.999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_76_75_overflow_78 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80 values (0, "1"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_0_overflow_80 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81 values (0, "1.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_1_overflow_81 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82 values (0, "1.9999999999999999999"),(1, "9999999999999999998.9999999999999999999"),(2, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_19_overflow_82 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83 values (0, "1.9999999999999999999999999999999999999"),(1, "8.9999999999999999999999999999999999999"),(2, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_37_overflow_83 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85 values (0, "1"),(1, "999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_0_overflow_85 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86 values (0, "1.9"),(1, "99999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_1_overflow_86 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87 values (0, "1.9999999999999999999"),(1, "99999999999999999998.9999999999999999999"),(2, "99999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_19_overflow_87 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88 values (0, "1.99999999999999999999999999999999999999"),(1, "8.99999999999999999999999999999999999999"),(2, "9.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_38_overflow_88 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89 values (0, "0.999999999999999999999999999999999999999"),(1, "0.999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_39_39_overflow_89 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90 values (0, "1"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_0_overflow_90 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91 values (0, "1.9"),(1, "99999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "99999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_1_overflow_91 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92 values (0, "1.9999999999999999999999999999999999999"),(1, "99999999999999999999999999999999999998.9999999999999999999999999999999999999"),(2, "99999999999999999999999999999999999999.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_37_overflow_92 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93 values (0, "0.99999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "0.99999999999999999999999999999999999999999999999999999999999999999999999999"),(2, "1.99999999999999999999999999999999999999999999999999999999999999999999999999"),(3, "1.99999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "8.99999999999999999999999999999999999999999999999999999999999999999999999999"),(5, "8.99999999999999999999999999999999999999999999999999999999999999999999999999"),(6, "9.99999999999999999999999999999999999999999999999999999999999999999999999999"),(7, "9.99999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_74_overflow_93 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94 values (0, "0.999999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "0.999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_75_75_overflow_94 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95 values (0, "1"),(1, "9999999999999999999999999999999999999999999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_0_overflow_95 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96 values (0, "1.9"),(1, "999999999999999999999999999999999999999999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999999999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_1_overflow_96 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97 values (0, "1.99999999999999999999999999999999999999"),(1, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),(2, "99999999999999999999999999999999999999.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_38_overflow_97 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98 values (0, "0.999999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "0.999999999999999999999999999999999999999999999999999999999999999999999999999"),(2, "1.999999999999999999999999999999999999999999999999999999999999999999999999999"),(3, "1.999999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "8.999999999999999999999999999999999999999999999999999999999999999999999999999"),(5, "8.999999999999999999999999999999999999999999999999999999999999999999999999999"),(6, "9.999999999999999999999999999999999999999999999999999999999999999999999999999"),(7, "9.999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98_data_end_index = 8
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_75_overflow_98 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99 values (0, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),(1, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99_data_end_index = 2
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_76_76_overflow_99 order by 1;'

}