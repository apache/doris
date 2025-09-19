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


suite("test_cast_to_decimal32_4_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0 values (0, "10000"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_0_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1 values (0, "9999.9"),(1, "9999.9"),(2, "10000.9"),(3, "10000.9"),(4, "999999999999999998.9"),(5, "999999999999999998.9"),(6, "999999999999999999.9"),(7, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_1_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2 values (0, "9999.999999999"),(1, "9999.999999999"),(2, "10000.999999999"),(3, "10000.999999999"),(4, "9999999998.999999999"),(5, "9999999998.999999999"),(6, "9999999999.999999999"),(7, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_19_9_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5 values (0, "10000"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_0_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6 values (0, "9999.9"),(1, "9999.9"),(2, "10000.9"),(3, "10000.9"),(4, "999999999999999999999999999999999998.9"),(5, "999999999999999999999999999999999998.9"),(6, "999999999999999999999999999999999999.9"),(7, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_1_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7 values (0, "9999.999999999999999999"),(1, "9999.999999999999999999"),(2, "10000.999999999999999999"),(3, "10000.999999999999999999"),(4, "9999999999999999998.999999999999999999"),(5, "9999999999999999998.999999999999999999"),(6, "9999999999999999999.999999999999999999"),(7, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_37_18_overflow_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10 values (0, "10000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_0_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11 values (0, "9999.9"),(1, "9999.9"),(2, "10000.9"),(3, "10000.9"),(4, "9999999999999999999999999999999999998.9"),(5, "9999999999999999999999999999999999998.9"),(6, "9999999999999999999999999999999999999.9"),(7, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_1_overflow_11 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12 values (0, "9999.9999999999999999999"),(1, "9999.9999999999999999999"),(2, "10000.9999999999999999999"),(3, "10000.9999999999999999999"),(4, "9999999999999999998.9999999999999999999"),(5, "9999999999999999998.9999999999999999999"),(6, "9999999999999999999.9999999999999999999"),(7, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12_data_start_index = 0
    def test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12_data_start_index; data_index < test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_38_19_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15 values (0, "1000"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_0_overflow_15 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16 values (0, "1000.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_1_overflow_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17 values (0, "999.999999999"),(1, "999.999999999"),(2, "1000.999999999"),(3, "1000.999999999"),(4, "9999999998.999999999"),(5, "9999999998.999999999"),(6, "9999999999.999999999"),(7, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_19_9_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20 values (0, "1000"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_0_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21 values (0, "1000.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_1_overflow_21 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22 values (0, "999.999999999999999999"),(1, "999.999999999999999999"),(2, "1000.999999999999999999"),(3, "1000.999999999999999999"),(4, "9999999999999999998.999999999999999999"),(5, "9999999999999999998.999999999999999999"),(6, "9999999999999999999.999999999999999999"),(7, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_37_18_overflow_22 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25 values (0, "1000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_0_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26 values (0, "1000.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_1_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27;"
    sql "create table test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27 values (0, "999.9999999999999999999"),(1, "999.9999999999999999999"),(2, "1000.9999999999999999999"),(3, "1000.9999999999999999999"),(4, "9999999999999999998.9999999999999999999"),(5, "9999999999999999998.9999999999999999999"),(6, "9999999999999999999.9999999999999999999"),(7, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27_data_start_index = 0
    def test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27_data_start_index; data_index < test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal_4_1_from_decimal_38_19_overflow_27 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30 values (0, "100"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_0_overflow_30 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31 values (0, "100.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_1_overflow_31 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32 values (0, "99.999999999"),(1, "99.999999999"),(2, "100.999999999"),(3, "100.999999999"),(4, "9999999998.999999999"),(5, "9999999998.999999999"),(6, "9999999999.999999999"),(7, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_19_9_overflow_32 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35 values (0, "100"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_0_overflow_35 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36 values (0, "100.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_1_overflow_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37 values (0, "99.999999999999999999"),(1, "99.999999999999999999"),(2, "100.999999999999999999"),(3, "100.999999999999999999"),(4, "9999999999999999998.999999999999999999"),(5, "9999999999999999998.999999999999999999"),(6, "9999999999999999999.999999999999999999"),(7, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_37_18_overflow_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40 values (0, "100"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_0_overflow_40 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41 values (0, "100.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_1_overflow_41 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42 values (0, "99.9999999999999999999"),(1, "99.9999999999999999999"),(2, "100.9999999999999999999"),(3, "100.9999999999999999999"),(4, "9999999999999999998.9999999999999999999"),(5, "9999999999999999998.9999999999999999999"),(6, "9999999999999999999.9999999999999999999"),(7, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42_data_start_index = 0
    def test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42_data_start_index; data_index < test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_38_19_overflow_42 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45 values (0, "10"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_0_overflow_45 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46 values (0, "10.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_1_overflow_46 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47 values (0, "9.999999999"),(1, "9.999999999"),(2, "10.999999999"),(3, "10.999999999"),(4, "9999999998.999999999"),(5, "9999999998.999999999"),(6, "9999999999.999999999"),(7, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_9_overflow_47 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48 values (0, "9.999999999999999999"),(1, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_19_18_overflow_48 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50 values (0, "10"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_0_overflow_50 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51 values (0, "10.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_1_overflow_51 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52 values (0, "9.999999999999999999"),(1, "9.999999999999999999"),(2, "10.999999999999999999"),(3, "10.999999999999999999"),(4, "9999999999999999998.999999999999999999"),(5, "9999999999999999998.999999999999999999"),(6, "9999999999999999999.999999999999999999"),(7, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_18_overflow_52 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53 values (0, "9.999999999999999999999999999999999999"),(1, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_37_36_overflow_53 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55 values (0, "10"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_0_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56 values (0, "10.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_1_overflow_56 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57 values (0, "9.9999999999999999999"),(1, "9.9999999999999999999"),(2, "10.9999999999999999999"),(3, "10.9999999999999999999"),(4, "9999999999999999998.9999999999999999999"),(5, "9999999999999999998.9999999999999999999"),(6, "9999999999999999999.9999999999999999999"),(7, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_19_overflow_57 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58;"
    sql "create table test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58 values (0, "9.9999999999999999999999999999999999999"),(1, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58_data_start_index = 0
    def test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58_data_start_index; data_index < test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal_4_3_from_decimal_38_37_overflow_58 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60 values (0, "1"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_0_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61 values (0, "1.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_1_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62 values (0, "0.999999999"),(1, "0.999999999"),(2, "1.999999999"),(3, "1.999999999"),(4, "9999999998.999999999"),(5, "9999999998.999999999"),(6, "9999999999.999999999"),(7, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_9_overflow_62 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63 values (0, "0.999999999999999999"),(1, "0.999999999999999999"),(2, "1.999999999999999999"),(3, "1.999999999999999999"),(4, "8.999999999999999999"),(5, "8.999999999999999999"),(6, "9.999999999999999999"),(7, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_18_overflow_63 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64 values (0, "0.9999999999999999999"),(1, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_19_19_overflow_64 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65 values (0, "1"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_0_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66 values (0, "1.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_1_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67 values (0, "0.999999999999999999"),(1, "0.999999999999999999"),(2, "1.999999999999999999"),(3, "1.999999999999999999"),(4, "9999999999999999998.999999999999999999"),(5, "9999999999999999998.999999999999999999"),(6, "9999999999999999999.999999999999999999"),(7, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_18_overflow_67 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68 values (0, "0.999999999999999999999999999999999999"),(1, "0.999999999999999999999999999999999999"),(2, "1.999999999999999999999999999999999999"),(3, "1.999999999999999999999999999999999999"),(4, "8.999999999999999999999999999999999999"),(5, "8.999999999999999999999999999999999999"),(6, "9.999999999999999999999999999999999999"),(7, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_36_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69 values (0, "0.9999999999999999999999999999999999999"),(1, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_37_37_overflow_69 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70 values (0, "1"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71 values (0, "1.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72 values (0, "0.9999999999999999999"),(1, "0.9999999999999999999"),(2, "1.9999999999999999999"),(3, "1.9999999999999999999"),(4, "9999999999999999998.9999999999999999999"),(5, "9999999999999999998.9999999999999999999"),(6, "9999999999999999999.9999999999999999999"),(7, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_19_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73 values (0, "0.9999999999999999999999999999999999999"),(1, "0.9999999999999999999999999999999999999"),(2, "1.9999999999999999999999999999999999999"),(3, "1.9999999999999999999999999999999999999"),(4, "8.9999999999999999999999999999999999999"),(5, "8.9999999999999999999999999999999999999"),(6, "9.9999999999999999999999999999999999999"),(7, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73_data_end_index = 8
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_37_overflow_73 order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74;"
    sql "create table test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74 values (0, "0.99999999999999999999999999999999999999"),(1, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74_data_start_index = 0
    def test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74_data_end_index = 2
    for (int data_index = test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74_data_start_index; data_index < test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74_non_strict 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal_4_4_from_decimal_38_38_overflow_74 order by 1;'

}