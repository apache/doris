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


suite("test_cast_to_decimal32_1_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0 values (0, "10"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_0_overflow_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1 values (0, "9.9"),(1, "9.9"),(2, "10.9"),(3, "10.9"),(4, "999999999999999998.9"),(5, "999999999999999998.9"),(6, "999999999999999999.9"),(7, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_1_overflow_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2 values (0, "9.999999999999999999"),(1, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_19_18_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4 values (0, "10"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_0_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5 values (0, "9.9"),(1, "9.9"),(2, "10.9"),(3, "10.9"),(4, "9999999999999999999999999999999999998.9"),(5, "9999999999999999999999999999999999998.9"),(6, "9999999999999999999999999999999999999.9"),(7, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_1_overflow_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6 values (0, "9.9999999999999999999999999999999999999"),(1, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_38_37_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8 values (0, "1"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_0_overflow_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9 values (0, "1.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_1_overflow_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10 values (0, "0.999999999999999999"),(1, "0.999999999999999999"),(2, "1.999999999999999999"),(3, "1.999999999999999999"),(4, "8.999999999999999999"),(5, "8.999999999999999999"),(6, "9.999999999999999999"),(7, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_18_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11 values (0, "0.9999999999999999999"),(1, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_19_19_overflow_11 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12 values (0, "1"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_0_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13 values (0, "1.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_1_overflow_13 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14 values (0, "0.9999999999999999999999999999999999999"),(1, "0.9999999999999999999999999999999999999"),(2, "1.9999999999999999999999999999999999999"),(3, "1.9999999999999999999999999999999999999"),(4, "8.9999999999999999999999999999999999999"),(5, "8.9999999999999999999999999999999999999"),(6, "9.9999999999999999999999999999999999999"),(7, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_37_overflow_14 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15 values (0, "0.99999999999999999999999999999999999999"),(1, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_38_38_overflow_15 order by 1;'

}