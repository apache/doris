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


suite("test_cast_to_decimal64_18_from_decimalv2_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2;"
    sql "create table test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2 values (0, "999999999999999999.999999999"),(1, "999999999999999999.500000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2_data_start_index = 0
    def test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2_data_end_index = 2
    for (int data_index = test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2_data_start_index; data_index < test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal_18_0_from_decimalv2_27_9_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6;"
    sql "create table test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6 values (0, "99999999999999999.999999999"),(1, "99999999999999999.950000000"),(2, "100000000000000000.999999999"),(3, "100000000000000000.950000000"),(4, "999999999999999998.999999999"),(5, "999999999999999998.950000000"),(6, "999999999999999999.999999999"),(7, "999999999999999999.950000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6_data_start_index = 0
    def test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6_data_end_index = 8
    for (int data_index = test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6_data_start_index; data_index < test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal_18_1_from_decimalv2_27_9_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10 values (0, "10.999999999"),(1, "999999999999999998.999999999"),(2, "999999999999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10_data_start_index = 0
    def test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10_data_end_index = 3
    for (int data_index = test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10_data_start_index; data_index < test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11 values (0, "10.999999"),(1, "99999999999998.999999"),(2, "99999999999999.999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11_data_start_index = 0
    def test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11_data_end_index = 3
    for (int data_index = test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11_data_start_index; data_index < test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_6_overflow_11 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12;"
    sql "create table test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12 values (0, "1"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12_data_start_index = 0
    def test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12_data_end_index = 3
    for (int data_index = test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12_data_start_index; data_index < test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_1_0_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14;"
    sql "create table test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14 values (0, "1.999999999"),(1, "999999999999999998.999999999"),(2, "999999999999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14_data_start_index = 0
    def test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14_data_end_index = 3
    for (int data_index = test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14_data_start_index; data_index < test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_27_9_overflow_14 order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15;"
    sql "create table test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15 values (0, "1.999999"),(1, "99999999999998.999999"),(2, "99999999999999.999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15_data_start_index = 0
    def test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15_data_end_index = 3
    for (int data_index = test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15_data_start_index; data_index < test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal_18_18_from_decimalv2_20_6_overflow_15 order by 1;'

}