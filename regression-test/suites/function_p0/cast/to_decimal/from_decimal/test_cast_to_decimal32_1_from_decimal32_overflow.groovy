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


suite("test_cast_to_decimal32_1_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2 values (0, "10"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_0_overflow_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3 values (0, "9.9"),(1, "9.5"),(2, "10.9"),(3, "10.5"),(4, "99999998.9"),(5, "99999998.5"),(6, "99999999.9"),(7, "99999999.5");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_1_overflow_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4;"
    sql "create table test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4 values (0, "9.99999999"),(1, "9.50000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4_data_start_index = 0
    def test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4_data_start_index; data_index < test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal_1_0_from_decimal_9_8_overflow_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6 values (0, "1"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_1_0_overflow_6 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8 values (0, "1"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_0_overflow_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9 values (0, "1.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9_data_end_index = 3
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_1_overflow_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10 values (0, "0.99999999"),(1, "0.95000000"),(2, "1.99999999"),(3, "1.95000000"),(4, "8.99999999"),(5, "8.95000000"),(6, "9.99999999"),(7, "9.95000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10_data_end_index = 8
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_8_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11;"
    sql "create table test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11 values (0, "0.999999999"),(1, "0.950000000");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11_data_start_index = 0
    def test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11_data_end_index = 2
    for (int data_index = test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11_data_start_index; data_index < test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal_1_1_from_decimal_9_9_overflow_11 order by 1;'

}