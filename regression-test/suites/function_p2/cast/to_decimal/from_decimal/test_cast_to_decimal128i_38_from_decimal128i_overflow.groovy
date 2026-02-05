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


suite("test_cast_to_decimal128i_38_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12;"
    sql "create table test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12 values (0, "10000000000000000000000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12_data_start_index = 0
    def test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12_data_start_index; data_index < test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimal_38_0_overflow_12 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16 values (0, "10"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_19_0_overflow_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17 values (0, "10.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_19_1_overflow_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20 values (0, "10"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_0_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21;"
    sql "create table test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21 values (0, "10.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21_data_start_index = 0
    def test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21_data_start_index; data_index < test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal_38_37_from_decimal_38_1_overflow_21 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24 values (0, "1"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_0_overflow_24 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25 values (0, "1.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_1_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26 values (0, "1.999999999999999999"),(1, "8.999999999999999999"),(2, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_19_18_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28 values (0, "1"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_0_overflow_28 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29 values (0, "1.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_1_overflow_29 order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30;"
    sql "create table test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30 values (0, "1.9999999999999999999999999999999999999"),(1, "8.9999999999999999999999999999999999999"),(2, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30_data_start_index = 0
    def test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30_data_end_index = 3
    for (int data_index = test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30_data_start_index; data_index < test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal_38_38_from_decimal_38_37_overflow_30 order by 1;'

}