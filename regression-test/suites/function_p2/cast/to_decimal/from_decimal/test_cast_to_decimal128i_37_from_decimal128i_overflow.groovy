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


suite("test_cast_to_decimal128i_37_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10;"
    sql "create table test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10 values (0, "10000000000000000000000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10_data_start_index = 0
    def test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10_data_start_index; data_index < test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal_37_0_from_decimal_38_0_overflow_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11;"
    sql "create table test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11 values (0, "9999999999999999999999999999999999999.9"),(1, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11_data_start_index = 0
    def test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11_data_end_index = 2
    for (int data_index = test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11_data_start_index; data_index < test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal_37_0_from_decimal_38_1_overflow_11 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20;"
    sql "create table test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20 values (0, "1000000000000000000000000000000000000"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20_data_start_index = 0
    def test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20_data_start_index; data_index < test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_37_0_overflow_20 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25;"
    sql "create table test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25 values (0, "1000000000000000000000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25_data_start_index = 0
    def test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25_data_start_index; data_index < test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_38_0_overflow_25 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26;"
    sql "create table test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26 values (0, "1000000000000000000000000000000000000.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26_data_start_index = 0
    def test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26_data_start_index; data_index < test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal_37_1_from_decimal_38_1_overflow_26 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35;"
    sql "create table test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35 values (0, "10000000000000000000"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35_data_start_index = 0
    def test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35_data_start_index; data_index < test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35_non_strict 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_37_0_overflow_35 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36;"
    sql "create table test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36 values (0, "10000000000000000000.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36_data_start_index = 0
    def test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36_data_start_index; data_index < test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36_non_strict 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_37_1_overflow_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40;"
    sql "create table test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40 values (0, "10000000000000000000"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40_data_start_index = 0
    def test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40_data_start_index; data_index < test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40_non_strict 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_0_overflow_40 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41;"
    sql "create table test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41 values (0, "10000000000000000000.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41_data_start_index = 0
    def test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41_data_start_index; data_index < test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41_non_strict 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_1_overflow_41 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42;"
    sql "create table test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42 values (0, "9999999999999999999.9999999999999999999"),(1, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42_data_start_index = 0
    def test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42_data_end_index = 2
    for (int data_index = test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42_data_start_index; data_index < test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42_non_strict 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal_37_18_from_decimal_38_19_overflow_42 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45 values (0, "10"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_0_overflow_45 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46 values (0, "10.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_1_overflow_46 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47 values (0, "10.999999999"),(1, "9999999998.999999999"),(2, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_19_9_overflow_47 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50 values (0, "10"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_0_overflow_50 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51 values (0, "10.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_1_overflow_51 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52 values (0, "10.999999999999999999"),(1, "9999999999999999998.999999999999999999"),(2, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_37_18_overflow_52 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55 values (0, "10"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_0_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56 values (0, "10.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_1_overflow_56 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57 values (0, "10.9999999999999999999"),(1, "9999999999999999998.9999999999999999999"),(2, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_19_overflow_57 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58 values (0, "9.9999999999999999999999999999999999999"),(1, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58_data_end_index = 2
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_38_37_overflow_58 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60 values (0, "1"),(1, "9999999999999999998"),(2, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_0_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61 values (0, "1.9"),(1, "999999999999999998.9"),(2, "999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_1_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62 values (0, "1.999999999"),(1, "9999999998.999999999"),(2, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_9_overflow_62 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63 values (0, "1.999999999999999999"),(1, "8.999999999999999999"),(2, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_19_18_overflow_63 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65 values (0, "1"),(1, "9999999999999999999999999999999999998"),(2, "9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_0_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66 values (0, "1.9"),(1, "999999999999999999999999999999999998.9"),(2, "999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_1_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67 values (0, "1.999999999999999999"),(1, "9999999999999999998.999999999999999999"),(2, "9999999999999999999.999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_18_overflow_67 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68 values (0, "1.999999999999999999999999999999999999"),(1, "8.999999999999999999999999999999999999"),(2, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_37_36_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70 values (0, "1"),(1, "99999999999999999999999999999999999998"),(2, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71 values (0, "1.9"),(1, "9999999999999999999999999999999999998.9"),(2, "9999999999999999999999999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72 values (0, "1.9999999999999999999"),(1, "9999999999999999998.9999999999999999999"),(2, "9999999999999999999.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_19_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73 values (0, "1.9999999999999999999999999999999999999"),(1, "8.9999999999999999999999999999999999999"),(2, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_37_overflow_73 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74 values (0, "0.99999999999999999999999999999999999999"),(1, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74_data_end_index = 2
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_38_38_overflow_74 order by 1;'

}