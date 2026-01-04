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


suite("test_cast_to_decimal128i_37_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45 values (0, "10"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_0_overflow_45 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46 values (0, "10.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_1_overflow_46 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47 values (0, "10.99999"),(1, "99998.99999"),(2, "99999.99999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_10_5_overflow_47 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50 values (0, "10"),(1, "99999999999999998"),(2, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_0_overflow_50 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51 values (0, "10.9"),(1, "9999999999999998.9"),(2, "9999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_1_overflow_51 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52 values (0, "10.99999999"),(1, "999999998.99999999"),(2, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_17_8_overflow_52 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55 values (0, "10"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_0_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56 values (0, "10.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_1_overflow_56 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57;"
    sql "create table test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57 values (0, "10.999999999"),(1, "999999998.999999999"),(2, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57_data_start_index = 0
    def test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57_data_start_index; data_index < test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57_non_strict 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal_37_36_from_decimal_18_9_overflow_57 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60 values (0, "1"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_0_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61 values (0, "1.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_1_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62 values (0, "1.99999"),(1, "99998.99999"),(2, "99999.99999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_5_overflow_62 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63 values (0, "1.999999999"),(1, "8.999999999"),(2, "9.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_10_9_overflow_63 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65 values (0, "1"),(1, "99999999999999998"),(2, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_0_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66 values (0, "1.9"),(1, "9999999999999998.9"),(2, "9999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_1_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67 values (0, "1.99999999"),(1, "999999998.99999999"),(2, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_8_overflow_67 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68 values (0, "1.9999999999999999"),(1, "8.9999999999999999"),(2, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_17_16_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70 values (0, "1"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71 values (0, "1.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72 values (0, "1.999999999"),(1, "999999998.999999999"),(2, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_9_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73 values (0, "1.99999999999999999"),(1, "8.99999999999999999"),(2, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73_data_start_index = 0
    def test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73_data_end_index = 3
    for (int data_index = test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73_data_start_index; data_index < test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_18_17_overflow_73 order by 1;'

}