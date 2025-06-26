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


suite("test_cast_to_decimal256_39_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60 values (0, "10"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_0_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61 values (0, "10.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_1_overflow_61 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62 values (0, "10.9999"),(1, "99998.9999"),(2, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_9_4_overflow_62 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65 values (0, "10"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_0_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66 values (0, "10.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_1_overflow_66 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67 values (0, "10.99999"),(1, "99998.99999"),(2, "99999.99999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_10_5_overflow_67 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70 values (0, "10"),(1, "99999999999999998"),(2, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71 values (0, "10.9"),(1, "9999999999999998.9"),(2, "9999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72 values (0, "10.99999999"),(1, "999999998.99999999"),(2, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_17_8_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75 values (0, "10"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_0_overflow_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76 values (0, "10.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_1_overflow_76 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77;"
    sql "create table test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77 values (0, "10.999999999"),(1, "999999998.999999999"),(2, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77_data_start_index = 0
    def test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77_data_start_index; data_index < test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77_non_strict 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal_39_38_from_decimal_18_9_overflow_77 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80 values (0, "1"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_0_overflow_80 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81 values (0, "1.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_1_overflow_81 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82 values (0, "1.9999"),(1, "99998.9999"),(2, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_4_overflow_82 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83 values (0, "1.99999999"),(1, "8.99999999"),(2, "9.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_9_8_overflow_83 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85 values (0, "1"),(1, "9999999998"),(2, "9999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_0_overflow_85 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86 values (0, "1.9"),(1, "999999998.9"),(2, "999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_1_overflow_86 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87 values (0, "1.99999"),(1, "99998.99999"),(2, "99999.99999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_5_overflow_87 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88 values (0, "1.999999999"),(1, "8.999999999"),(2, "9.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_10_9_overflow_88 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90 values (0, "1"),(1, "99999999999999998"),(2, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_0_overflow_90 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91 values (0, "1.9"),(1, "9999999999999998.9"),(2, "9999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_1_overflow_91 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92 values (0, "1.99999999"),(1, "999999998.99999999"),(2, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_8_overflow_92 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93 values (0, "1.9999999999999999"),(1, "8.9999999999999999"),(2, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_17_16_overflow_93 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95 values (0, "1"),(1, "999999999999999998"),(2, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_0_overflow_95 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96 values (0, "1.9"),(1, "99999999999999998.9"),(2, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_1_overflow_96 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97 values (0, "1.999999999"),(1, "999999998.999999999"),(2, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_9_overflow_97 order by 1;'

    sql "drop table if exists test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98;"
    sql "create table test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98 values (0, "1.99999999999999999"),(1, "8.99999999999999999"),(2, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98_data_start_index = 0
    def test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98_data_end_index = 3
    for (int data_index = test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98_data_start_index; data_index < test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal_39_39_from_decimal_18_17_overflow_98 order by 1;'

}