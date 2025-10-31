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


suite("test_cast_to_decimal64_17_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53 values (0, "10"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_0_overflow_53 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54 values (0, "10.9"),(1, "998.9"),(2, "999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_1_overflow_54 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55 values (0, "10.99"),(1, "98.99"),(2, "99.99");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_4_2_overflow_55 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58 values (0, "10"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_0_overflow_58 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59 values (0, "10.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_1_overflow_59 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60 values (0, "10.9999"),(1, "9998.9999"),(2, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_8_4_overflow_60 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63 values (0, "10"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_0_overflow_63 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64 values (0, "10.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_1_overflow_64 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65;"
    sql "create table test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65 values (0, "10.9999"),(1, "99998.9999"),(2, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65_data_start_index = 0
    def test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65_data_start_index; data_index < test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65_non_strict 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal_17_16_from_decimal_9_4_overflow_65 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68 values (0, "1"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_1_0_overflow_68 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70 values (0, "1"),(1, "9998"),(2, "9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_0_overflow_70 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71 values (0, "1.9"),(1, "998.9"),(2, "999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_1_overflow_71 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72 values (0, "1.99"),(1, "98.99"),(2, "99.99");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_2_overflow_72 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73 values (0, "1.999"),(1, "8.999"),(2, "9.999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_4_3_overflow_73 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75 values (0, "1"),(1, "99999998"),(2, "99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_0_overflow_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76 values (0, "1.9"),(1, "9999998.9"),(2, "9999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_1_overflow_76 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77 values (0, "1.9999"),(1, "9998.9999"),(2, "9999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_4_overflow_77 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78 values (0, "1.9999999"),(1, "8.9999999"),(2, "9.9999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_8_7_overflow_78 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80 values (0, "1"),(1, "999999998"),(2, "999999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_0_overflow_80 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81 values (0, "1.9"),(1, "99999998.9"),(2, "99999999.9");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_1_overflow_81 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82 values (0, "1.9999"),(1, "99998.9999"),(2, "99999.9999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_4_overflow_82 order by 1;'

    sql "drop table if exists test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83;"
    sql "create table test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83 values (0, "1.99999999"),(1, "8.99999999"),(2, "9.99999999");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83_data_start_index = 0
    def test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83_data_end_index = 3
    for (int data_index = test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83_data_start_index; data_index < test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83_non_strict 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal_17_17_from_decimal_9_8_overflow_83 order by 1;'

}