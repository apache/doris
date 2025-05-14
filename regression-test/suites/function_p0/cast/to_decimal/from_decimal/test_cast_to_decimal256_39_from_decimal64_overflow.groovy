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
    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_9_0 values (0, 10),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_9_0_data_start_index = 0
    def test_cast_to_decimal256_39_38_from_decimal64_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_9_1 values (3, 10.9),(4, 99999998.9),(5, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_9_1_data_start_index = 3
    def test_cast_to_decimal256_39_38_from_decimal64_9_1_data_end_index = 6
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_9_4 values (6, 10.9999),(7, 99998.9999),(8, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_9_4_data_start_index = 6
    def test_cast_to_decimal256_39_38_from_decimal64_9_4_data_end_index = 9
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_10_0 values (9, 10),(10, 9999999998),(11, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_10_0_data_start_index = 9
    def test_cast_to_decimal256_39_38_from_decimal64_10_0_data_end_index = 12
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_10_1 values (12, 10.9),(13, 999999998.9),(14, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_10_1_data_start_index = 12
    def test_cast_to_decimal256_39_38_from_decimal64_10_1_data_end_index = 15
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_10_5 values (15, 10.99999),(16, 99998.99999),(17, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_10_5_data_start_index = 15
    def test_cast_to_decimal256_39_38_from_decimal64_10_5_data_end_index = 18
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_17_0 values (18, 10),(19, 99999999999999998),(20, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_17_0_data_start_index = 18
    def test_cast_to_decimal256_39_38_from_decimal64_17_0_data_end_index = 21
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_17_1 values (21, 10.9),(22, 9999999999999998.9),(23, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_17_1_data_start_index = 21
    def test_cast_to_decimal256_39_38_from_decimal64_17_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_17_8 values (24, 10.99999999),(25, 999999998.99999999),(26, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_17_8_data_start_index = 24
    def test_cast_to_decimal256_39_38_from_decimal64_17_8_data_end_index = 27
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_18_0 values (27, 10),(28, 999999999999999998),(29, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_18_0_data_start_index = 27
    def test_cast_to_decimal256_39_38_from_decimal64_18_0_data_end_index = 30
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_18_1 values (30, 10.9),(31, 99999999999999998.9),(32, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_18_1_data_start_index = 30
    def test_cast_to_decimal256_39_38_from_decimal64_18_1_data_end_index = 33
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal64_18_9 values (33, 10.999999999),(34, 999999998.999999999),(35, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal64_18_9_data_start_index = 33
    def test_cast_to_decimal256_39_38_from_decimal64_18_9_data_end_index = 36
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_9_0 values (36, 1),(37, 999999998),(38, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_9_0_data_start_index = 36
    def test_cast_to_decimal256_39_39_from_decimal64_9_0_data_end_index = 39
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_9_1 values (39, 1.9),(40, 99999998.9),(41, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_9_1_data_start_index = 39
    def test_cast_to_decimal256_39_39_from_decimal64_9_1_data_end_index = 42
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_9_4 values (42, 1.9999),(43, 99998.9999),(44, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_9_4_data_start_index = 42
    def test_cast_to_decimal256_39_39_from_decimal64_9_4_data_end_index = 45
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_9_8 values (45, 1.99999999),(46, 8.99999999),(47, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_9_8_data_start_index = 45
    def test_cast_to_decimal256_39_39_from_decimal64_9_8_data_end_index = 48
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_10_0 values (48, 1),(49, 9999999998),(50, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_10_0_data_start_index = 48
    def test_cast_to_decimal256_39_39_from_decimal64_10_0_data_end_index = 51
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_10_1 values (51, 1.9),(52, 999999998.9),(53, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_10_1_data_start_index = 51
    def test_cast_to_decimal256_39_39_from_decimal64_10_1_data_end_index = 54
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_10_5 values (54, 1.99999),(55, 99998.99999),(56, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_10_5_data_start_index = 54
    def test_cast_to_decimal256_39_39_from_decimal64_10_5_data_end_index = 57
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_10_9 values (57, 1.999999999),(58, 8.999999999),(59, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_10_9_data_start_index = 57
    def test_cast_to_decimal256_39_39_from_decimal64_10_9_data_end_index = 60
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_17_0 values (60, 1),(61, 99999999999999998),(62, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_17_0_data_start_index = 60
    def test_cast_to_decimal256_39_39_from_decimal64_17_0_data_end_index = 63
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_17_1 values (63, 1.9),(64, 9999999999999998.9),(65, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_17_1_data_start_index = 63
    def test_cast_to_decimal256_39_39_from_decimal64_17_1_data_end_index = 66
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_17_8 values (66, 1.99999999),(67, 999999998.99999999),(68, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_17_8_data_start_index = 66
    def test_cast_to_decimal256_39_39_from_decimal64_17_8_data_end_index = 69
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_17_16 values (69, 1.9999999999999999),(70, 8.9999999999999999),(71, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_17_16_data_start_index = 69
    def test_cast_to_decimal256_39_39_from_decimal64_17_16_data_end_index = 72
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_18_0 values (72, 1),(73, 999999999999999998),(74, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_18_0_data_start_index = 72
    def test_cast_to_decimal256_39_39_from_decimal64_18_0_data_end_index = 75
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_18_1 values (75, 1.9),(76, 99999999999999998.9),(77, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_18_1_data_start_index = 75
    def test_cast_to_decimal256_39_39_from_decimal64_18_1_data_end_index = 78
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_18_9 values (78, 1.999999999),(79, 999999998.999999999),(80, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_18_9_data_start_index = 78
    def test_cast_to_decimal256_39_39_from_decimal64_18_9_data_end_index = 81
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal64_18_17 values (81, 1.99999999999999999),(82, 8.99999999999999999),(83, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal64_18_17_data_start_index = 81
    def test_cast_to_decimal256_39_39_from_decimal64_18_17_data_end_index = 84
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal64_18_17 order by 1;'

}