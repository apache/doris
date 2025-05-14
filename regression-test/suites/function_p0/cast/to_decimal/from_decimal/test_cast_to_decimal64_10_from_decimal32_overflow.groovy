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


suite("test_cast_to_decimal64_10_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal32_8_0 values (0, 100000),(1, 99999998),(2, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal32_8_0_data_start_index = 0
    def test_cast_to_decimal64_10_5_from_decimal32_8_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal32_8_1 values (3, 100000.9),(4, 9999998.9),(5, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal32_8_1_data_start_index = 3
    def test_cast_to_decimal64_10_5_from_decimal32_8_1_data_end_index = 6
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal32_9_0 values (6, 100000),(7, 999999998),(8, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal32_9_0_data_start_index = 6
    def test_cast_to_decimal64_10_5_from_decimal32_9_0_data_end_index = 9
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal32_9_1 values (9, 100000.9),(10, 99999998.9),(11, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal32_9_1_data_start_index = 9
    def test_cast_to_decimal64_10_5_from_decimal32_9_1_data_end_index = 12
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_4_0 values (12, 10),(13, 9998),(14, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_4_0_data_start_index = 12
    def test_cast_to_decimal64_10_9_from_decimal32_4_0_data_end_index = 15
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_4_1 values (15, 10.9),(16, 998.9),(17, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_4_1_data_start_index = 15
    def test_cast_to_decimal64_10_9_from_decimal32_4_1_data_end_index = 18
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_4_2 values (18, 10.99),(19, 98.99),(20, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_4_2_data_start_index = 18
    def test_cast_to_decimal64_10_9_from_decimal32_4_2_data_end_index = 21
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_8_0 values (21, 10),(22, 99999998),(23, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_8_0_data_start_index = 21
    def test_cast_to_decimal64_10_9_from_decimal32_8_0_data_end_index = 24
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_8_1 values (24, 10.9),(25, 9999998.9),(26, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_8_1_data_start_index = 24
    def test_cast_to_decimal64_10_9_from_decimal32_8_1_data_end_index = 27
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_8_4 values (27, 10.9999),(28, 9998.9999),(29, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_8_4_data_start_index = 27
    def test_cast_to_decimal64_10_9_from_decimal32_8_4_data_end_index = 30
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_9_0 values (30, 10),(31, 999999998),(32, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_9_0_data_start_index = 30
    def test_cast_to_decimal64_10_9_from_decimal32_9_0_data_end_index = 33
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_9_1 values (33, 10.9),(34, 99999998.9),(35, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_9_1_data_start_index = 33
    def test_cast_to_decimal64_10_9_from_decimal32_9_1_data_end_index = 36
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal32_9_4 values (36, 10.9999),(37, 99998.9999),(38, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal32_9_4_data_start_index = 36
    def test_cast_to_decimal64_10_9_from_decimal32_9_4_data_end_index = 39
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_1_0 values (39, 1),(40, 8),(41, 9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_1_0_data_start_index = 39
    def test_cast_to_decimal64_10_10_from_decimal32_1_0_data_end_index = 42
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_1_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_4_0 values (42, 1),(43, 9998),(44, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_4_0_data_start_index = 42
    def test_cast_to_decimal64_10_10_from_decimal32_4_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_4_1 values (45, 1.9),(46, 998.9),(47, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_4_1_data_start_index = 45
    def test_cast_to_decimal64_10_10_from_decimal32_4_1_data_end_index = 48
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_4_2 values (48, 1.99),(49, 98.99),(50, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_4_2_data_start_index = 48
    def test_cast_to_decimal64_10_10_from_decimal32_4_2_data_end_index = 51
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_4_3 values (51, 1.999),(52, 8.999),(53, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_4_3_data_start_index = 51
    def test_cast_to_decimal64_10_10_from_decimal32_4_3_data_end_index = 54
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_8_0 values (54, 1),(55, 99999998),(56, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_8_0_data_start_index = 54
    def test_cast_to_decimal64_10_10_from_decimal32_8_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_8_1 values (57, 1.9),(58, 9999998.9),(59, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_8_1_data_start_index = 57
    def test_cast_to_decimal64_10_10_from_decimal32_8_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_8_4 values (60, 1.9999),(61, 9998.9999),(62, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_8_4_data_start_index = 60
    def test_cast_to_decimal64_10_10_from_decimal32_8_4_data_end_index = 63
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_8_7 values (63, 1.9999999),(64, 8.9999999),(65, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_8_7_data_start_index = 63
    def test_cast_to_decimal64_10_10_from_decimal32_8_7_data_end_index = 66
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_9_0 values (66, 1),(67, 999999998),(68, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_9_0_data_start_index = 66
    def test_cast_to_decimal64_10_10_from_decimal32_9_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_9_1 values (69, 1.9),(70, 99999998.9),(71, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_9_1_data_start_index = 69
    def test_cast_to_decimal64_10_10_from_decimal32_9_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_9_4 values (72, 1.9999),(73, 99998.9999),(74, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_9_4_data_start_index = 72
    def test_cast_to_decimal64_10_10_from_decimal32_9_4_data_end_index = 75
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal32_9_8 values (75, 1.99999999),(76, 8.99999999),(77, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal32_9_8_data_start_index = 75
    def test_cast_to_decimal64_10_10_from_decimal32_9_8_data_end_index = 78
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal32_9_8 order by 1;'

}