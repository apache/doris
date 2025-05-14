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


suite("test_cast_to_decimal32_9_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_0 values (0, 100000000),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal32_9_0_data_start_index = 0
    def test_cast_to_decimal32_9_1_from_decimal32_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal32_8_0 values (3, 100000),(4, 99999998),(5, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal32_8_0_data_start_index = 3
    def test_cast_to_decimal32_9_4_from_decimal32_8_0_data_end_index = 6
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal32_8_1 values (6, 100000.9),(7, 9999998.9),(8, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal32_8_1_data_start_index = 6
    def test_cast_to_decimal32_9_4_from_decimal32_8_1_data_end_index = 9
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal32_9_0 values (9, 100000),(10, 999999998),(11, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal32_9_0_data_start_index = 9
    def test_cast_to_decimal32_9_4_from_decimal32_9_0_data_end_index = 12
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal32_9_1 values (12, 100000.9),(13, 99999998.9),(14, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal32_9_1_data_start_index = 12
    def test_cast_to_decimal32_9_4_from_decimal32_9_1_data_end_index = 15
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_4_0 values (15, 10),(16, 9998),(17, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_4_0_data_start_index = 15
    def test_cast_to_decimal32_9_8_from_decimal32_4_0_data_end_index = 18
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_4_1 values (18, 10.9),(19, 998.9),(20, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_4_1_data_start_index = 18
    def test_cast_to_decimal32_9_8_from_decimal32_4_1_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_4_2 values (21, 10.99),(22, 98.99),(23, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_4_2_data_start_index = 21
    def test_cast_to_decimal32_9_8_from_decimal32_4_2_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_8_0 values (24, 10),(25, 99999998),(26, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_8_0_data_start_index = 24
    def test_cast_to_decimal32_9_8_from_decimal32_8_0_data_end_index = 27
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_8_1 values (27, 10.9),(28, 9999998.9),(29, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_8_1_data_start_index = 27
    def test_cast_to_decimal32_9_8_from_decimal32_8_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_8_4 values (30, 10.9999),(31, 9998.9999),(32, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_8_4_data_start_index = 30
    def test_cast_to_decimal32_9_8_from_decimal32_8_4_data_end_index = 33
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_9_0 values (33, 10),(34, 999999998),(35, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_9_0_data_start_index = 33
    def test_cast_to_decimal32_9_8_from_decimal32_9_0_data_end_index = 36
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_9_1 values (36, 10.9),(37, 99999998.9),(38, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_9_1_data_start_index = 36
    def test_cast_to_decimal32_9_8_from_decimal32_9_1_data_end_index = 39
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal32_9_4 values (39, 10.9999),(40, 99998.9999),(41, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal32_9_4_data_start_index = 39
    def test_cast_to_decimal32_9_8_from_decimal32_9_4_data_end_index = 42
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_1_0 values (42, 1),(43, 8),(44, 9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_1_0_data_start_index = 42
    def test_cast_to_decimal32_9_9_from_decimal32_1_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_1_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_4_0 values (45, 1),(46, 9998),(47, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_4_0_data_start_index = 45
    def test_cast_to_decimal32_9_9_from_decimal32_4_0_data_end_index = 48
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_4_1 values (48, 1.9),(49, 998.9),(50, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_4_1_data_start_index = 48
    def test_cast_to_decimal32_9_9_from_decimal32_4_1_data_end_index = 51
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_4_2 values (51, 1.99),(52, 98.99),(53, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_4_2_data_start_index = 51
    def test_cast_to_decimal32_9_9_from_decimal32_4_2_data_end_index = 54
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_4_3 values (54, 1.999),(55, 8.999),(56, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_4_3_data_start_index = 54
    def test_cast_to_decimal32_9_9_from_decimal32_4_3_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_8_0 values (57, 1),(58, 99999998),(59, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_8_0_data_start_index = 57
    def test_cast_to_decimal32_9_9_from_decimal32_8_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_8_1 values (60, 1.9),(61, 9999998.9),(62, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_8_1_data_start_index = 60
    def test_cast_to_decimal32_9_9_from_decimal32_8_1_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_8_4 values (63, 1.9999),(64, 9998.9999),(65, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_8_4_data_start_index = 63
    def test_cast_to_decimal32_9_9_from_decimal32_8_4_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_8_7 values (66, 1.9999999),(67, 8.9999999),(68, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_8_7_data_start_index = 66
    def test_cast_to_decimal32_9_9_from_decimal32_8_7_data_end_index = 69
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_9_0 values (69, 1),(70, 999999998),(71, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_9_0_data_start_index = 69
    def test_cast_to_decimal32_9_9_from_decimal32_9_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_9_1 values (72, 1.9),(73, 99999998.9),(74, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_9_1_data_start_index = 72
    def test_cast_to_decimal32_9_9_from_decimal32_9_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_9_4 values (75, 1.9999),(76, 99998.9999),(77, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_9_4_data_start_index = 75
    def test_cast_to_decimal32_9_9_from_decimal32_9_4_data_end_index = 78
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal32_9_8 values (78, 1.99999999),(79, 8.99999999),(80, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal32_9_8_data_start_index = 78
    def test_cast_to_decimal32_9_9_from_decimal32_9_8_data_end_index = 81
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal32_9_8 order by 1;'

}