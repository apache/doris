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


suite("test_cast_to_decimal32_8_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal32_9_0 values (0, 100000000),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal32_9_0_data_start_index = 0
    def test_cast_to_decimal32_8_0_from_decimal32_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal32_9_1 values (3, 99999999.9),(4, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal32_9_1_data_start_index = 3
    def test_cast_to_decimal32_8_0_from_decimal32_9_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal32_8_0 values (5, 10000000),(6, 99999998),(7, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal32_8_0_data_start_index = 5
    def test_cast_to_decimal32_8_1_from_decimal32_8_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal32_9_0 values (8, 10000000),(9, 999999998),(10, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal32_9_0_data_start_index = 8
    def test_cast_to_decimal32_8_1_from_decimal32_9_0_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal32_9_1 values (11, 10000000.9),(12, 99999998.9),(13, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal32_9_1_data_start_index = 11
    def test_cast_to_decimal32_8_1_from_decimal32_9_1_data_end_index = 14
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal32_8_0 values (14, 10000),(15, 99999998),(16, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal32_8_0_data_start_index = 14
    def test_cast_to_decimal32_8_4_from_decimal32_8_0_data_end_index = 17
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal32_8_1 values (17, 10000.9),(18, 9999998.9),(19, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal32_8_1_data_start_index = 17
    def test_cast_to_decimal32_8_4_from_decimal32_8_1_data_end_index = 20
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal32_9_0 values (20, 10000),(21, 999999998),(22, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal32_9_0_data_start_index = 20
    def test_cast_to_decimal32_8_4_from_decimal32_9_0_data_end_index = 23
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal32_9_1 values (23, 10000.9),(24, 99999998.9),(25, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal32_9_1_data_start_index = 23
    def test_cast_to_decimal32_8_4_from_decimal32_9_1_data_end_index = 26
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal32_9_4 values (26, 10000.9999),(27, 99998.9999),(28, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal32_9_4_data_start_index = 26
    def test_cast_to_decimal32_8_4_from_decimal32_9_4_data_end_index = 29
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_4_0 values (29, 10),(30, 9998),(31, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_4_0_data_start_index = 29
    def test_cast_to_decimal32_8_7_from_decimal32_4_0_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_4_1 values (32, 10.9),(33, 998.9),(34, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_4_1_data_start_index = 32
    def test_cast_to_decimal32_8_7_from_decimal32_4_1_data_end_index = 35
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_4_2 values (35, 10.99),(36, 98.99),(37, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_4_2_data_start_index = 35
    def test_cast_to_decimal32_8_7_from_decimal32_4_2_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_8_0 values (38, 10),(39, 99999998),(40, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_8_0_data_start_index = 38
    def test_cast_to_decimal32_8_7_from_decimal32_8_0_data_end_index = 41
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_8_1 values (41, 10.9),(42, 9999998.9),(43, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_8_1_data_start_index = 41
    def test_cast_to_decimal32_8_7_from_decimal32_8_1_data_end_index = 44
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_8_4 values (44, 10.9999),(45, 9998.9999),(46, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_8_4_data_start_index = 44
    def test_cast_to_decimal32_8_7_from_decimal32_8_4_data_end_index = 47
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_9_0 values (47, 10),(48, 999999998),(49, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_9_0_data_start_index = 47
    def test_cast_to_decimal32_8_7_from_decimal32_9_0_data_end_index = 50
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_9_1 values (50, 10.9),(51, 99999998.9),(52, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_9_1_data_start_index = 50
    def test_cast_to_decimal32_8_7_from_decimal32_9_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_9_4 values (53, 10.9999),(54, 99998.9999),(55, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_9_4_data_start_index = 53
    def test_cast_to_decimal32_8_7_from_decimal32_9_4_data_end_index = 56
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal32_9_8 values (56, 9.99999999),(57, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal32_9_8_data_start_index = 56
    def test_cast_to_decimal32_8_7_from_decimal32_9_8_data_end_index = 58
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_1_0 values (58, 1),(59, 8),(60, 9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_1_0_data_start_index = 58
    def test_cast_to_decimal32_8_8_from_decimal32_1_0_data_end_index = 61
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_1_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_4_0 values (61, 1),(62, 9998),(63, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_4_0_data_start_index = 61
    def test_cast_to_decimal32_8_8_from_decimal32_4_0_data_end_index = 64
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_4_1 values (64, 1.9),(65, 998.9),(66, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_4_1_data_start_index = 64
    def test_cast_to_decimal32_8_8_from_decimal32_4_1_data_end_index = 67
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_4_2 values (67, 1.99),(68, 98.99),(69, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_4_2_data_start_index = 67
    def test_cast_to_decimal32_8_8_from_decimal32_4_2_data_end_index = 70
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_4_3 values (70, 1.999),(71, 8.999),(72, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_4_3_data_start_index = 70
    def test_cast_to_decimal32_8_8_from_decimal32_4_3_data_end_index = 73
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_8_0 values (73, 1),(74, 99999998),(75, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_8_0_data_start_index = 73
    def test_cast_to_decimal32_8_8_from_decimal32_8_0_data_end_index = 76
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_8_1 values (76, 1.9),(77, 9999998.9),(78, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_8_1_data_start_index = 76
    def test_cast_to_decimal32_8_8_from_decimal32_8_1_data_end_index = 79
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_8_4 values (79, 1.9999),(80, 9998.9999),(81, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_8_4_data_start_index = 79
    def test_cast_to_decimal32_8_8_from_decimal32_8_4_data_end_index = 82
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_8_7 values (82, 1.9999999),(83, 8.9999999),(84, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_8_7_data_start_index = 82
    def test_cast_to_decimal32_8_8_from_decimal32_8_7_data_end_index = 85
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_9_0 values (85, 1),(86, 999999998),(87, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_9_0_data_start_index = 85
    def test_cast_to_decimal32_8_8_from_decimal32_9_0_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_9_1 values (88, 1.9),(89, 99999998.9),(90, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_9_1_data_start_index = 88
    def test_cast_to_decimal32_8_8_from_decimal32_9_1_data_end_index = 91
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_9_4 values (91, 1.9999),(92, 99998.9999),(93, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_9_4_data_start_index = 91
    def test_cast_to_decimal32_8_8_from_decimal32_9_4_data_end_index = 94
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_9_8 values (94, 1.99999999),(95, 8.99999999),(96, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_9_8_data_start_index = 94
    def test_cast_to_decimal32_8_8_from_decimal32_9_8_data_end_index = 97
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal32_9_9 values (97, 0.999999999),(98, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal32_9_9_data_start_index = 97
    def test_cast_to_decimal32_8_8_from_decimal32_9_9_data_end_index = 99
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal32_9_9_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal32_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal32_9_9 order by 1;'

}