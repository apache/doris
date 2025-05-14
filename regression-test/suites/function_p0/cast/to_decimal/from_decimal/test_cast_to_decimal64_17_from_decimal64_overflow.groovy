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


suite("test_cast_to_decimal64_17_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal64_18_0 values (0, 100000000000000000),(1, 999999999999999998),(2, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal64_18_0_data_start_index = 0
    def test_cast_to_decimal64_17_0_from_decimal64_18_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal64_18_1 values (3, 99999999999999999.9),(4, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal64_18_1_data_start_index = 3
    def test_cast_to_decimal64_17_0_from_decimal64_18_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal64_17_0 values (5, 10000000000000000),(6, 99999999999999998),(7, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal64_17_0_data_start_index = 5
    def test_cast_to_decimal64_17_1_from_decimal64_17_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal64_18_0 values (8, 10000000000000000),(9, 999999999999999998),(10, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal64_18_0_data_start_index = 8
    def test_cast_to_decimal64_17_1_from_decimal64_18_0_data_end_index = 11
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal64_18_1 values (11, 10000000000000000.9),(12, 99999999999999998.9),(13, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal64_18_1_data_start_index = 11
    def test_cast_to_decimal64_17_1_from_decimal64_18_1_data_end_index = 14
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_10_0 values (14, 1000000000),(15, 9999999998),(16, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_10_0_data_start_index = 14
    def test_cast_to_decimal64_17_8_from_decimal64_10_0_data_end_index = 17
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_17_0 values (17, 1000000000),(18, 99999999999999998),(19, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_17_0_data_start_index = 17
    def test_cast_to_decimal64_17_8_from_decimal64_17_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_17_1 values (20, 1000000000.9),(21, 9999999999999998.9),(22, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_17_1_data_start_index = 20
    def test_cast_to_decimal64_17_8_from_decimal64_17_1_data_end_index = 23
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_18_0 values (23, 1000000000),(24, 999999999999999998),(25, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_18_0_data_start_index = 23
    def test_cast_to_decimal64_17_8_from_decimal64_18_0_data_end_index = 26
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_18_1 values (26, 1000000000.9),(27, 99999999999999998.9),(28, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_18_1_data_start_index = 26
    def test_cast_to_decimal64_17_8_from_decimal64_18_1_data_end_index = 29
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal64_18_9 values (29, 999999999.999999999),(30, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal64_18_9_data_start_index = 29
    def test_cast_to_decimal64_17_8_from_decimal64_18_9_data_end_index = 31
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_9_0 values (31, 10),(32, 999999998),(33, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_9_0_data_start_index = 31
    def test_cast_to_decimal64_17_16_from_decimal64_9_0_data_end_index = 34
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_9_1 values (34, 10.9),(35, 99999998.9),(36, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_9_1_data_start_index = 34
    def test_cast_to_decimal64_17_16_from_decimal64_9_1_data_end_index = 37
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_9_4 values (37, 10.9999),(38, 99998.9999),(39, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_9_4_data_start_index = 37
    def test_cast_to_decimal64_17_16_from_decimal64_9_4_data_end_index = 40
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_10_0 values (40, 10),(41, 9999999998),(42, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_10_0_data_start_index = 40
    def test_cast_to_decimal64_17_16_from_decimal64_10_0_data_end_index = 43
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_10_1 values (43, 10.9),(44, 999999998.9),(45, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_10_1_data_start_index = 43
    def test_cast_to_decimal64_17_16_from_decimal64_10_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_10_5 values (46, 10.99999),(47, 99998.99999),(48, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_10_5_data_start_index = 46
    def test_cast_to_decimal64_17_16_from_decimal64_10_5_data_end_index = 49
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_17_0 values (49, 10),(50, 99999999999999998),(51, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_17_0_data_start_index = 49
    def test_cast_to_decimal64_17_16_from_decimal64_17_0_data_end_index = 52
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_17_1 values (52, 10.9),(53, 9999999999999998.9),(54, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_17_1_data_start_index = 52
    def test_cast_to_decimal64_17_16_from_decimal64_17_1_data_end_index = 55
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_17_8 values (55, 10.99999999),(56, 999999998.99999999),(57, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_17_8_data_start_index = 55
    def test_cast_to_decimal64_17_16_from_decimal64_17_8_data_end_index = 58
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_18_0 values (58, 10),(59, 999999999999999998),(60, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_18_0_data_start_index = 58
    def test_cast_to_decimal64_17_16_from_decimal64_18_0_data_end_index = 61
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_18_1 values (61, 10.9),(62, 99999999999999998.9),(63, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_18_1_data_start_index = 61
    def test_cast_to_decimal64_17_16_from_decimal64_18_1_data_end_index = 64
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_18_9 values (64, 10.999999999),(65, 999999998.999999999),(66, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_18_9_data_start_index = 64
    def test_cast_to_decimal64_17_16_from_decimal64_18_9_data_end_index = 67
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal64_18_17 values (67, 9.99999999999999999),(68, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal64_18_17_data_start_index = 67
    def test_cast_to_decimal64_17_16_from_decimal64_18_17_data_end_index = 69
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_9_0 values (69, 1),(70, 999999998),(71, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_9_0_data_start_index = 69
    def test_cast_to_decimal64_17_17_from_decimal64_9_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_9_1 values (72, 1.9),(73, 99999998.9),(74, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_9_1_data_start_index = 72
    def test_cast_to_decimal64_17_17_from_decimal64_9_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_9_4 values (75, 1.9999),(76, 99998.9999),(77, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_9_4_data_start_index = 75
    def test_cast_to_decimal64_17_17_from_decimal64_9_4_data_end_index = 78
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_9_8 values (78, 1.99999999),(79, 8.99999999),(80, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_9_8_data_start_index = 78
    def test_cast_to_decimal64_17_17_from_decimal64_9_8_data_end_index = 81
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_10_0 values (81, 1),(82, 9999999998),(83, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_10_0_data_start_index = 81
    def test_cast_to_decimal64_17_17_from_decimal64_10_0_data_end_index = 84
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_10_1 values (84, 1.9),(85, 999999998.9),(86, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_10_1_data_start_index = 84
    def test_cast_to_decimal64_17_17_from_decimal64_10_1_data_end_index = 87
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_10_5 values (87, 1.99999),(88, 99998.99999),(89, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_10_5_data_start_index = 87
    def test_cast_to_decimal64_17_17_from_decimal64_10_5_data_end_index = 90
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_10_9 values (90, 1.999999999),(91, 8.999999999),(92, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_10_9_data_start_index = 90
    def test_cast_to_decimal64_17_17_from_decimal64_10_9_data_end_index = 93
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_17_0 values (93, 1),(94, 99999999999999998),(95, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_17_0_data_start_index = 93
    def test_cast_to_decimal64_17_17_from_decimal64_17_0_data_end_index = 96
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_17_1 values (96, 1.9),(97, 9999999999999998.9),(98, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_17_1_data_start_index = 96
    def test_cast_to_decimal64_17_17_from_decimal64_17_1_data_end_index = 99
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_17_8 values (99, 1.99999999),(100, 999999998.99999999),(101, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_17_8_data_start_index = 99
    def test_cast_to_decimal64_17_17_from_decimal64_17_8_data_end_index = 102
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_17_16 values (102, 1.9999999999999999),(103, 8.9999999999999999),(104, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_17_16_data_start_index = 102
    def test_cast_to_decimal64_17_17_from_decimal64_17_16_data_end_index = 105
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_18_0 values (105, 1),(106, 999999999999999998),(107, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_18_0_data_start_index = 105
    def test_cast_to_decimal64_17_17_from_decimal64_18_0_data_end_index = 108
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_18_1 values (108, 1.9),(109, 99999999999999998.9),(110, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_18_1_data_start_index = 108
    def test_cast_to_decimal64_17_17_from_decimal64_18_1_data_end_index = 111
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_18_9 values (111, 1.999999999),(112, 999999998.999999999),(113, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_18_9_data_start_index = 111
    def test_cast_to_decimal64_17_17_from_decimal64_18_9_data_end_index = 114
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_18_17 values (114, 1.99999999999999999),(115, 8.99999999999999999),(116, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_18_17_data_start_index = 114
    def test_cast_to_decimal64_17_17_from_decimal64_18_17_data_end_index = 117
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal64_18_18 values (117, 0.999999999999999999),(118, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal64_18_18_data_start_index = 117
    def test_cast_to_decimal64_17_17_from_decimal64_18_18_data_end_index = 119
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal64_18_18 order by 1;'

}