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


suite("test_cast_to_decimal64_10_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_0 values (0, 10000000000),(1, 99999999999999998),(2, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal64_17_0_data_start_index = 0
    def test_cast_to_decimal64_10_0_from_decimal64_17_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_1 values (3, 9999999999.9),(4, 9999999999.9),(5, 10000000000.9),(6, 10000000000.9),(7, 9999999999999998.9),
      (8, 9999999999999998.9),(9, 9999999999999999.9),(10, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal64_17_1_data_start_index = 3
    def test_cast_to_decimal64_10_0_from_decimal64_17_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_0 values (11, 10000000000),(12, 999999999999999998),(13, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal64_18_0_data_start_index = 11
    def test_cast_to_decimal64_10_0_from_decimal64_18_0_data_end_index = 14
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_1 values (14, 9999999999.9),(15, 9999999999.9),(16, 10000000000.9),(17, 10000000000.9),(18, 99999999999999998.9),
      (19, 99999999999999998.9),(20, 99999999999999999.9),(21, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal64_18_1_data_start_index = 14
    def test_cast_to_decimal64_10_0_from_decimal64_18_1_data_end_index = 22
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_10_0 values (22, 1000000000),(23, 9999999998),(24, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_10_0_data_start_index = 22
    def test_cast_to_decimal64_10_1_from_decimal64_10_0_data_end_index = 25
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_17_0 values (25, 1000000000),(26, 99999999999999998),(27, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_17_0_data_start_index = 25
    def test_cast_to_decimal64_10_1_from_decimal64_17_0_data_end_index = 28
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_17_1 values (28, 1000000000.9),(29, 9999999999999998.9),(30, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_17_1_data_start_index = 28
    def test_cast_to_decimal64_10_1_from_decimal64_17_1_data_end_index = 31
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_17_8 values (31, 999999999.99999999),(32, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_17_8_data_start_index = 31
    def test_cast_to_decimal64_10_1_from_decimal64_17_8_data_end_index = 33
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_18_0 values (33, 1000000000),(34, 999999999999999998),(35, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_18_0_data_start_index = 33
    def test_cast_to_decimal64_10_1_from_decimal64_18_0_data_end_index = 36
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_18_1 values (36, 1000000000.9),(37, 99999999999999998.9),(38, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_18_1_data_start_index = 36
    def test_cast_to_decimal64_10_1_from_decimal64_18_1_data_end_index = 39
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal64_18_9 values (39, 999999999.999999999),(40, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal64_18_9_data_start_index = 39
    def test_cast_to_decimal64_10_1_from_decimal64_18_9_data_end_index = 41
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_9_0 values (41, 100000),(42, 999999998),(43, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_9_0_data_start_index = 41
    def test_cast_to_decimal64_10_5_from_decimal64_9_0_data_end_index = 44
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_9_1 values (44, 100000.9),(45, 99999998.9),(46, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_9_1_data_start_index = 44
    def test_cast_to_decimal64_10_5_from_decimal64_9_1_data_end_index = 47
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_10_0 values (47, 100000),(48, 9999999998),(49, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_10_0_data_start_index = 47
    def test_cast_to_decimal64_10_5_from_decimal64_10_0_data_end_index = 50
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_10_1 values (50, 100000.9),(51, 999999998.9),(52, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_10_1_data_start_index = 50
    def test_cast_to_decimal64_10_5_from_decimal64_10_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_17_0 values (53, 100000),(54, 99999999999999998),(55, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_17_0_data_start_index = 53
    def test_cast_to_decimal64_10_5_from_decimal64_17_0_data_end_index = 56
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_17_1 values (56, 100000.9),(57, 9999999999999998.9),(58, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_17_1_data_start_index = 56
    def test_cast_to_decimal64_10_5_from_decimal64_17_1_data_end_index = 59
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_17_8 values (59, 99999.99999999),(60, 99999.99999999),(61, 100000.99999999),(62, 100000.99999999),(63, 999999998.99999999),
      (64, 999999998.99999999),(65, 999999999.99999999),(66, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_17_8_data_start_index = 59
    def test_cast_to_decimal64_10_5_from_decimal64_17_8_data_end_index = 67
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_18_0 values (67, 100000),(68, 999999999999999998),(69, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_18_0_data_start_index = 67
    def test_cast_to_decimal64_10_5_from_decimal64_18_0_data_end_index = 70
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_18_1 values (70, 100000.9),(71, 99999999999999998.9),(72, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_18_1_data_start_index = 70
    def test_cast_to_decimal64_10_5_from_decimal64_18_1_data_end_index = 73
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal64_18_9 values (73, 99999.999999999),(74, 99999.999999999),(75, 100000.999999999),(76, 100000.999999999),(77, 999999998.999999999),
      (78, 999999998.999999999),(79, 999999999.999999999),(80, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal64_18_9_data_start_index = 73
    def test_cast_to_decimal64_10_5_from_decimal64_18_9_data_end_index = 81
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_9_0 values (81, 10),(82, 999999998),(83, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_9_0_data_start_index = 81
    def test_cast_to_decimal64_10_9_from_decimal64_9_0_data_end_index = 84
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_9_1 values (84, 10.9),(85, 99999998.9),(86, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_9_1_data_start_index = 84
    def test_cast_to_decimal64_10_9_from_decimal64_9_1_data_end_index = 87
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_9_4 values (87, 10.9999),(88, 99998.9999),(89, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_9_4_data_start_index = 87
    def test_cast_to_decimal64_10_9_from_decimal64_9_4_data_end_index = 90
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_10_0 values (90, 10),(91, 9999999998),(92, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_10_0_data_start_index = 90
    def test_cast_to_decimal64_10_9_from_decimal64_10_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_10_1 values (93, 10.9),(94, 999999998.9),(95, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_10_1_data_start_index = 93
    def test_cast_to_decimal64_10_9_from_decimal64_10_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_10_5 values (96, 10.99999),(97, 99998.99999),(98, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_10_5_data_start_index = 96
    def test_cast_to_decimal64_10_9_from_decimal64_10_5_data_end_index = 99
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_17_0 values (99, 10),(100, 99999999999999998),(101, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_17_0_data_start_index = 99
    def test_cast_to_decimal64_10_9_from_decimal64_17_0_data_end_index = 102
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_17_1 values (102, 10.9),(103, 9999999999999998.9),(104, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_17_1_data_start_index = 102
    def test_cast_to_decimal64_10_9_from_decimal64_17_1_data_end_index = 105
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_17_8 values (105, 10.99999999),(106, 999999998.99999999),(107, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_17_8_data_start_index = 105
    def test_cast_to_decimal64_10_9_from_decimal64_17_8_data_end_index = 108
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_17_16 values (108, 9.9999999999999999),(109, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_17_16_data_start_index = 108
    def test_cast_to_decimal64_10_9_from_decimal64_17_16_data_end_index = 110
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_18_0 values (110, 10),(111, 999999999999999998),(112, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_18_0_data_start_index = 110
    def test_cast_to_decimal64_10_9_from_decimal64_18_0_data_end_index = 113
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_18_1 values (113, 10.9),(114, 99999999999999998.9),(115, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_18_1_data_start_index = 113
    def test_cast_to_decimal64_10_9_from_decimal64_18_1_data_end_index = 116
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_18_9 values (116, 10.999999999),(117, 999999998.999999999),(118, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_18_9_data_start_index = 116
    def test_cast_to_decimal64_10_9_from_decimal64_18_9_data_end_index = 119
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal64_18_17 values (119, 9.99999999999999999),(120, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal64_18_17_data_start_index = 119
    def test_cast_to_decimal64_10_9_from_decimal64_18_17_data_end_index = 121
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_9_0 values (121, 1),(122, 999999998),(123, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_9_0_data_start_index = 121
    def test_cast_to_decimal64_10_10_from_decimal64_9_0_data_end_index = 124
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_9_1 values (124, 1.9),(125, 99999998.9),(126, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_9_1_data_start_index = 124
    def test_cast_to_decimal64_10_10_from_decimal64_9_1_data_end_index = 127
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_9_4 values (127, 1.9999),(128, 99998.9999),(129, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_9_4_data_start_index = 127
    def test_cast_to_decimal64_10_10_from_decimal64_9_4_data_end_index = 130
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_9_8 values (130, 1.99999999),(131, 8.99999999),(132, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_9_8_data_start_index = 130
    def test_cast_to_decimal64_10_10_from_decimal64_9_8_data_end_index = 133
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_10_0 values (133, 1),(134, 9999999998),(135, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_10_0_data_start_index = 133
    def test_cast_to_decimal64_10_10_from_decimal64_10_0_data_end_index = 136
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_10_1 values (136, 1.9),(137, 999999998.9),(138, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_10_1_data_start_index = 136
    def test_cast_to_decimal64_10_10_from_decimal64_10_1_data_end_index = 139
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_10_5 values (139, 1.99999),(140, 99998.99999),(141, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_10_5_data_start_index = 139
    def test_cast_to_decimal64_10_10_from_decimal64_10_5_data_end_index = 142
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_10_9 values (142, 1.999999999),(143, 8.999999999),(144, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_10_9_data_start_index = 142
    def test_cast_to_decimal64_10_10_from_decimal64_10_9_data_end_index = 145
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_17_0 values (145, 1),(146, 99999999999999998),(147, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_17_0_data_start_index = 145
    def test_cast_to_decimal64_10_10_from_decimal64_17_0_data_end_index = 148
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_17_1 values (148, 1.9),(149, 9999999999999998.9),(150, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_17_1_data_start_index = 148
    def test_cast_to_decimal64_10_10_from_decimal64_17_1_data_end_index = 151
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_17_8 values (151, 1.99999999),(152, 999999998.99999999),(153, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_17_8_data_start_index = 151
    def test_cast_to_decimal64_10_10_from_decimal64_17_8_data_end_index = 154
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_17_16 values (154, 0.9999999999999999),(155, 0.9999999999999999),(156, 1.9999999999999999),(157, 1.9999999999999999),(158, 8.9999999999999999),
      (159, 8.9999999999999999),(160, 9.9999999999999999),(161, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_17_16_data_start_index = 154
    def test_cast_to_decimal64_10_10_from_decimal64_17_16_data_end_index = 162
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_17_17 values (162, 0.99999999999999999),(163, 0.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_17_17_data_start_index = 162
    def test_cast_to_decimal64_10_10_from_decimal64_17_17_data_end_index = 164
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_17_17_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_17_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_18_0 values (164, 1),(165, 999999999999999998),(166, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_18_0_data_start_index = 164
    def test_cast_to_decimal64_10_10_from_decimal64_18_0_data_end_index = 167
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_18_1 values (167, 1.9),(168, 99999999999999998.9),(169, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_18_1_data_start_index = 167
    def test_cast_to_decimal64_10_10_from_decimal64_18_1_data_end_index = 170
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_18_9 values (170, 1.999999999),(171, 999999998.999999999),(172, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_18_9_data_start_index = 170
    def test_cast_to_decimal64_10_10_from_decimal64_18_9_data_end_index = 173
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_18_17 values (173, 0.99999999999999999),(174, 0.99999999999999999),(175, 1.99999999999999999),(176, 1.99999999999999999),(177, 8.99999999999999999),
      (178, 8.99999999999999999),(179, 9.99999999999999999),(180, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_18_17_data_start_index = 173
    def test_cast_to_decimal64_10_10_from_decimal64_18_17_data_end_index = 181
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal64_18_18 values (181, 0.999999999999999999),(182, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal64_18_18_data_start_index = 181
    def test_cast_to_decimal64_10_10_from_decimal64_18_18_data_end_index = 183
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal64_18_18 order by 1;'

}