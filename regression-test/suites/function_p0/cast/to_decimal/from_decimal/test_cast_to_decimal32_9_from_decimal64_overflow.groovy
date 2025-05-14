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


suite("test_cast_to_decimal32_9_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_0 values (0, 1000000000),(1, 9999999998),(2, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_10_0_data_start_index = 0
    def test_cast_to_decimal32_9_0_from_decimal64_10_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_1 values (3, 999999999.9),(4, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_10_1_data_start_index = 3
    def test_cast_to_decimal32_9_0_from_decimal64_10_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_0 values (5, 1000000000),(6, 99999999999999998),(7, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_17_0_data_start_index = 5
    def test_cast_to_decimal32_9_0_from_decimal64_17_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_1 values (8, 999999999.9),(9, 999999999.9),(10, 1000000000.9),(11, 1000000000.9),(12, 9999999999999998.9),
      (13, 9999999999999998.9),(14, 9999999999999999.9),(15, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_17_1_data_start_index = 8
    def test_cast_to_decimal32_9_0_from_decimal64_17_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_8 values (16, 999999999.99999999),(17, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_17_8_data_start_index = 16
    def test_cast_to_decimal32_9_0_from_decimal64_17_8_data_end_index = 18
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_0 values (18, 1000000000),(19, 999999999999999998),(20, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_18_0_data_start_index = 18
    def test_cast_to_decimal32_9_0_from_decimal64_18_0_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_1 values (21, 999999999.9),(22, 999999999.9),(23, 1000000000.9),(24, 1000000000.9),(25, 99999999999999998.9),
      (26, 99999999999999998.9),(27, 99999999999999999.9),(28, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_18_1_data_start_index = 21
    def test_cast_to_decimal32_9_0_from_decimal64_18_1_data_end_index = 29
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_9 values (29, 999999999.999999999),(30, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal64_18_9_data_start_index = 29
    def test_cast_to_decimal32_9_0_from_decimal64_18_9_data_end_index = 31
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_9_0 values (31, 100000000),(32, 999999998),(33, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_9_0_data_start_index = 31
    def test_cast_to_decimal32_9_1_from_decimal64_9_0_data_end_index = 34
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_0 values (34, 100000000),(35, 9999999998),(36, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_10_0_data_start_index = 34
    def test_cast_to_decimal32_9_1_from_decimal64_10_0_data_end_index = 37
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_1 values (37, 100000000.9),(38, 999999998.9),(39, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_10_1_data_start_index = 37
    def test_cast_to_decimal32_9_1_from_decimal64_10_1_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_0 values (40, 100000000),(41, 99999999999999998),(42, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_17_0_data_start_index = 40
    def test_cast_to_decimal32_9_1_from_decimal64_17_0_data_end_index = 43
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_1 values (43, 100000000.9),(44, 9999999999999998.9),(45, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_17_1_data_start_index = 43
    def test_cast_to_decimal32_9_1_from_decimal64_17_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_8 values (46, 99999999.99999999),(47, 99999999.99999999),(48, 100000000.99999999),(49, 100000000.99999999),(50, 999999998.99999999),
      (51, 999999998.99999999),(52, 999999999.99999999),(53, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_17_8_data_start_index = 46
    def test_cast_to_decimal32_9_1_from_decimal64_17_8_data_end_index = 54
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_0 values (54, 100000000),(55, 999999999999999998),(56, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_18_0_data_start_index = 54
    def test_cast_to_decimal32_9_1_from_decimal64_18_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_1 values (57, 100000000.9),(58, 99999999999999998.9),(59, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_18_1_data_start_index = 57
    def test_cast_to_decimal32_9_1_from_decimal64_18_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_9 values (60, 99999999.999999999),(61, 99999999.999999999),(62, 100000000.999999999),(63, 100000000.999999999),(64, 999999998.999999999),
      (65, 999999998.999999999),(66, 999999999.999999999),(67, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal64_18_9_data_start_index = 60
    def test_cast_to_decimal32_9_1_from_decimal64_18_9_data_end_index = 68
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_9_0 values (68, 100000),(69, 999999998),(70, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_9_0_data_start_index = 68
    def test_cast_to_decimal32_9_4_from_decimal64_9_0_data_end_index = 71
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_9_1 values (71, 100000.9),(72, 99999998.9),(73, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_9_1_data_start_index = 71
    def test_cast_to_decimal32_9_4_from_decimal64_9_1_data_end_index = 74
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_10_0 values (74, 100000),(75, 9999999998),(76, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_10_0_data_start_index = 74
    def test_cast_to_decimal32_9_4_from_decimal64_10_0_data_end_index = 77
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_10_1 values (77, 100000.9),(78, 999999998.9),(79, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_10_1_data_start_index = 77
    def test_cast_to_decimal32_9_4_from_decimal64_10_1_data_end_index = 80
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_10_5 values (80, 99999.99999),(81, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_10_5_data_start_index = 80
    def test_cast_to_decimal32_9_4_from_decimal64_10_5_data_end_index = 82
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_17_0 values (82, 100000),(83, 99999999999999998),(84, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_17_0_data_start_index = 82
    def test_cast_to_decimal32_9_4_from_decimal64_17_0_data_end_index = 85
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_17_1 values (85, 100000.9),(86, 9999999999999998.9),(87, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_17_1_data_start_index = 85
    def test_cast_to_decimal32_9_4_from_decimal64_17_1_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_17_8 values (88, 99999.99999999),(89, 99999.99999999),(90, 100000.99999999),(91, 100000.99999999),(92, 999999998.99999999),
      (93, 999999998.99999999),(94, 999999999.99999999),(95, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_17_8_data_start_index = 88
    def test_cast_to_decimal32_9_4_from_decimal64_17_8_data_end_index = 96
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_18_0 values (96, 100000),(97, 999999999999999998),(98, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_18_0_data_start_index = 96
    def test_cast_to_decimal32_9_4_from_decimal64_18_0_data_end_index = 99
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_18_1 values (99, 100000.9),(100, 99999999999999998.9),(101, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_18_1_data_start_index = 99
    def test_cast_to_decimal32_9_4_from_decimal64_18_1_data_end_index = 102
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal64_18_9 values (102, 99999.999999999),(103, 99999.999999999),(104, 100000.999999999),(105, 100000.999999999),(106, 999999998.999999999),
      (107, 999999998.999999999),(108, 999999999.999999999),(109, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal64_18_9_data_start_index = 102
    def test_cast_to_decimal32_9_4_from_decimal64_18_9_data_end_index = 110
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_9_0 values (110, 10),(111, 999999998),(112, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_9_0_data_start_index = 110
    def test_cast_to_decimal32_9_8_from_decimal64_9_0_data_end_index = 113
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_9_1 values (113, 10.9),(114, 99999998.9),(115, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_9_1_data_start_index = 113
    def test_cast_to_decimal32_9_8_from_decimal64_9_1_data_end_index = 116
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_9_4 values (116, 10.9999),(117, 99998.9999),(118, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_9_4_data_start_index = 116
    def test_cast_to_decimal32_9_8_from_decimal64_9_4_data_end_index = 119
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_10_0 values (119, 10),(120, 9999999998),(121, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_10_0_data_start_index = 119
    def test_cast_to_decimal32_9_8_from_decimal64_10_0_data_end_index = 122
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_10_1 values (122, 10.9),(123, 999999998.9),(124, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_10_1_data_start_index = 122
    def test_cast_to_decimal32_9_8_from_decimal64_10_1_data_end_index = 125
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_10_5 values (125, 10.99999),(126, 99998.99999),(127, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_10_5_data_start_index = 125
    def test_cast_to_decimal32_9_8_from_decimal64_10_5_data_end_index = 128
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_10_9 values (128, 9.999999999),(129, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_10_9_data_start_index = 128
    def test_cast_to_decimal32_9_8_from_decimal64_10_9_data_end_index = 130
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_17_0 values (130, 10),(131, 99999999999999998),(132, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_17_0_data_start_index = 130
    def test_cast_to_decimal32_9_8_from_decimal64_17_0_data_end_index = 133
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_17_1 values (133, 10.9),(134, 9999999999999998.9),(135, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_17_1_data_start_index = 133
    def test_cast_to_decimal32_9_8_from_decimal64_17_1_data_end_index = 136
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_17_8 values (136, 10.99999999),(137, 999999998.99999999),(138, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_17_8_data_start_index = 136
    def test_cast_to_decimal32_9_8_from_decimal64_17_8_data_end_index = 139
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_17_16 values (139, 9.9999999999999999),(140, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_17_16_data_start_index = 139
    def test_cast_to_decimal32_9_8_from_decimal64_17_16_data_end_index = 141
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_18_0 values (141, 10),(142, 999999999999999998),(143, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_18_0_data_start_index = 141
    def test_cast_to_decimal32_9_8_from_decimal64_18_0_data_end_index = 144
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_18_1 values (144, 10.9),(145, 99999999999999998.9),(146, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_18_1_data_start_index = 144
    def test_cast_to_decimal32_9_8_from_decimal64_18_1_data_end_index = 147
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_18_9 values (147, 9.999999999),(148, 9.999999999),(149, 10.999999999),(150, 10.999999999),(151, 999999998.999999999),
      (152, 999999998.999999999),(153, 999999999.999999999),(154, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_18_9_data_start_index = 147
    def test_cast_to_decimal32_9_8_from_decimal64_18_9_data_end_index = 155
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal64_18_17 values (155, 9.99999999999999999),(156, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal64_18_17_data_start_index = 155
    def test_cast_to_decimal32_9_8_from_decimal64_18_17_data_end_index = 157
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_9_0 values (157, 1),(158, 999999998),(159, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_9_0_data_start_index = 157
    def test_cast_to_decimal32_9_9_from_decimal64_9_0_data_end_index = 160
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_9_1 values (160, 1.9),(161, 99999998.9),(162, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_9_1_data_start_index = 160
    def test_cast_to_decimal32_9_9_from_decimal64_9_1_data_end_index = 163
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_9_4 values (163, 1.9999),(164, 99998.9999),(165, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_9_4_data_start_index = 163
    def test_cast_to_decimal32_9_9_from_decimal64_9_4_data_end_index = 166
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_9_8 values (166, 1.99999999),(167, 8.99999999),(168, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_9_8_data_start_index = 166
    def test_cast_to_decimal32_9_9_from_decimal64_9_8_data_end_index = 169
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_0 values (169, 1),(170, 9999999998),(171, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_10_0_data_start_index = 169
    def test_cast_to_decimal32_9_9_from_decimal64_10_0_data_end_index = 172
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_1 values (172, 1.9),(173, 999999998.9),(174, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_10_1_data_start_index = 172
    def test_cast_to_decimal32_9_9_from_decimal64_10_1_data_end_index = 175
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_5 values (175, 1.99999),(176, 99998.99999),(177, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_10_5_data_start_index = 175
    def test_cast_to_decimal32_9_9_from_decimal64_10_5_data_end_index = 178
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_9 values (178, 1.999999999),(179, 8.999999999),(180, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_10_9_data_start_index = 178
    def test_cast_to_decimal32_9_9_from_decimal64_10_9_data_end_index = 181
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_10 values (181, 0.9999999999),(182, 0.9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_10_10_data_start_index = 181
    def test_cast_to_decimal32_9_9_from_decimal64_10_10_data_end_index = 183
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_10_10_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_10_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_0 values (183, 1),(184, 99999999999999998),(185, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_17_0_data_start_index = 183
    def test_cast_to_decimal32_9_9_from_decimal64_17_0_data_end_index = 186
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_1 values (186, 1.9),(187, 9999999999999998.9),(188, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_17_1_data_start_index = 186
    def test_cast_to_decimal32_9_9_from_decimal64_17_1_data_end_index = 189
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_8 values (189, 1.99999999),(190, 999999998.99999999),(191, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_17_8_data_start_index = 189
    def test_cast_to_decimal32_9_9_from_decimal64_17_8_data_end_index = 192
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_16 values (192, 0.9999999999999999),(193, 0.9999999999999999),(194, 1.9999999999999999),(195, 1.9999999999999999),(196, 8.9999999999999999),
      (197, 8.9999999999999999),(198, 9.9999999999999999),(199, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_17_16_data_start_index = 192
    def test_cast_to_decimal32_9_9_from_decimal64_17_16_data_end_index = 200
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_17 values (200, 0.99999999999999999),(201, 0.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_17_17_data_start_index = 200
    def test_cast_to_decimal32_9_9_from_decimal64_17_17_data_end_index = 202
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_17_17_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_17_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_0 values (202, 1),(203, 999999999999999998),(204, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_18_0_data_start_index = 202
    def test_cast_to_decimal32_9_9_from_decimal64_18_0_data_end_index = 205
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_1 values (205, 1.9),(206, 99999999999999998.9),(207, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_18_1_data_start_index = 205
    def test_cast_to_decimal32_9_9_from_decimal64_18_1_data_end_index = 208
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_9 values (208, 1.999999999),(209, 999999998.999999999),(210, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_18_9_data_start_index = 208
    def test_cast_to_decimal32_9_9_from_decimal64_18_9_data_end_index = 211
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_17 values (211, 0.99999999999999999),(212, 0.99999999999999999),(213, 1.99999999999999999),(214, 1.99999999999999999),(215, 8.99999999999999999),
      (216, 8.99999999999999999),(217, 9.99999999999999999),(218, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_18_17_data_start_index = 211
    def test_cast_to_decimal32_9_9_from_decimal64_18_17_data_end_index = 219
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_18 values (219, 0.999999999999999999),(220, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal64_18_18_data_start_index = 219
    def test_cast_to_decimal32_9_9_from_decimal64_18_18_data_end_index = 221
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_18 order by 1;'

}