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


suite("test_cast_to_decimal32_1_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_9_0 values (0, 10),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_9_0_data_start_index = 0
    def test_cast_to_decimal32_1_0_from_decimal64_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_9_1 values (3, 9.9),(4, 9.9),(5, 10.9),(6, 10.9),(7, 99999998.9),
      (8, 99999998.9),(9, 99999999.9),(10, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_9_1_data_start_index = 3
    def test_cast_to_decimal32_1_0_from_decimal64_9_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_9_4 values (11, 9.9999),(12, 9.9999),(13, 10.9999),(14, 10.9999),(15, 99998.9999),
      (16, 99998.9999),(17, 99999.9999),(18, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_9_4_data_start_index = 11
    def test_cast_to_decimal32_1_0_from_decimal64_9_4_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_9_8 values (19, 9.99999999),(20, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_9_8_data_start_index = 19
    def test_cast_to_decimal32_1_0_from_decimal64_9_8_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_0 values (21, 10),(22, 9999999998),(23, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_10_0_data_start_index = 21
    def test_cast_to_decimal32_1_0_from_decimal64_10_0_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_1 values (24, 9.9),(25, 9.9),(26, 10.9),(27, 10.9),(28, 999999998.9),
      (29, 999999998.9),(30, 999999999.9),(31, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_10_1_data_start_index = 24
    def test_cast_to_decimal32_1_0_from_decimal64_10_1_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_5 values (32, 9.99999),(33, 9.99999),(34, 10.99999),(35, 10.99999),(36, 99998.99999),
      (37, 99998.99999),(38, 99999.99999),(39, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_10_5_data_start_index = 32
    def test_cast_to_decimal32_1_0_from_decimal64_10_5_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_9 values (40, 9.999999999),(41, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_10_9_data_start_index = 40
    def test_cast_to_decimal32_1_0_from_decimal64_10_9_data_end_index = 42
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_0 values (42, 10),(43, 99999999999999998),(44, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_17_0_data_start_index = 42
    def test_cast_to_decimal32_1_0_from_decimal64_17_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_1 values (45, 9.9),(46, 9.9),(47, 10.9),(48, 10.9),(49, 9999999999999998.9),
      (50, 9999999999999998.9),(51, 9999999999999999.9),(52, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_17_1_data_start_index = 45
    def test_cast_to_decimal32_1_0_from_decimal64_17_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_8 values (53, 9.99999999),(54, 9.99999999),(55, 10.99999999),(56, 10.99999999),(57, 999999998.99999999),
      (58, 999999998.99999999),(59, 999999999.99999999),(60, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_17_8_data_start_index = 53
    def test_cast_to_decimal32_1_0_from_decimal64_17_8_data_end_index = 61
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_16 values (61, 9.9999999999999999),(62, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_17_16_data_start_index = 61
    def test_cast_to_decimal32_1_0_from_decimal64_17_16_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_0 values (63, 10),(64, 999999999999999998),(65, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_18_0_data_start_index = 63
    def test_cast_to_decimal32_1_0_from_decimal64_18_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_1 values (66, 9.9),(67, 9.9),(68, 10.9),(69, 10.9),(70, 99999999999999998.9),
      (71, 99999999999999998.9),(72, 99999999999999999.9),(73, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_18_1_data_start_index = 66
    def test_cast_to_decimal32_1_0_from_decimal64_18_1_data_end_index = 74
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_9 values (74, 9.999999999),(75, 9.999999999),(76, 10.999999999),(77, 10.999999999),(78, 999999998.999999999),
      (79, 999999998.999999999),(80, 999999999.999999999),(81, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_18_9_data_start_index = 74
    def test_cast_to_decimal32_1_0_from_decimal64_18_9_data_end_index = 82
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_17 values (82, 9.99999999999999999),(83, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal64_18_17_data_start_index = 82
    def test_cast_to_decimal32_1_0_from_decimal64_18_17_data_end_index = 84
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_9_0 values (84, 1),(85, 999999998),(86, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_9_0_data_start_index = 84
    def test_cast_to_decimal32_1_1_from_decimal64_9_0_data_end_index = 87
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_9_1 values (87, 1.9),(88, 99999998.9),(89, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_9_1_data_start_index = 87
    def test_cast_to_decimal32_1_1_from_decimal64_9_1_data_end_index = 90
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_9_4 values (90, 0.9999),(91, 0.9999),(92, 1.9999),(93, 1.9999),(94, 99998.9999),
      (95, 99998.9999),(96, 99999.9999),(97, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_9_4_data_start_index = 90
    def test_cast_to_decimal32_1_1_from_decimal64_9_4_data_end_index = 98
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_9_8 values (98, 0.99999999),(99, 0.99999999),(100, 1.99999999),(101, 1.99999999),(102, 8.99999999),
      (103, 8.99999999),(104, 9.99999999),(105, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_9_8_data_start_index = 98
    def test_cast_to_decimal32_1_1_from_decimal64_9_8_data_end_index = 106
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_9_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_9_9 values (106, 0.999999999),(107, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_9_9_data_start_index = 106
    def test_cast_to_decimal32_1_1_from_decimal64_9_9_data_end_index = 108
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_9_9_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_9_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_10_0 values (108, 1),(109, 9999999998),(110, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_10_0_data_start_index = 108
    def test_cast_to_decimal32_1_1_from_decimal64_10_0_data_end_index = 111
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_10_1 values (111, 1.9),(112, 999999998.9),(113, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_10_1_data_start_index = 111
    def test_cast_to_decimal32_1_1_from_decimal64_10_1_data_end_index = 114
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_10_5 values (114, 0.99999),(115, 0.99999),(116, 1.99999),(117, 1.99999),(118, 99998.99999),
      (119, 99998.99999),(120, 99999.99999),(121, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_10_5_data_start_index = 114
    def test_cast_to_decimal32_1_1_from_decimal64_10_5_data_end_index = 122
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_10_9 values (122, 0.999999999),(123, 0.999999999),(124, 1.999999999),(125, 1.999999999),(126, 8.999999999),
      (127, 8.999999999),(128, 9.999999999),(129, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_10_9_data_start_index = 122
    def test_cast_to_decimal32_1_1_from_decimal64_10_9_data_end_index = 130
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_10_10 values (130, 0.9999999999),(131, 0.9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_10_10_data_start_index = 130
    def test_cast_to_decimal32_1_1_from_decimal64_10_10_data_end_index = 132
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_10_10_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_10_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_17_0 values (132, 1),(133, 99999999999999998),(134, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_17_0_data_start_index = 132
    def test_cast_to_decimal32_1_1_from_decimal64_17_0_data_end_index = 135
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_17_1 values (135, 1.9),(136, 9999999999999998.9),(137, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_17_1_data_start_index = 135
    def test_cast_to_decimal32_1_1_from_decimal64_17_1_data_end_index = 138
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_17_8 values (138, 0.99999999),(139, 0.99999999),(140, 1.99999999),(141, 1.99999999),(142, 999999998.99999999),
      (143, 999999998.99999999),(144, 999999999.99999999),(145, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_17_8_data_start_index = 138
    def test_cast_to_decimal32_1_1_from_decimal64_17_8_data_end_index = 146
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_17_16 values (146, 0.9999999999999999),(147, 0.9999999999999999),(148, 1.9999999999999999),(149, 1.9999999999999999),(150, 8.9999999999999999),
      (151, 8.9999999999999999),(152, 9.9999999999999999),(153, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_17_16_data_start_index = 146
    def test_cast_to_decimal32_1_1_from_decimal64_17_16_data_end_index = 154
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_17_17 values (154, 0.99999999999999999),(155, 0.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_17_17_data_start_index = 154
    def test_cast_to_decimal32_1_1_from_decimal64_17_17_data_end_index = 156
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_17_17_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_17_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_18_0 values (156, 1),(157, 999999999999999998),(158, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_18_0_data_start_index = 156
    def test_cast_to_decimal32_1_1_from_decimal64_18_0_data_end_index = 159
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_18_1 values (159, 1.9),(160, 99999999999999998.9),(161, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_18_1_data_start_index = 159
    def test_cast_to_decimal32_1_1_from_decimal64_18_1_data_end_index = 162
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_18_9 values (162, 0.999999999),(163, 0.999999999),(164, 1.999999999),(165, 1.999999999),(166, 999999998.999999999),
      (167, 999999998.999999999),(168, 999999999.999999999),(169, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_18_9_data_start_index = 162
    def test_cast_to_decimal32_1_1_from_decimal64_18_9_data_end_index = 170
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_18_17 values (170, 0.99999999999999999),(171, 0.99999999999999999),(172, 1.99999999999999999),(173, 1.99999999999999999),(174, 8.99999999999999999),
      (175, 8.99999999999999999),(176, 9.99999999999999999),(177, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_18_17_data_start_index = 170
    def test_cast_to_decimal32_1_1_from_decimal64_18_17_data_end_index = 178
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_38 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal64_18_18 values (178, 0.999999999999999999),(179, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal64_18_18_data_start_index = 178
    def test_cast_to_decimal32_1_1_from_decimal64_18_18_data_end_index = 180
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_39 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal64_18_18 order by 1;'

}