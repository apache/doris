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


suite("test_cast_to_decimal32_4_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_9_0 values (0, 10000),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_9_0_data_start_index = 0
    def test_cast_to_decimal32_4_0_from_decimal64_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_9_1 values (3, 9999.9),(4, 9999.9),(5, 10000.9),(6, 10000.9),(7, 99999998.9),
      (8, 99999998.9),(9, 99999999.9),(10, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_9_1_data_start_index = 3
    def test_cast_to_decimal32_4_0_from_decimal64_9_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_9_4 values (11, 9999.9999),(12, 9999.9999),(13, 10000.9999),(14, 10000.9999),(15, 99998.9999),
      (16, 99998.9999),(17, 99999.9999),(18, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_9_4_data_start_index = 11
    def test_cast_to_decimal32_4_0_from_decimal64_9_4_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_10_0 values (19, 10000),(20, 9999999998),(21, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_10_0_data_start_index = 19
    def test_cast_to_decimal32_4_0_from_decimal64_10_0_data_end_index = 22
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_10_1 values (22, 9999.9),(23, 9999.9),(24, 10000.9),(25, 10000.9),(26, 999999998.9),
      (27, 999999998.9),(28, 999999999.9),(29, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_10_1_data_start_index = 22
    def test_cast_to_decimal32_4_0_from_decimal64_10_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_10_5 values (30, 9999.99999),(31, 9999.99999),(32, 10000.99999),(33, 10000.99999),(34, 99998.99999),
      (35, 99998.99999),(36, 99999.99999),(37, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_10_5_data_start_index = 30
    def test_cast_to_decimal32_4_0_from_decimal64_10_5_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_17_0 values (38, 10000),(39, 99999999999999998),(40, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_17_0_data_start_index = 38
    def test_cast_to_decimal32_4_0_from_decimal64_17_0_data_end_index = 41
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_17_1 values (41, 9999.9),(42, 9999.9),(43, 10000.9),(44, 10000.9),(45, 9999999999999998.9),
      (46, 9999999999999998.9),(47, 9999999999999999.9),(48, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_17_1_data_start_index = 41
    def test_cast_to_decimal32_4_0_from_decimal64_17_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_17_8 values (49, 9999.99999999),(50, 9999.99999999),(51, 10000.99999999),(52, 10000.99999999),(53, 999999998.99999999),
      (54, 999999998.99999999),(55, 999999999.99999999),(56, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_17_8_data_start_index = 49
    def test_cast_to_decimal32_4_0_from_decimal64_17_8_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_18_0 values (57, 10000),(58, 999999999999999998),(59, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_18_0_data_start_index = 57
    def test_cast_to_decimal32_4_0_from_decimal64_18_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_18_1 values (60, 9999.9),(61, 9999.9),(62, 10000.9),(63, 10000.9),(64, 99999999999999998.9),
      (65, 99999999999999998.9),(66, 99999999999999999.9),(67, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_18_1_data_start_index = 60
    def test_cast_to_decimal32_4_0_from_decimal64_18_1_data_end_index = 68
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal64_18_9 values (68, 9999.999999999),(69, 9999.999999999),(70, 10000.999999999),(71, 10000.999999999),(72, 999999998.999999999),
      (73, 999999998.999999999),(74, 999999999.999999999),(75, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal64_18_9_data_start_index = 68
    def test_cast_to_decimal32_4_0_from_decimal64_18_9_data_end_index = 76
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_9_0 values (76, 1000),(77, 999999998),(78, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_9_0_data_start_index = 76
    def test_cast_to_decimal32_4_1_from_decimal64_9_0_data_end_index = 79
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_9_1 values (79, 1000.9),(80, 99999998.9),(81, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_9_1_data_start_index = 79
    def test_cast_to_decimal32_4_1_from_decimal64_9_1_data_end_index = 82
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_9_4 values (82, 999.9999),(83, 999.9999),(84, 1000.9999),(85, 1000.9999),(86, 99998.9999),
      (87, 99998.9999),(88, 99999.9999),(89, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_9_4_data_start_index = 82
    def test_cast_to_decimal32_4_1_from_decimal64_9_4_data_end_index = 90
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_10_0 values (90, 1000),(91, 9999999998),(92, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_10_0_data_start_index = 90
    def test_cast_to_decimal32_4_1_from_decimal64_10_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_10_1 values (93, 1000.9),(94, 999999998.9),(95, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_10_1_data_start_index = 93
    def test_cast_to_decimal32_4_1_from_decimal64_10_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_10_5 values (96, 999.99999),(97, 999.99999),(98, 1000.99999),(99, 1000.99999),(100, 99998.99999),
      (101, 99998.99999),(102, 99999.99999),(103, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_10_5_data_start_index = 96
    def test_cast_to_decimal32_4_1_from_decimal64_10_5_data_end_index = 104
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_17_0 values (104, 1000),(105, 99999999999999998),(106, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_17_0_data_start_index = 104
    def test_cast_to_decimal32_4_1_from_decimal64_17_0_data_end_index = 107
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_17_1 values (107, 1000.9),(108, 9999999999999998.9),(109, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_17_1_data_start_index = 107
    def test_cast_to_decimal32_4_1_from_decimal64_17_1_data_end_index = 110
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_17_8 values (110, 999.99999999),(111, 999.99999999),(112, 1000.99999999),(113, 1000.99999999),(114, 999999998.99999999),
      (115, 999999998.99999999),(116, 999999999.99999999),(117, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_17_8_data_start_index = 110
    def test_cast_to_decimal32_4_1_from_decimal64_17_8_data_end_index = 118
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_18_0 values (118, 1000),(119, 999999999999999998),(120, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_18_0_data_start_index = 118
    def test_cast_to_decimal32_4_1_from_decimal64_18_0_data_end_index = 121
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_18_1 values (121, 1000.9),(122, 99999999999999998.9),(123, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_18_1_data_start_index = 121
    def test_cast_to_decimal32_4_1_from_decimal64_18_1_data_end_index = 124
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal64_18_9 values (124, 999.999999999),(125, 999.999999999),(126, 1000.999999999),(127, 1000.999999999),(128, 999999998.999999999),
      (129, 999999998.999999999),(130, 999999999.999999999),(131, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal64_18_9_data_start_index = 124
    def test_cast_to_decimal32_4_1_from_decimal64_18_9_data_end_index = 132
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_9_0 values (132, 100),(133, 999999998),(134, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_9_0_data_start_index = 132
    def test_cast_to_decimal32_4_2_from_decimal64_9_0_data_end_index = 135
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_9_1 values (135, 100.9),(136, 99999998.9),(137, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_9_1_data_start_index = 135
    def test_cast_to_decimal32_4_2_from_decimal64_9_1_data_end_index = 138
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_9_4 values (138, 99.9999),(139, 99.9999),(140, 100.9999),(141, 100.9999),(142, 99998.9999),
      (143, 99998.9999),(144, 99999.9999),(145, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_9_4_data_start_index = 138
    def test_cast_to_decimal32_4_2_from_decimal64_9_4_data_end_index = 146
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_0 values (146, 100),(147, 9999999998),(148, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_10_0_data_start_index = 146
    def test_cast_to_decimal32_4_2_from_decimal64_10_0_data_end_index = 149
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_1 values (149, 100.9),(150, 999999998.9),(151, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_10_1_data_start_index = 149
    def test_cast_to_decimal32_4_2_from_decimal64_10_1_data_end_index = 152
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_5 values (152, 99.99999),(153, 99.99999),(154, 100.99999),(155, 100.99999),(156, 99998.99999),
      (157, 99998.99999),(158, 99999.99999),(159, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_10_5_data_start_index = 152
    def test_cast_to_decimal32_4_2_from_decimal64_10_5_data_end_index = 160
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_0 values (160, 100),(161, 99999999999999998),(162, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_17_0_data_start_index = 160
    def test_cast_to_decimal32_4_2_from_decimal64_17_0_data_end_index = 163
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_1 values (163, 100.9),(164, 9999999999999998.9),(165, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_17_1_data_start_index = 163
    def test_cast_to_decimal32_4_2_from_decimal64_17_1_data_end_index = 166
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_8 values (166, 99.99999999),(167, 99.99999999),(168, 100.99999999),(169, 100.99999999),(170, 999999998.99999999),
      (171, 999999998.99999999),(172, 999999999.99999999),(173, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_17_8_data_start_index = 166
    def test_cast_to_decimal32_4_2_from_decimal64_17_8_data_end_index = 174
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_0 values (174, 100),(175, 999999999999999998),(176, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_18_0_data_start_index = 174
    def test_cast_to_decimal32_4_2_from_decimal64_18_0_data_end_index = 177
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_1 values (177, 100.9),(178, 99999999999999998.9),(179, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_18_1_data_start_index = 177
    def test_cast_to_decimal32_4_2_from_decimal64_18_1_data_end_index = 180
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_9 values (180, 99.999999999),(181, 99.999999999),(182, 100.999999999),(183, 100.999999999),(184, 999999998.999999999),
      (185, 999999998.999999999),(186, 999999999.999999999),(187, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal64_18_9_data_start_index = 180
    def test_cast_to_decimal32_4_2_from_decimal64_18_9_data_end_index = 188
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_9_0 values (188, 10),(189, 999999998),(190, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_9_0_data_start_index = 188
    def test_cast_to_decimal32_4_3_from_decimal64_9_0_data_end_index = 191
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_9_1 values (191, 10.9),(192, 99999998.9),(193, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_9_1_data_start_index = 191
    def test_cast_to_decimal32_4_3_from_decimal64_9_1_data_end_index = 194
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_9_4 values (194, 9.9999),(195, 9.9999),(196, 10.9999),(197, 10.9999),(198, 99998.9999),
      (199, 99998.9999),(200, 99999.9999),(201, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_9_4_data_start_index = 194
    def test_cast_to_decimal32_4_3_from_decimal64_9_4_data_end_index = 202
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_9_8 values (202, 9.99999999),(203, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_9_8_data_start_index = 202
    def test_cast_to_decimal32_4_3_from_decimal64_9_8_data_end_index = 204
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_0 values (204, 10),(205, 9999999998),(206, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_10_0_data_start_index = 204
    def test_cast_to_decimal32_4_3_from_decimal64_10_0_data_end_index = 207
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_1 values (207, 10.9),(208, 999999998.9),(209, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_10_1_data_start_index = 207
    def test_cast_to_decimal32_4_3_from_decimal64_10_1_data_end_index = 210
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_5 values (210, 9.99999),(211, 9.99999),(212, 10.99999),(213, 10.99999),(214, 99998.99999),
      (215, 99998.99999),(216, 99999.99999),(217, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_10_5_data_start_index = 210
    def test_cast_to_decimal32_4_3_from_decimal64_10_5_data_end_index = 218
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_9 values (218, 9.999999999),(219, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_10_9_data_start_index = 218
    def test_cast_to_decimal32_4_3_from_decimal64_10_9_data_end_index = 220
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_0 values (220, 10),(221, 99999999999999998),(222, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_17_0_data_start_index = 220
    def test_cast_to_decimal32_4_3_from_decimal64_17_0_data_end_index = 223
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_1 values (223, 10.9),(224, 9999999999999998.9),(225, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_17_1_data_start_index = 223
    def test_cast_to_decimal32_4_3_from_decimal64_17_1_data_end_index = 226
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_8 values (226, 9.99999999),(227, 9.99999999),(228, 10.99999999),(229, 10.99999999),(230, 999999998.99999999),
      (231, 999999998.99999999),(232, 999999999.99999999),(233, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_17_8_data_start_index = 226
    def test_cast_to_decimal32_4_3_from_decimal64_17_8_data_end_index = 234
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_16 values (234, 9.9999999999999999),(235, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_17_16_data_start_index = 234
    def test_cast_to_decimal32_4_3_from_decimal64_17_16_data_end_index = 236
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_0 values (236, 10),(237, 999999999999999998),(238, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_18_0_data_start_index = 236
    def test_cast_to_decimal32_4_3_from_decimal64_18_0_data_end_index = 239
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_1 values (239, 10.9),(240, 99999999999999998.9),(241, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_18_1_data_start_index = 239
    def test_cast_to_decimal32_4_3_from_decimal64_18_1_data_end_index = 242
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_9 values (242, 9.999999999),(243, 9.999999999),(244, 10.999999999),(245, 10.999999999),(246, 999999998.999999999),
      (247, 999999998.999999999),(248, 999999999.999999999),(249, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_18_9_data_start_index = 242
    def test_cast_to_decimal32_4_3_from_decimal64_18_9_data_end_index = 250
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_17 values (250, 9.99999999999999999),(251, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal64_18_17_data_start_index = 250
    def test_cast_to_decimal32_4_3_from_decimal64_18_17_data_end_index = 252
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_9_0 values (252, 1),(253, 999999998),(254, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_9_0_data_start_index = 252
    def test_cast_to_decimal32_4_4_from_decimal64_9_0_data_end_index = 255
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_9_1 values (255, 1.9),(256, 99999998.9),(257, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_9_1_data_start_index = 255
    def test_cast_to_decimal32_4_4_from_decimal64_9_1_data_end_index = 258
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_9_4 values (258, 1.9999),(259, 99998.9999),(260, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_9_4_data_start_index = 258
    def test_cast_to_decimal32_4_4_from_decimal64_9_4_data_end_index = 261
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_9_8 values (261, 0.99999999),(262, 0.99999999),(263, 1.99999999),(264, 1.99999999),(265, 8.99999999),
      (266, 8.99999999),(267, 9.99999999),(268, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_9_8_data_start_index = 261
    def test_cast_to_decimal32_4_4_from_decimal64_9_8_data_end_index = 269
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_9_9;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_9_9 values (269, 0.999999999),(270, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_9_9_data_start_index = 269
    def test_cast_to_decimal32_4_4_from_decimal64_9_9_data_end_index = 271
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_9_9_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_9_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_10_0 values (271, 1),(272, 9999999998),(273, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_10_0_data_start_index = 271
    def test_cast_to_decimal32_4_4_from_decimal64_10_0_data_end_index = 274
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_10_1 values (274, 1.9),(275, 999999998.9),(276, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_10_1_data_start_index = 274
    def test_cast_to_decimal32_4_4_from_decimal64_10_1_data_end_index = 277
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_10_5 values (277, 0.99999),(278, 0.99999),(279, 1.99999),(280, 1.99999),(281, 99998.99999),
      (282, 99998.99999),(283, 99999.99999),(284, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_10_5_data_start_index = 277
    def test_cast_to_decimal32_4_4_from_decimal64_10_5_data_end_index = 285
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_10_9 values (285, 0.999999999),(286, 0.999999999),(287, 1.999999999),(288, 1.999999999),(289, 8.999999999),
      (290, 8.999999999),(291, 9.999999999),(292, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_10_9_data_start_index = 285
    def test_cast_to_decimal32_4_4_from_decimal64_10_9_data_end_index = 293
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_10_10 values (293, 0.9999999999),(294, 0.9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_10_10_data_start_index = 293
    def test_cast_to_decimal32_4_4_from_decimal64_10_10_data_end_index = 295
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_10_10_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_10_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_17_0 values (295, 1),(296, 99999999999999998),(297, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_17_0_data_start_index = 295
    def test_cast_to_decimal32_4_4_from_decimal64_17_0_data_end_index = 298
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_17_1 values (298, 1.9),(299, 9999999999999998.9),(300, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_17_1_data_start_index = 298
    def test_cast_to_decimal32_4_4_from_decimal64_17_1_data_end_index = 301
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_17_8 values (301, 0.99999999),(302, 0.99999999),(303, 1.99999999),(304, 1.99999999),(305, 999999998.99999999),
      (306, 999999998.99999999),(307, 999999999.99999999),(308, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_17_8_data_start_index = 301
    def test_cast_to_decimal32_4_4_from_decimal64_17_8_data_end_index = 309
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_17_16 values (309, 0.9999999999999999),(310, 0.9999999999999999),(311, 1.9999999999999999),(312, 1.9999999999999999),(313, 8.9999999999999999),
      (314, 8.9999999999999999),(315, 9.9999999999999999),(316, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_17_16_data_start_index = 309
    def test_cast_to_decimal32_4_4_from_decimal64_17_16_data_end_index = 317
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_17_17 values (317, 0.99999999999999999),(318, 0.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_17_17_data_start_index = 317
    def test_cast_to_decimal32_4_4_from_decimal64_17_17_data_end_index = 319
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_17_17_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_17_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_18_0 values (319, 1),(320, 999999999999999998),(321, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_18_0_data_start_index = 319
    def test_cast_to_decimal32_4_4_from_decimal64_18_0_data_end_index = 322
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_18_1 values (322, 1.9),(323, 99999999999999998.9),(324, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_18_1_data_start_index = 322
    def test_cast_to_decimal32_4_4_from_decimal64_18_1_data_end_index = 325
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_18_9 values (325, 0.999999999),(326, 0.999999999),(327, 1.999999999),(328, 1.999999999),(329, 999999998.999999999),
      (330, 999999998.999999999),(331, 999999999.999999999),(332, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_18_9_data_start_index = 325
    def test_cast_to_decimal32_4_4_from_decimal64_18_9_data_end_index = 333
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_18_17 values (333, 0.99999999999999999),(334, 0.99999999999999999),(335, 1.99999999999999999),(336, 1.99999999999999999),(337, 8.99999999999999999),
      (338, 8.99999999999999999),(339, 9.99999999999999999),(340, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_18_17_data_start_index = 333
    def test_cast_to_decimal32_4_4_from_decimal64_18_17_data_end_index = 341
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal64_18_18 values (341, 0.999999999999999999),(342, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal64_18_18_data_start_index = 341
    def test_cast_to_decimal32_4_4_from_decimal64_18_18_data_end_index = 343
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal64_18_18 order by 1;'

}