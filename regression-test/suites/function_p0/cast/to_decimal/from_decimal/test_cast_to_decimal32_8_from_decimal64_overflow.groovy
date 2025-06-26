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


suite("test_cast_to_decimal32_8_from_decimal64_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_9_0 values (0, 100000000),(1, 999999998),(2, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_9_0_data_start_index = 0
    def test_cast_to_decimal32_8_0_from_decimal64_9_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_9_1 values (3, 99999999.9),(4, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_9_1_data_start_index = 3
    def test_cast_to_decimal32_8_0_from_decimal64_9_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_10_0 values (5, 100000000),(6, 9999999998),(7, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_10_0_data_start_index = 5
    def test_cast_to_decimal32_8_0_from_decimal64_10_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_10_1 values (8, 99999999.9),(9, 99999999.9),(10, 100000000.9),(11, 100000000.9),(12, 999999998.9),
      (13, 999999998.9),(14, 999999999.9),(15, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_10_1_data_start_index = 8
    def test_cast_to_decimal32_8_0_from_decimal64_10_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_17_0 values (16, 100000000),(17, 99999999999999998),(18, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_17_0_data_start_index = 16
    def test_cast_to_decimal32_8_0_from_decimal64_17_0_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_17_1 values (19, 99999999.9),(20, 99999999.9),(21, 100000000.9),(22, 100000000.9),(23, 9999999999999998.9),
      (24, 9999999999999998.9),(25, 9999999999999999.9),(26, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_17_1_data_start_index = 19
    def test_cast_to_decimal32_8_0_from_decimal64_17_1_data_end_index = 27
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_17_8 values (27, 99999999.99999999),(28, 99999999.99999999),(29, 100000000.99999999),(30, 100000000.99999999),(31, 999999998.99999999),
      (32, 999999998.99999999),(33, 999999999.99999999),(34, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_17_8_data_start_index = 27
    def test_cast_to_decimal32_8_0_from_decimal64_17_8_data_end_index = 35
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_18_0 values (35, 100000000),(36, 999999999999999998),(37, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_18_0_data_start_index = 35
    def test_cast_to_decimal32_8_0_from_decimal64_18_0_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_18_1 values (38, 99999999.9),(39, 99999999.9),(40, 100000000.9),(41, 100000000.9),(42, 99999999999999998.9),
      (43, 99999999999999998.9),(44, 99999999999999999.9),(45, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_18_1_data_start_index = 38
    def test_cast_to_decimal32_8_0_from_decimal64_18_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal64_18_9 values (46, 99999999.999999999),(47, 99999999.999999999),(48, 100000000.999999999),(49, 100000000.999999999),(50, 999999998.999999999),
      (51, 999999998.999999999),(52, 999999999.999999999),(53, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal64_18_9_data_start_index = 46
    def test_cast_to_decimal32_8_0_from_decimal64_18_9_data_end_index = 54
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_9_0 values (54, 10000000),(55, 999999998),(56, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_9_0_data_start_index = 54
    def test_cast_to_decimal32_8_1_from_decimal64_9_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_9_1 values (57, 10000000.9),(58, 99999998.9),(59, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_9_1_data_start_index = 57
    def test_cast_to_decimal32_8_1_from_decimal64_9_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_10_0 values (60, 10000000),(61, 9999999998),(62, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_10_0_data_start_index = 60
    def test_cast_to_decimal32_8_1_from_decimal64_10_0_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_10_1 values (63, 10000000.9),(64, 999999998.9),(65, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_10_1_data_start_index = 63
    def test_cast_to_decimal32_8_1_from_decimal64_10_1_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_17_0 values (66, 10000000),(67, 99999999999999998),(68, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_17_0_data_start_index = 66
    def test_cast_to_decimal32_8_1_from_decimal64_17_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_17_1 values (69, 10000000.9),(70, 9999999999999998.9),(71, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_17_1_data_start_index = 69
    def test_cast_to_decimal32_8_1_from_decimal64_17_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_17_8 values (72, 9999999.99999999),(73, 9999999.99999999),(74, 10000000.99999999),(75, 10000000.99999999),(76, 999999998.99999999),
      (77, 999999998.99999999),(78, 999999999.99999999),(79, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_17_8_data_start_index = 72
    def test_cast_to_decimal32_8_1_from_decimal64_17_8_data_end_index = 80
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_18_0 values (80, 10000000),(81, 999999999999999998),(82, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_18_0_data_start_index = 80
    def test_cast_to_decimal32_8_1_from_decimal64_18_0_data_end_index = 83
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_18_1 values (83, 10000000.9),(84, 99999999999999998.9),(85, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_18_1_data_start_index = 83
    def test_cast_to_decimal32_8_1_from_decimal64_18_1_data_end_index = 86
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal64_18_9 values (86, 9999999.999999999),(87, 9999999.999999999),(88, 10000000.999999999),(89, 10000000.999999999),(90, 999999998.999999999),
      (91, 999999998.999999999),(92, 999999999.999999999),(93, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal64_18_9_data_start_index = 86
    def test_cast_to_decimal32_8_1_from_decimal64_18_9_data_end_index = 94
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_9_0 values (94, 10000),(95, 999999998),(96, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_9_0_data_start_index = 94
    def test_cast_to_decimal32_8_4_from_decimal64_9_0_data_end_index = 97
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_9_1 values (97, 10000.9),(98, 99999998.9),(99, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_9_1_data_start_index = 97
    def test_cast_to_decimal32_8_4_from_decimal64_9_1_data_end_index = 100
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_9_4 values (100, 10000.9999),(101, 99998.9999),(102, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_9_4_data_start_index = 100
    def test_cast_to_decimal32_8_4_from_decimal64_9_4_data_end_index = 103
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_10_0 values (103, 10000),(104, 9999999998),(105, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_10_0_data_start_index = 103
    def test_cast_to_decimal32_8_4_from_decimal64_10_0_data_end_index = 106
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_10_1 values (106, 10000.9),(107, 999999998.9),(108, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_10_1_data_start_index = 106
    def test_cast_to_decimal32_8_4_from_decimal64_10_1_data_end_index = 109
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_10_5 values (109, 9999.99999),(110, 9999.99999),(111, 10000.99999),(112, 10000.99999),(113, 99998.99999),
      (114, 99998.99999),(115, 99999.99999),(116, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_10_5_data_start_index = 109
    def test_cast_to_decimal32_8_4_from_decimal64_10_5_data_end_index = 117
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_17_0 values (117, 10000),(118, 99999999999999998),(119, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_17_0_data_start_index = 117
    def test_cast_to_decimal32_8_4_from_decimal64_17_0_data_end_index = 120
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_17_1 values (120, 10000.9),(121, 9999999999999998.9),(122, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_17_1_data_start_index = 120
    def test_cast_to_decimal32_8_4_from_decimal64_17_1_data_end_index = 123
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_17_8 values (123, 9999.99999999),(124, 9999.99999999),(125, 10000.99999999),(126, 10000.99999999),(127, 999999998.99999999),
      (128, 999999998.99999999),(129, 999999999.99999999),(130, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_17_8_data_start_index = 123
    def test_cast_to_decimal32_8_4_from_decimal64_17_8_data_end_index = 131
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_18_0 values (131, 10000),(132, 999999999999999998),(133, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_18_0_data_start_index = 131
    def test_cast_to_decimal32_8_4_from_decimal64_18_0_data_end_index = 134
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_18_1 values (134, 10000.9),(135, 99999999999999998.9),(136, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_18_1_data_start_index = 134
    def test_cast_to_decimal32_8_4_from_decimal64_18_1_data_end_index = 137
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal64_18_9 values (137, 9999.999999999),(138, 9999.999999999),(139, 10000.999999999),(140, 10000.999999999),(141, 999999998.999999999),
      (142, 999999998.999999999),(143, 999999999.999999999),(144, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal64_18_9_data_start_index = 137
    def test_cast_to_decimal32_8_4_from_decimal64_18_9_data_end_index = 145
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_9_0 values (145, 10),(146, 999999998),(147, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_9_0_data_start_index = 145
    def test_cast_to_decimal32_8_7_from_decimal64_9_0_data_end_index = 148
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_9_1 values (148, 10.9),(149, 99999998.9),(150, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_9_1_data_start_index = 148
    def test_cast_to_decimal32_8_7_from_decimal64_9_1_data_end_index = 151
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_9_4 values (151, 10.9999),(152, 99998.9999),(153, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_9_4_data_start_index = 151
    def test_cast_to_decimal32_8_7_from_decimal64_9_4_data_end_index = 154
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_9_8 values (154, 9.99999999),(155, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_9_8_data_start_index = 154
    def test_cast_to_decimal32_8_7_from_decimal64_9_8_data_end_index = 156
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_10_0 values (156, 10),(157, 9999999998),(158, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_10_0_data_start_index = 156
    def test_cast_to_decimal32_8_7_from_decimal64_10_0_data_end_index = 159
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_10_1 values (159, 10.9),(160, 999999998.9),(161, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_10_1_data_start_index = 159
    def test_cast_to_decimal32_8_7_from_decimal64_10_1_data_end_index = 162
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_10_5 values (162, 10.99999),(163, 99998.99999),(164, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_10_5_data_start_index = 162
    def test_cast_to_decimal32_8_7_from_decimal64_10_5_data_end_index = 165
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_10_9 values (165, 9.999999999),(166, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_10_9_data_start_index = 165
    def test_cast_to_decimal32_8_7_from_decimal64_10_9_data_end_index = 167
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_17_0 values (167, 10),(168, 99999999999999998),(169, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_17_0_data_start_index = 167
    def test_cast_to_decimal32_8_7_from_decimal64_17_0_data_end_index = 170
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_17_1 values (170, 10.9),(171, 9999999999999998.9),(172, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_17_1_data_start_index = 170
    def test_cast_to_decimal32_8_7_from_decimal64_17_1_data_end_index = 173
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_17_8 values (173, 9.99999999),(174, 9.99999999),(175, 10.99999999),(176, 10.99999999),(177, 999999998.99999999),
      (178, 999999998.99999999),(179, 999999999.99999999),(180, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_17_8_data_start_index = 173
    def test_cast_to_decimal32_8_7_from_decimal64_17_8_data_end_index = 181
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_17_16 values (181, 9.9999999999999999),(182, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_17_16_data_start_index = 181
    def test_cast_to_decimal32_8_7_from_decimal64_17_16_data_end_index = 183
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_18_0 values (183, 10),(184, 999999999999999998),(185, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_18_0_data_start_index = 183
    def test_cast_to_decimal32_8_7_from_decimal64_18_0_data_end_index = 186
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_18_1 values (186, 10.9),(187, 99999999999999998.9),(188, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_18_1_data_start_index = 186
    def test_cast_to_decimal32_8_7_from_decimal64_18_1_data_end_index = 189
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_18_9 values (189, 9.999999999),(190, 9.999999999),(191, 10.999999999),(192, 10.999999999),(193, 999999998.999999999),
      (194, 999999998.999999999),(195, 999999999.999999999),(196, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_18_9_data_start_index = 189
    def test_cast_to_decimal32_8_7_from_decimal64_18_9_data_end_index = 197
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal64_18_17 values (197, 9.99999999999999999),(198, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal64_18_17_data_start_index = 197
    def test_cast_to_decimal32_8_7_from_decimal64_18_17_data_end_index = 199
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_9_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_9_0 values (199, 1),(200, 999999998),(201, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_9_0_data_start_index = 199
    def test_cast_to_decimal32_8_8_from_decimal64_9_0_data_end_index = 202
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_9_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_9_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_9_1 values (202, 1.9),(203, 99999998.9),(204, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_9_1_data_start_index = 202
    def test_cast_to_decimal32_8_8_from_decimal64_9_1_data_end_index = 205
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_9_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_9_4;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_9_4 values (205, 1.9999),(206, 99998.9999),(207, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_9_4_data_start_index = 205
    def test_cast_to_decimal32_8_8_from_decimal64_9_4_data_end_index = 208
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_9_4_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_9_8;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_9_8 values (208, 1.99999999),(209, 8.99999999),(210, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_9_8_data_start_index = 208
    def test_cast_to_decimal32_8_8_from_decimal64_9_8_data_end_index = 211
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_9_8_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_9_9;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_9_9 values (211, 0.999999999),(212, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_9_9_data_start_index = 211
    def test_cast_to_decimal32_8_8_from_decimal64_9_9_data_end_index = 213
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_9_9_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_9_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_10_0 values (213, 1),(214, 9999999998),(215, 9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_10_0_data_start_index = 213
    def test_cast_to_decimal32_8_8_from_decimal64_10_0_data_end_index = 216
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_10_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_10_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_10_1 values (216, 1.9),(217, 999999998.9),(218, 999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_10_1_data_start_index = 216
    def test_cast_to_decimal32_8_8_from_decimal64_10_1_data_end_index = 219
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_10_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_10_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_10_5 values (219, 1.99999),(220, 99998.99999),(221, 99999.99999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_10_5_data_start_index = 219
    def test_cast_to_decimal32_8_8_from_decimal64_10_5_data_end_index = 222
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_10_5_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_10_5_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_5 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_10_9 values (222, 0.999999999),(223, 0.999999999),(224, 1.999999999),(225, 1.999999999),(226, 8.999999999),
      (227, 8.999999999),(228, 9.999999999),(229, 9.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_10_9_data_start_index = 222
    def test_cast_to_decimal32_8_8_from_decimal64_10_9_data_end_index = 230
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_10_9_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_10_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_10_10 values (230, 0.9999999999),(231, 0.9999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_10_10_data_start_index = 230
    def test_cast_to_decimal32_8_8_from_decimal64_10_10_data_end_index = 232
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_10_10_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_10_10_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_10 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_17_0 values (232, 1),(233, 99999999999999998),(234, 99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_17_0_data_start_index = 232
    def test_cast_to_decimal32_8_8_from_decimal64_17_0_data_end_index = 235
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_17_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_17_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_17_1 values (235, 1.9),(236, 9999999999999998.9),(237, 9999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_17_1_data_start_index = 235
    def test_cast_to_decimal32_8_8_from_decimal64_17_1_data_end_index = 238
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_17_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_17_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_17_8 values (238, 1.99999999),(239, 999999998.99999999),(240, 999999999.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_17_8_data_start_index = 238
    def test_cast_to_decimal32_8_8_from_decimal64_17_8_data_end_index = 241
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_17_8_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_17_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_17_16 values (241, 0.9999999999999999),(242, 0.9999999999999999),(243, 1.9999999999999999),(244, 1.9999999999999999),(245, 8.9999999999999999),
      (246, 8.9999999999999999),(247, 9.9999999999999999),(248, 9.9999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_17_16_data_start_index = 241
    def test_cast_to_decimal32_8_8_from_decimal64_17_16_data_end_index = 249
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_17_16_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_17_16_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_16 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_17_17 values (249, 0.99999999999999999),(250, 0.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_17_17_data_start_index = 249
    def test_cast_to_decimal32_8_8_from_decimal64_17_17_data_end_index = 251
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_17_17_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_17_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_18_0 values (251, 1),(252, 999999999999999998),(253, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_18_0_data_start_index = 251
    def test_cast_to_decimal32_8_8_from_decimal64_18_0_data_end_index = 254
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_18_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_18_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_18_1 values (254, 1.9),(255, 99999999999999998.9),(256, 99999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_18_1_data_start_index = 254
    def test_cast_to_decimal32_8_8_from_decimal64_18_1_data_end_index = 257
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_18_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_18_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_18_9 values (257, 0.999999999),(258, 0.999999999),(259, 1.999999999),(260, 1.999999999),(261, 999999998.999999999),
      (262, 999999998.999999999),(263, 999999999.999999999),(264, 999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_18_9_data_start_index = 257
    def test_cast_to_decimal32_8_8_from_decimal64_18_9_data_end_index = 265
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_18_9_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_18_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_18_17 values (265, 0.99999999999999999),(266, 0.99999999999999999),(267, 1.99999999999999999),(268, 1.99999999999999999),(269, 8.99999999999999999),
      (270, 8.99999999999999999),(271, 9.99999999999999999),(272, 9.99999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_18_17_data_start_index = 265
    def test_cast_to_decimal32_8_8_from_decimal64_18_17_data_end_index = 273
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_18_17_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_18_17_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_17 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal64_18_18 values (273, 0.999999999999999999),(274, 0.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal64_18_18_data_start_index = 273
    def test_cast_to_decimal32_8_8_from_decimal64_18_18_data_end_index = 275
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal64_18_18_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal64_18_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal64_18_18 order by 1;'

}