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


suite("test_cast_to_decimal32_4_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_0 values (0, 10000),(1, 99999998),(2, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_8_0_data_start_index = 0
    def test_cast_to_decimal32_4_0_from_decimal32_8_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_1 values (3, 9999.9),(4, 9999.9),(5, 10000.9),(6, 10000.9),(7, 9999998.9),
      (8, 9999998.9),(9, 9999999.9),(10, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_8_1_data_start_index = 3
    def test_cast_to_decimal32_4_0_from_decimal32_8_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_4 values (11, 9999.9999),(12, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_8_4_data_start_index = 11
    def test_cast_to_decimal32_4_0_from_decimal32_8_4_data_end_index = 13
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_0 values (13, 10000),(14, 999999998),(15, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_9_0_data_start_index = 13
    def test_cast_to_decimal32_4_0_from_decimal32_9_0_data_end_index = 16
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_1 values (16, 9999.9),(17, 9999.9),(18, 10000.9),(19, 10000.9),(20, 99999998.9),
      (21, 99999998.9),(22, 99999999.9),(23, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_9_1_data_start_index = 16
    def test_cast_to_decimal32_4_0_from_decimal32_9_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_4 values (24, 9999.9999),(25, 9999.9999),(26, 10000.9999),(27, 10000.9999),(28, 99998.9999),
      (29, 99998.9999),(30, 99999.9999),(31, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_0_from_decimal32_9_4_data_start_index = 24
    def test_cast_to_decimal32_4_0_from_decimal32_9_4_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_4_0_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_4_0_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_0 values (32, 1000),(33, 9998),(34, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_4_0_data_start_index = 32
    def test_cast_to_decimal32_4_1_from_decimal32_4_0_data_end_index = 35
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_0 values (35, 1000),(36, 99999998),(37, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_8_0_data_start_index = 35
    def test_cast_to_decimal32_4_1_from_decimal32_8_0_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_1 values (38, 1000.9),(39, 9999998.9),(40, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_8_1_data_start_index = 38
    def test_cast_to_decimal32_4_1_from_decimal32_8_1_data_end_index = 41
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_4 values (41, 999.9999),(42, 999.9999),(43, 1000.9999),(44, 1000.9999),(45, 9998.9999),
      (46, 9998.9999),(47, 9999.9999),(48, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_8_4_data_start_index = 41
    def test_cast_to_decimal32_4_1_from_decimal32_8_4_data_end_index = 49
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_0 values (49, 1000),(50, 999999998),(51, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_9_0_data_start_index = 49
    def test_cast_to_decimal32_4_1_from_decimal32_9_0_data_end_index = 52
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_1 values (52, 1000.9),(53, 99999998.9),(54, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_9_1_data_start_index = 52
    def test_cast_to_decimal32_4_1_from_decimal32_9_1_data_end_index = 55
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_4 values (55, 999.9999),(56, 999.9999),(57, 1000.9999),(58, 1000.9999),(59, 99998.9999),
      (60, 99998.9999),(61, 99999.9999),(62, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_1_from_decimal32_9_4_data_start_index = 55
    def test_cast_to_decimal32_4_1_from_decimal32_9_4_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_4_1_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_4_1_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_0 values (63, 100),(64, 9998),(65, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_4_0_data_start_index = 63
    def test_cast_to_decimal32_4_2_from_decimal32_4_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_1 values (66, 100.9),(67, 998.9),(68, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_4_1_data_start_index = 66
    def test_cast_to_decimal32_4_2_from_decimal32_4_1_data_end_index = 69
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_0 values (69, 100),(70, 99999998),(71, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_8_0_data_start_index = 69
    def test_cast_to_decimal32_4_2_from_decimal32_8_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_1 values (72, 100.9),(73, 9999998.9),(74, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_8_1_data_start_index = 72
    def test_cast_to_decimal32_4_2_from_decimal32_8_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_4 values (75, 99.9999),(76, 99.9999),(77, 100.9999),(78, 100.9999),(79, 9998.9999),
      (80, 9998.9999),(81, 9999.9999),(82, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_8_4_data_start_index = 75
    def test_cast_to_decimal32_4_2_from_decimal32_8_4_data_end_index = 83
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_43 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_0 values (83, 100),(84, 999999998),(85, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_9_0_data_start_index = 83
    def test_cast_to_decimal32_4_2_from_decimal32_9_0_data_end_index = 86
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_1 values (86, 100.9),(87, 99999998.9),(88, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_9_1_data_start_index = 86
    def test_cast_to_decimal32_4_2_from_decimal32_9_1_data_end_index = 89
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_4 values (89, 99.9999),(90, 99.9999),(91, 100.9999),(92, 100.9999),(93, 99998.9999),
      (94, 99998.9999),(95, 99999.9999),(96, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_2_from_decimal32_9_4_data_start_index = 89
    def test_cast_to_decimal32_4_2_from_decimal32_9_4_data_end_index = 97
    for (int data_index = test_cast_to_decimal32_4_2_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_4_2_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_0 values (97, 10),(98, 9998),(99, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_4_0_data_start_index = 97
    def test_cast_to_decimal32_4_3_from_decimal32_4_0_data_end_index = 100
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_1 values (100, 10.9),(101, 998.9),(102, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_4_1_data_start_index = 100
    def test_cast_to_decimal32_4_3_from_decimal32_4_1_data_end_index = 103
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_54 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_2 values (103, 10.99),(104, 98.99),(105, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_4_2_data_start_index = 103
    def test_cast_to_decimal32_4_3_from_decimal32_4_2_data_end_index = 106
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_0 values (106, 10),(107, 99999998),(108, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_8_0_data_start_index = 106
    def test_cast_to_decimal32_4_3_from_decimal32_8_0_data_end_index = 109
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_1 values (109, 10.9),(110, 9999998.9),(111, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_8_1_data_start_index = 109
    def test_cast_to_decimal32_4_3_from_decimal32_8_1_data_end_index = 112
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_59 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_4 values (112, 9.9999),(113, 9.9999),(114, 10.9999),(115, 10.9999),(116, 9998.9999),
      (117, 9998.9999),(118, 9999.9999),(119, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_8_4_data_start_index = 112
    def test_cast_to_decimal32_4_3_from_decimal32_8_4_data_end_index = 120
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_7 values (120, 9.9999999),(121, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_8_7_data_start_index = 120
    def test_cast_to_decimal32_4_3_from_decimal32_8_7_data_end_index = 122
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_0 values (122, 10),(123, 999999998),(124, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_9_0_data_start_index = 122
    def test_cast_to_decimal32_4_3_from_decimal32_9_0_data_end_index = 125
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_1 values (125, 10.9),(126, 99999998.9),(127, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_9_1_data_start_index = 125
    def test_cast_to_decimal32_4_3_from_decimal32_9_1_data_end_index = 128
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_4 values (128, 9.9999),(129, 9.9999),(130, 10.9999),(131, 10.9999),(132, 99998.9999),
      (133, 99998.9999),(134, 99999.9999),(135, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_9_4_data_start_index = 128
    def test_cast_to_decimal32_4_3_from_decimal32_9_4_data_end_index = 136
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_8 values (136, 9.99999999),(137, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_3_from_decimal32_9_8_data_start_index = 136
    def test_cast_to_decimal32_4_3_from_decimal32_9_8_data_end_index = 138
    for (int data_index = test_cast_to_decimal32_4_3_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_4_3_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_1_0 values (138, 1),(139, 8),(140, 9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_1_0_data_start_index = 138
    def test_cast_to_decimal32_4_4_from_decimal32_1_0_data_end_index = 141
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_1_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_4_0 values (141, 1),(142, 9998),(143, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_4_0_data_start_index = 141
    def test_cast_to_decimal32_4_4_from_decimal32_4_0_data_end_index = 144
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_4_1 values (144, 1.9),(145, 998.9),(146, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_4_1_data_start_index = 144
    def test_cast_to_decimal32_4_4_from_decimal32_4_1_data_end_index = 147
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_4_2 values (147, 1.99),(148, 98.99),(149, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_4_2_data_start_index = 147
    def test_cast_to_decimal32_4_4_from_decimal32_4_2_data_end_index = 150
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_4_3 values (150, 1.999),(151, 8.999),(152, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_4_3_data_start_index = 150
    def test_cast_to_decimal32_4_4_from_decimal32_4_3_data_end_index = 153
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_8_0 values (153, 1),(154, 99999998),(155, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_8_0_data_start_index = 153
    def test_cast_to_decimal32_4_4_from_decimal32_8_0_data_end_index = 156
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_8_1 values (156, 1.9),(157, 9999998.9),(158, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_8_1_data_start_index = 156
    def test_cast_to_decimal32_4_4_from_decimal32_8_1_data_end_index = 159
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_8_4 values (159, 1.9999),(160, 9998.9999),(161, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_8_4_data_start_index = 159
    def test_cast_to_decimal32_4_4_from_decimal32_8_4_data_end_index = 162
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_8_7 values (162, 0.9999999),(163, 0.9999999),(164, 1.9999999),(165, 1.9999999),(166, 8.9999999),
      (167, 8.9999999),(168, 9.9999999),(169, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_8_7_data_start_index = 162
    def test_cast_to_decimal32_4_4_from_decimal32_8_7_data_end_index = 170
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_8_8 values (170, 0.99999999),(171, 0.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_8_8_data_start_index = 170
    def test_cast_to_decimal32_4_4_from_decimal32_8_8_data_end_index = 172
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_8_8_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_8_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_79 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_9_0 values (172, 1),(173, 999999998),(174, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_9_0_data_start_index = 172
    def test_cast_to_decimal32_4_4_from_decimal32_9_0_data_end_index = 175
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_9_1 values (175, 1.9),(176, 99999998.9),(177, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_9_1_data_start_index = 175
    def test_cast_to_decimal32_4_4_from_decimal32_9_1_data_end_index = 178
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_9_4 values (178, 1.9999),(179, 99998.9999),(180, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_9_4_data_start_index = 178
    def test_cast_to_decimal32_4_4_from_decimal32_9_4_data_end_index = 181
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_9_8 values (181, 0.99999999),(182, 0.99999999),(183, 1.99999999),(184, 1.99999999),(185, 8.99999999),
      (186, 8.99999999),(187, 9.99999999),(188, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_9_8_data_start_index = 181
    def test_cast_to_decimal32_4_4_from_decimal32_9_8_data_end_index = 189
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_4_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_4_4_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_4_from_decimal32_9_9 values (189, 0.999999999),(190, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_4_4_from_decimal32_9_9_data_start_index = 189
    def test_cast_to_decimal32_4_4_from_decimal32_9_9_data_end_index = 191
    for (int data_index = test_cast_to_decimal32_4_4_from_decimal32_9_9_data_start_index; data_index < test_cast_to_decimal32_4_4_from_decimal32_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(4, 4)) from test_cast_to_decimal32_4_4_from_decimal32_9_9 order by 1;'

}