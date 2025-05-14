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


suite("test_cast_to_decimal32_1_from_decimal32_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_0 values (0, 10),(1, 9998),(2, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_4_0_data_start_index = 0
    def test_cast_to_decimal32_1_0_from_decimal32_4_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_1 values (3, 9.9),(4, 9.9),(5, 10.9),(6, 10.9),(7, 998.9),
      (8, 998.9),(9, 999.9),(10, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_4_1_data_start_index = 3
    def test_cast_to_decimal32_1_0_from_decimal32_4_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_2 values (11, 9.99),(12, 9.99),(13, 10.99),(14, 10.99),(15, 98.99),
      (16, 98.99),(17, 99.99),(18, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_4_2_data_start_index = 11
    def test_cast_to_decimal32_1_0_from_decimal32_4_2_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_4 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_3 values (19, 9.999),(20, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_4_3_data_start_index = 19
    def test_cast_to_decimal32_1_0_from_decimal32_4_3_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_0 values (21, 10),(22, 99999998),(23, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_8_0_data_start_index = 21
    def test_cast_to_decimal32_1_0_from_decimal32_8_0_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_1 values (24, 9.9),(25, 9.9),(26, 10.9),(27, 10.9),(28, 9999998.9),
      (29, 9999998.9),(30, 9999999.9),(31, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_8_1_data_start_index = 24
    def test_cast_to_decimal32_1_0_from_decimal32_8_1_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_4 values (32, 9.9999),(33, 9.9999),(34, 10.9999),(35, 10.9999),(36, 9998.9999),
      (37, 9998.9999),(38, 9999.9999),(39, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_8_4_data_start_index = 32
    def test_cast_to_decimal32_1_0_from_decimal32_8_4_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_9 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_7 values (40, 9.9999999),(41, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_8_7_data_start_index = 40
    def test_cast_to_decimal32_1_0_from_decimal32_8_7_data_end_index = 42
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_0 values (42, 10),(43, 999999998),(44, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_9_0_data_start_index = 42
    def test_cast_to_decimal32_1_0_from_decimal32_9_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_1 values (45, 9.9),(46, 9.9),(47, 10.9),(48, 10.9),(49, 99999998.9),
      (50, 99999998.9),(51, 99999999.9),(52, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_9_1_data_start_index = 45
    def test_cast_to_decimal32_1_0_from_decimal32_9_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_4 values (53, 9.9999),(54, 9.9999),(55, 10.9999),(56, 10.9999),(57, 99998.9999),
      (58, 99998.9999),(59, 99999.9999),(60, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_9_4_data_start_index = 53
    def test_cast_to_decimal32_1_0_from_decimal32_9_4_data_end_index = 61
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_14 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_8 values (61, 9.99999999),(62, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal32_9_8_data_start_index = 61
    def test_cast_to_decimal32_1_0_from_decimal32_9_8_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_1_0 values (63, 1),(64, 8),(65, 9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_1_0_data_start_index = 63
    def test_cast_to_decimal32_1_1_from_decimal32_1_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_1_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_1_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_0 values (66, 1),(67, 9998),(68, 9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_4_0_data_start_index = 66
    def test_cast_to_decimal32_1_1_from_decimal32_4_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_4_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_4_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_1 values (69, 1.9),(70, 998.9),(71, 999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_4_1_data_start_index = 69
    def test_cast_to_decimal32_1_1_from_decimal32_4_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_4_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_4_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_2 values (72, 0.99),(73, 0.99),(74, 1.99),(75, 1.99),(76, 98.99),
      (77, 98.99),(78, 99.99),(79, 99.99);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_4_2_data_start_index = 72
    def test_cast_to_decimal32_1_1_from_decimal32_4_2_data_end_index = 80
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_4_2_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_4_2_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_2 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_3 values (80, 0.999),(81, 0.999),(82, 1.999),(83, 1.999),(84, 8.999),
      (85, 8.999),(86, 9.999),(87, 9.999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_4_3_data_start_index = 80
    def test_cast_to_decimal32_1_1_from_decimal32_4_3_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_4_3_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_4_3_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_3 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_4 values (88, 0.9999),(89, 0.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_4_4_data_start_index = 88
    def test_cast_to_decimal32_1_1_from_decimal32_4_4_data_end_index = 90
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_4_4_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_4_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_0 values (90, 1),(91, 99999998),(92, 99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_8_0_data_start_index = 90
    def test_cast_to_decimal32_1_1_from_decimal32_8_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_8_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_8_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_1 values (93, 1.9),(94, 9999998.9),(95, 9999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_8_1_data_start_index = 93
    def test_cast_to_decimal32_1_1_from_decimal32_8_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_8_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_8_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_4 values (96, 0.9999),(97, 0.9999),(98, 1.9999),(99, 1.9999),(100, 9998.9999),
      (101, 9998.9999),(102, 9999.9999),(103, 9999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_8_4_data_start_index = 96
    def test_cast_to_decimal32_1_1_from_decimal32_8_4_data_end_index = 104
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_8_4_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_8_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_7 values (104, 0.9999999),(105, 0.9999999),(106, 1.9999999),(107, 1.9999999),(108, 8.9999999),
      (109, 8.9999999),(110, 9.9999999),(111, 9.9999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_8_7_data_start_index = 104
    def test_cast_to_decimal32_1_1_from_decimal32_8_7_data_end_index = 112
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_8_7_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_8_7_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_7 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_8 values (112, 0.99999999),(113, 0.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_8_8_data_start_index = 112
    def test_cast_to_decimal32_1_1_from_decimal32_8_8_data_end_index = 114
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_8_8_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_8_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_0 values (114, 1),(115, 999999998),(116, 999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_9_0_data_start_index = 114
    def test_cast_to_decimal32_1_1_from_decimal32_9_0_data_end_index = 117
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_9_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_9_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_1 values (117, 1.9),(118, 99999998.9),(119, 99999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_9_1_data_start_index = 117
    def test_cast_to_decimal32_1_1_from_decimal32_9_1_data_end_index = 120
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_9_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_9_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_4 values (120, 0.9999),(121, 0.9999),(122, 1.9999),(123, 1.9999),(124, 99998.9999),
      (125, 99998.9999),(126, 99999.9999),(127, 99999.9999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_9_4_data_start_index = 120
    def test_cast_to_decimal32_1_1_from_decimal32_9_4_data_end_index = 128
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_9_4_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_9_4_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_4 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_8 values (128, 0.99999999),(129, 0.99999999),(130, 1.99999999),(131, 1.99999999),(132, 8.99999999),
      (133, 8.99999999),(134, 9.99999999),(135, 9.99999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_9_8_data_start_index = 128
    def test_cast_to_decimal32_1_1_from_decimal32_9_8_data_end_index = 136
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_9_8_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_9_8_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_8 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_9 values (136, 0.999999999),(137, 0.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal32_9_9_data_start_index = 136
    def test_cast_to_decimal32_1_1_from_decimal32_9_9_data_end_index = 138
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal32_9_9_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal32_9_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_9 order by 1;'

}