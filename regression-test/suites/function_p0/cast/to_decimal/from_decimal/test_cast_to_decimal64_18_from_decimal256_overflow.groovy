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


suite("test_cast_to_decimal64_18_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_38_0 values (0, 1000000000000000000),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_38_0_data_start_index = 0
    def test_cast_to_decimal64_18_0_from_decimal256_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_38_1 values (3, 999999999999999999.9),(4, 999999999999999999.9),(5, 1000000000000000000.9),(6, 1000000000000000000.9),(7, 9999999999999999999999999999999999998.9),
      (8, 9999999999999999999999999999999999998.9),(9, 9999999999999999999999999999999999999.9),(10, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_38_1_data_start_index = 3
    def test_cast_to_decimal64_18_0_from_decimal256_38_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_38_19 values (11, 999999999999999999.9999999999999999999),(12, 999999999999999999.9999999999999999999),(13, 1000000000000000000.9999999999999999999),(14, 1000000000000000000.9999999999999999999),(15, 9999999999999999998.9999999999999999999),
      (16, 9999999999999999998.9999999999999999999),(17, 9999999999999999999.9999999999999999999),(18, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_38_19_data_start_index = 11
    def test_cast_to_decimal64_18_0_from_decimal256_38_19_data_end_index = 19
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_39_0 values (19, 1000000000000000000),(20, 999999999999999999999999999999999999998),(21, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_39_0_data_start_index = 19
    def test_cast_to_decimal64_18_0_from_decimal256_39_0_data_end_index = 22
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_39_1 values (22, 999999999999999999.9),(23, 999999999999999999.9),(24, 1000000000000000000.9),(25, 1000000000000000000.9),(26, 99999999999999999999999999999999999998.9),
      (27, 99999999999999999999999999999999999998.9),(28, 99999999999999999999999999999999999999.9),(29, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_39_1_data_start_index = 22
    def test_cast_to_decimal64_18_0_from_decimal256_39_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_39_19 values (30, 999999999999999999.9999999999999999999),(31, 999999999999999999.9999999999999999999),(32, 1000000000000000000.9999999999999999999),(33, 1000000000000000000.9999999999999999999),(34, 99999999999999999998.9999999999999999999),
      (35, 99999999999999999998.9999999999999999999),(36, 99999999999999999999.9999999999999999999),(37, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_39_19_data_start_index = 30
    def test_cast_to_decimal64_18_0_from_decimal256_39_19_data_end_index = 38
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_75_0 values (38, 1000000000000000000),(39, 999999999999999999999999999999999999999999999999999999999999999999999999998),(40, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_75_0_data_start_index = 38
    def test_cast_to_decimal64_18_0_from_decimal256_75_0_data_end_index = 41
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_75_1 values (41, 999999999999999999.9),(42, 999999999999999999.9),(43, 1000000000000000000.9),(44, 1000000000000000000.9),(45, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (46, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(47, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(48, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_75_1_data_start_index = 41
    def test_cast_to_decimal64_18_0_from_decimal256_75_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_75_37 values (49, 999999999999999999.9999999999999999999999999999999999999),(50, 999999999999999999.9999999999999999999999999999999999999),(51, 1000000000000000000.9999999999999999999999999999999999999),(52, 1000000000000000000.9999999999999999999999999999999999999),(53, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (54, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(55, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(56, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_75_37_data_start_index = 49
    def test_cast_to_decimal64_18_0_from_decimal256_75_37_data_end_index = 57
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_76_0 values (57, 1000000000000000000),(58, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(59, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_76_0_data_start_index = 57
    def test_cast_to_decimal64_18_0_from_decimal256_76_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_76_1 values (60, 999999999999999999.9),(61, 999999999999999999.9),(62, 1000000000000000000.9),(63, 1000000000000000000.9),(64, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (65, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(66, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(67, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_76_1_data_start_index = 60
    def test_cast_to_decimal64_18_0_from_decimal256_76_1_data_end_index = 68
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal256_76_38 values (68, 999999999999999999.99999999999999999999999999999999999999),(69, 999999999999999999.99999999999999999999999999999999999999),(70, 1000000000000000000.99999999999999999999999999999999999999),(71, 1000000000000000000.99999999999999999999999999999999999999),(72, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (73, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(74, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(75, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal256_76_38_data_start_index = 68
    def test_cast_to_decimal64_18_0_from_decimal256_76_38_data_end_index = 76
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_38_0 values (76, 100000000000000000),(77, 99999999999999999999999999999999999998),(78, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_38_0_data_start_index = 76
    def test_cast_to_decimal64_18_1_from_decimal256_38_0_data_end_index = 79
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_38_1 values (79, 100000000000000000.9),(80, 9999999999999999999999999999999999998.9),(81, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_38_1_data_start_index = 79
    def test_cast_to_decimal64_18_1_from_decimal256_38_1_data_end_index = 82
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_38_19 values (82, 99999999999999999.9999999999999999999),(83, 99999999999999999.9999999999999999999),(84, 100000000000000000.9999999999999999999),(85, 100000000000000000.9999999999999999999),(86, 9999999999999999998.9999999999999999999),
      (87, 9999999999999999998.9999999999999999999),(88, 9999999999999999999.9999999999999999999),(89, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_38_19_data_start_index = 82
    def test_cast_to_decimal64_18_1_from_decimal256_38_19_data_end_index = 90
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_39_0 values (90, 100000000000000000),(91, 999999999999999999999999999999999999998),(92, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_39_0_data_start_index = 90
    def test_cast_to_decimal64_18_1_from_decimal256_39_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_39_1 values (93, 100000000000000000.9),(94, 99999999999999999999999999999999999998.9),(95, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_39_1_data_start_index = 93
    def test_cast_to_decimal64_18_1_from_decimal256_39_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_39_19 values (96, 99999999999999999.9999999999999999999),(97, 99999999999999999.9999999999999999999),(98, 100000000000000000.9999999999999999999),(99, 100000000000000000.9999999999999999999),(100, 99999999999999999998.9999999999999999999),
      (101, 99999999999999999998.9999999999999999999),(102, 99999999999999999999.9999999999999999999),(103, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_39_19_data_start_index = 96
    def test_cast_to_decimal64_18_1_from_decimal256_39_19_data_end_index = 104
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_75_0 values (104, 100000000000000000),(105, 999999999999999999999999999999999999999999999999999999999999999999999999998),(106, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_75_0_data_start_index = 104
    def test_cast_to_decimal64_18_1_from_decimal256_75_0_data_end_index = 107
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_75_1 values (107, 100000000000000000.9),(108, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(109, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_75_1_data_start_index = 107
    def test_cast_to_decimal64_18_1_from_decimal256_75_1_data_end_index = 110
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_75_37 values (110, 99999999999999999.9999999999999999999999999999999999999),(111, 99999999999999999.9999999999999999999999999999999999999),(112, 100000000000000000.9999999999999999999999999999999999999),(113, 100000000000000000.9999999999999999999999999999999999999),(114, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (115, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(116, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(117, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_75_37_data_start_index = 110
    def test_cast_to_decimal64_18_1_from_decimal256_75_37_data_end_index = 118
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_76_0 values (118, 100000000000000000),(119, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(120, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_76_0_data_start_index = 118
    def test_cast_to_decimal64_18_1_from_decimal256_76_0_data_end_index = 121
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_76_1 values (121, 100000000000000000.9),(122, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(123, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_76_1_data_start_index = 121
    def test_cast_to_decimal64_18_1_from_decimal256_76_1_data_end_index = 124
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal256_76_38 values (124, 99999999999999999.99999999999999999999999999999999999999),(125, 99999999999999999.99999999999999999999999999999999999999),(126, 100000000000000000.99999999999999999999999999999999999999),(127, 100000000000000000.99999999999999999999999999999999999999),(128, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (129, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(130, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(131, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal256_76_38_data_start_index = 124
    def test_cast_to_decimal64_18_1_from_decimal256_76_38_data_end_index = 132
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_38_0 values (132, 1000000000),(133, 99999999999999999999999999999999999998),(134, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_38_0_data_start_index = 132
    def test_cast_to_decimal64_18_9_from_decimal256_38_0_data_end_index = 135
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_38_1 values (135, 1000000000.9),(136, 9999999999999999999999999999999999998.9),(137, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_38_1_data_start_index = 135
    def test_cast_to_decimal64_18_9_from_decimal256_38_1_data_end_index = 138
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_38_19 values (138, 999999999.9999999999999999999),(139, 999999999.9999999999999999999),(140, 1000000000.9999999999999999999),(141, 1000000000.9999999999999999999),(142, 9999999999999999998.9999999999999999999),
      (143, 9999999999999999998.9999999999999999999),(144, 9999999999999999999.9999999999999999999),(145, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_38_19_data_start_index = 138
    def test_cast_to_decimal64_18_9_from_decimal256_38_19_data_end_index = 146
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_39_0 values (146, 1000000000),(147, 999999999999999999999999999999999999998),(148, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_39_0_data_start_index = 146
    def test_cast_to_decimal64_18_9_from_decimal256_39_0_data_end_index = 149
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_39_1 values (149, 1000000000.9),(150, 99999999999999999999999999999999999998.9),(151, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_39_1_data_start_index = 149
    def test_cast_to_decimal64_18_9_from_decimal256_39_1_data_end_index = 152
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_39_19 values (152, 999999999.9999999999999999999),(153, 999999999.9999999999999999999),(154, 1000000000.9999999999999999999),(155, 1000000000.9999999999999999999),(156, 99999999999999999998.9999999999999999999),
      (157, 99999999999999999998.9999999999999999999),(158, 99999999999999999999.9999999999999999999),(159, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_39_19_data_start_index = 152
    def test_cast_to_decimal64_18_9_from_decimal256_39_19_data_end_index = 160
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_75_0 values (160, 1000000000),(161, 999999999999999999999999999999999999999999999999999999999999999999999999998),(162, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_75_0_data_start_index = 160
    def test_cast_to_decimal64_18_9_from_decimal256_75_0_data_end_index = 163
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_75_1 values (163, 1000000000.9),(164, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(165, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_75_1_data_start_index = 163
    def test_cast_to_decimal64_18_9_from_decimal256_75_1_data_end_index = 166
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_75_37 values (166, 999999999.9999999999999999999999999999999999999),(167, 999999999.9999999999999999999999999999999999999),(168, 1000000000.9999999999999999999999999999999999999),(169, 1000000000.9999999999999999999999999999999999999),(170, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (171, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(172, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(173, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_75_37_data_start_index = 166
    def test_cast_to_decimal64_18_9_from_decimal256_75_37_data_end_index = 174
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_76_0 values (174, 1000000000),(175, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(176, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_76_0_data_start_index = 174
    def test_cast_to_decimal64_18_9_from_decimal256_76_0_data_end_index = 177
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_76_1 values (177, 1000000000.9),(178, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(179, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_76_1_data_start_index = 177
    def test_cast_to_decimal64_18_9_from_decimal256_76_1_data_end_index = 180
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal256_76_38 values (180, 999999999.99999999999999999999999999999999999999),(181, 999999999.99999999999999999999999999999999999999),(182, 1000000000.99999999999999999999999999999999999999),(183, 1000000000.99999999999999999999999999999999999999),(184, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (185, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(186, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(187, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal256_76_38_data_start_index = 180
    def test_cast_to_decimal64_18_9_from_decimal256_76_38_data_end_index = 188
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_38_0 values (188, 10),(189, 99999999999999999999999999999999999998),(190, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_38_0_data_start_index = 188
    def test_cast_to_decimal64_18_17_from_decimal256_38_0_data_end_index = 191
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_38_1 values (191, 10.9),(192, 9999999999999999999999999999999999998.9),(193, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_38_1_data_start_index = 191
    def test_cast_to_decimal64_18_17_from_decimal256_38_1_data_end_index = 194
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_38_19 values (194, 9.9999999999999999999),(195, 9.9999999999999999999),(196, 10.9999999999999999999),(197, 10.9999999999999999999),(198, 9999999999999999998.9999999999999999999),
      (199, 9999999999999999998.9999999999999999999),(200, 9999999999999999999.9999999999999999999),(201, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_38_19_data_start_index = 194
    def test_cast_to_decimal64_18_17_from_decimal256_38_19_data_end_index = 202
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_38_37 values (202, 9.9999999999999999999999999999999999999),(203, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_38_37_data_start_index = 202
    def test_cast_to_decimal64_18_17_from_decimal256_38_37_data_end_index = 204
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_39_0 values (204, 10),(205, 999999999999999999999999999999999999998),(206, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_39_0_data_start_index = 204
    def test_cast_to_decimal64_18_17_from_decimal256_39_0_data_end_index = 207
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_39_1 values (207, 10.9),(208, 99999999999999999999999999999999999998.9),(209, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_39_1_data_start_index = 207
    def test_cast_to_decimal64_18_17_from_decimal256_39_1_data_end_index = 210
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_39_19 values (210, 9.9999999999999999999),(211, 9.9999999999999999999),(212, 10.9999999999999999999),(213, 10.9999999999999999999),(214, 99999999999999999998.9999999999999999999),
      (215, 99999999999999999998.9999999999999999999),(216, 99999999999999999999.9999999999999999999),(217, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_39_19_data_start_index = 210
    def test_cast_to_decimal64_18_17_from_decimal256_39_19_data_end_index = 218
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_39_38 values (218, 9.99999999999999999999999999999999999999),(219, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_39_38_data_start_index = 218
    def test_cast_to_decimal64_18_17_from_decimal256_39_38_data_end_index = 220
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_75_0 values (220, 10),(221, 999999999999999999999999999999999999999999999999999999999999999999999999998),(222, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_75_0_data_start_index = 220
    def test_cast_to_decimal64_18_17_from_decimal256_75_0_data_end_index = 223
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_75_1 values (223, 10.9),(224, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(225, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_75_1_data_start_index = 223
    def test_cast_to_decimal64_18_17_from_decimal256_75_1_data_end_index = 226
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_75_37 values (226, 9.9999999999999999999999999999999999999),(227, 9.9999999999999999999999999999999999999),(228, 10.9999999999999999999999999999999999999),(229, 10.9999999999999999999999999999999999999),(230, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (231, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(232, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(233, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_75_37_data_start_index = 226
    def test_cast_to_decimal64_18_17_from_decimal256_75_37_data_end_index = 234
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_75_74 values (234, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(235, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_75_74_data_start_index = 234
    def test_cast_to_decimal64_18_17_from_decimal256_75_74_data_end_index = 236
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_76_0 values (236, 10),(237, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(238, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_76_0_data_start_index = 236
    def test_cast_to_decimal64_18_17_from_decimal256_76_0_data_end_index = 239
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_76_1 values (239, 10.9),(240, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(241, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_76_1_data_start_index = 239
    def test_cast_to_decimal64_18_17_from_decimal256_76_1_data_end_index = 242
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_76_38 values (242, 9.99999999999999999999999999999999999999),(243, 9.99999999999999999999999999999999999999),(244, 10.99999999999999999999999999999999999999),(245, 10.99999999999999999999999999999999999999),(246, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (247, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(248, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(249, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_76_38_data_start_index = 242
    def test_cast_to_decimal64_18_17_from_decimal256_76_38_data_end_index = 250
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal256_76_75 values (250, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(251, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal256_76_75_data_start_index = 250
    def test_cast_to_decimal64_18_17_from_decimal256_76_75_data_end_index = 252
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_38_0 values (252, 1),(253, 99999999999999999999999999999999999998),(254, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_38_0_data_start_index = 252
    def test_cast_to_decimal64_18_18_from_decimal256_38_0_data_end_index = 255
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_38_1 values (255, 1.9),(256, 9999999999999999999999999999999999998.9),(257, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_38_1_data_start_index = 255
    def test_cast_to_decimal64_18_18_from_decimal256_38_1_data_end_index = 258
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_38_19 values (258, 0.9999999999999999999),(259, 0.9999999999999999999),(260, 1.9999999999999999999),(261, 1.9999999999999999999),(262, 9999999999999999998.9999999999999999999),
      (263, 9999999999999999998.9999999999999999999),(264, 9999999999999999999.9999999999999999999),(265, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_38_19_data_start_index = 258
    def test_cast_to_decimal64_18_18_from_decimal256_38_19_data_end_index = 266
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_38_37 values (266, 0.9999999999999999999999999999999999999),(267, 0.9999999999999999999999999999999999999),(268, 1.9999999999999999999999999999999999999),(269, 1.9999999999999999999999999999999999999),(270, 8.9999999999999999999999999999999999999),
      (271, 8.9999999999999999999999999999999999999),(272, 9.9999999999999999999999999999999999999),(273, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_38_37_data_start_index = 266
    def test_cast_to_decimal64_18_18_from_decimal256_38_37_data_end_index = 274
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_38_38;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_38_38 values (274, 0.99999999999999999999999999999999999999),(275, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_38_38_data_start_index = 274
    def test_cast_to_decimal64_18_18_from_decimal256_38_38_data_end_index = 276
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_38_38_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_38_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_39_0 values (276, 1),(277, 999999999999999999999999999999999999998),(278, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_39_0_data_start_index = 276
    def test_cast_to_decimal64_18_18_from_decimal256_39_0_data_end_index = 279
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_39_1 values (279, 1.9),(280, 99999999999999999999999999999999999998.9),(281, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_39_1_data_start_index = 279
    def test_cast_to_decimal64_18_18_from_decimal256_39_1_data_end_index = 282
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_39_19 values (282, 0.9999999999999999999),(283, 0.9999999999999999999),(284, 1.9999999999999999999),(285, 1.9999999999999999999),(286, 99999999999999999998.9999999999999999999),
      (287, 99999999999999999998.9999999999999999999),(288, 99999999999999999999.9999999999999999999),(289, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_39_19_data_start_index = 282
    def test_cast_to_decimal64_18_18_from_decimal256_39_19_data_end_index = 290
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_39_38 values (290, 0.99999999999999999999999999999999999999),(291, 0.99999999999999999999999999999999999999),(292, 1.99999999999999999999999999999999999999),(293, 1.99999999999999999999999999999999999999),(294, 8.99999999999999999999999999999999999999),
      (295, 8.99999999999999999999999999999999999999),(296, 9.99999999999999999999999999999999999999),(297, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_39_38_data_start_index = 290
    def test_cast_to_decimal64_18_18_from_decimal256_39_38_data_end_index = 298
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_39_39 values (298, 0.999999999999999999999999999999999999999),(299, 0.999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_39_39_data_start_index = 298
    def test_cast_to_decimal64_18_18_from_decimal256_39_39_data_end_index = 300
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_39_39_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_39_39_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_39 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_75_0 values (300, 1),(301, 999999999999999999999999999999999999999999999999999999999999999999999999998),(302, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_75_0_data_start_index = 300
    def test_cast_to_decimal64_18_18_from_decimal256_75_0_data_end_index = 303
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_75_1 values (303, 1.9),(304, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(305, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_75_1_data_start_index = 303
    def test_cast_to_decimal64_18_18_from_decimal256_75_1_data_end_index = 306
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_75_37 values (306, 0.9999999999999999999999999999999999999),(307, 0.9999999999999999999999999999999999999),(308, 1.9999999999999999999999999999999999999),(309, 1.9999999999999999999999999999999999999),(310, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (311, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(312, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(313, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_75_37_data_start_index = 306
    def test_cast_to_decimal64_18_18_from_decimal256_75_37_data_end_index = 314
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_75_74 values (314, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(315, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(316, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(317, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(318, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (319, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(320, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(321, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_75_74_data_start_index = 314
    def test_cast_to_decimal64_18_18_from_decimal256_75_74_data_end_index = 322
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_75_75 values (322, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(323, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_75_75_data_start_index = 322
    def test_cast_to_decimal64_18_18_from_decimal256_75_75_data_end_index = 324
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_76_0 values (324, 1),(325, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(326, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_76_0_data_start_index = 324
    def test_cast_to_decimal64_18_18_from_decimal256_76_0_data_end_index = 327
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_76_1 values (327, 1.9),(328, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(329, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_76_1_data_start_index = 327
    def test_cast_to_decimal64_18_18_from_decimal256_76_1_data_end_index = 330
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_76_38 values (330, 0.99999999999999999999999999999999999999),(331, 0.99999999999999999999999999999999999999),(332, 1.99999999999999999999999999999999999999),(333, 1.99999999999999999999999999999999999999),(334, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (335, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(336, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(337, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_76_38_data_start_index = 330
    def test_cast_to_decimal64_18_18_from_decimal256_76_38_data_end_index = 338
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_76_75 values (338, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(339, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(340, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(341, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(342, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (343, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(344, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(345, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_76_75_data_start_index = 338
    def test_cast_to_decimal64_18_18_from_decimal256_76_75_data_end_index = 346
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal256_76_76 values (346, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(347, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal256_76_76_data_start_index = 346
    def test_cast_to_decimal64_18_18_from_decimal256_76_76_data_end_index = 348
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal256_76_76 order by 1;'

}