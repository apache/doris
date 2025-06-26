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


suite("test_cast_to_decimal64_10_from_decimal128v3_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_19_0 values (0, 10000000000),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_19_0_data_start_index = 0
    def test_cast_to_decimal64_10_0_from_decimal128v3_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_19_1 values (3, 9999999999.9),(4, 9999999999.9),(5, 10000000000.9),(6, 10000000000.9),(7, 999999999999999998.9),
      (8, 999999999999999998.9),(9, 999999999999999999.9),(10, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_19_1_data_start_index = 3
    def test_cast_to_decimal64_10_0_from_decimal128v3_19_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_19_9 values (11, 9999999999.999999999),(12, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_19_9_data_start_index = 11
    def test_cast_to_decimal64_10_0_from_decimal128v3_19_9_data_end_index = 13
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_37_0 values (13, 10000000000),(14, 9999999999999999999999999999999999998),(15, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_37_0_data_start_index = 13
    def test_cast_to_decimal64_10_0_from_decimal128v3_37_0_data_end_index = 16
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_37_1 values (16, 9999999999.9),(17, 9999999999.9),(18, 10000000000.9),(19, 10000000000.9),(20, 999999999999999999999999999999999998.9),
      (21, 999999999999999999999999999999999998.9),(22, 999999999999999999999999999999999999.9),(23, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_37_1_data_start_index = 16
    def test_cast_to_decimal64_10_0_from_decimal128v3_37_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_37_18 values (24, 9999999999.999999999999999999),(25, 9999999999.999999999999999999),(26, 10000000000.999999999999999999),(27, 10000000000.999999999999999999),(28, 9999999999999999998.999999999999999999),
      (29, 9999999999999999998.999999999999999999),(30, 9999999999999999999.999999999999999999),(31, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_37_18_data_start_index = 24
    def test_cast_to_decimal64_10_0_from_decimal128v3_37_18_data_end_index = 32
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_38_0 values (32, 10000000000),(33, 99999999999999999999999999999999999998),(34, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_38_0_data_start_index = 32
    def test_cast_to_decimal64_10_0_from_decimal128v3_38_0_data_end_index = 35
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_38_1 values (35, 9999999999.9),(36, 9999999999.9),(37, 10000000000.9),(38, 10000000000.9),(39, 9999999999999999999999999999999999998.9),
      (40, 9999999999999999999999999999999999998.9),(41, 9999999999999999999999999999999999999.9),(42, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_38_1_data_start_index = 35
    def test_cast_to_decimal64_10_0_from_decimal128v3_38_1_data_end_index = 43
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal128v3_38_19 values (43, 9999999999.9999999999999999999),(44, 9999999999.9999999999999999999),(45, 10000000000.9999999999999999999),(46, 10000000000.9999999999999999999),(47, 9999999999999999998.9999999999999999999),
      (48, 9999999999999999998.9999999999999999999),(49, 9999999999999999999.9999999999999999999),(50, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_0_from_decimal128v3_38_19_data_start_index = 43
    def test_cast_to_decimal64_10_0_from_decimal128v3_38_19_data_end_index = 51
    for (int data_index = test_cast_to_decimal64_10_0_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_10_0_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_19_0 values (51, 1000000000),(52, 9999999999999999998),(53, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_19_0_data_start_index = 51
    def test_cast_to_decimal64_10_1_from_decimal128v3_19_0_data_end_index = 54
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_19_1 values (54, 1000000000.9),(55, 999999999999999998.9),(56, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_19_1_data_start_index = 54
    def test_cast_to_decimal64_10_1_from_decimal128v3_19_1_data_end_index = 57
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_19_9 values (57, 999999999.999999999),(58, 999999999.999999999),(59, 1000000000.999999999),(60, 1000000000.999999999),(61, 9999999998.999999999),
      (62, 9999999998.999999999),(63, 9999999999.999999999),(64, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_19_9_data_start_index = 57
    def test_cast_to_decimal64_10_1_from_decimal128v3_19_9_data_end_index = 65
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_37_0 values (65, 1000000000),(66, 9999999999999999999999999999999999998),(67, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_37_0_data_start_index = 65
    def test_cast_to_decimal64_10_1_from_decimal128v3_37_0_data_end_index = 68
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_37_1 values (68, 1000000000.9),(69, 999999999999999999999999999999999998.9),(70, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_37_1_data_start_index = 68
    def test_cast_to_decimal64_10_1_from_decimal128v3_37_1_data_end_index = 71
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_37_18 values (71, 999999999.999999999999999999),(72, 999999999.999999999999999999),(73, 1000000000.999999999999999999),(74, 1000000000.999999999999999999),(75, 9999999999999999998.999999999999999999),
      (76, 9999999999999999998.999999999999999999),(77, 9999999999999999999.999999999999999999),(78, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_37_18_data_start_index = 71
    def test_cast_to_decimal64_10_1_from_decimal128v3_37_18_data_end_index = 79
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_38_0 values (79, 1000000000),(80, 99999999999999999999999999999999999998),(81, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_38_0_data_start_index = 79
    def test_cast_to_decimal64_10_1_from_decimal128v3_38_0_data_end_index = 82
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_38_1 values (82, 1000000000.9),(83, 9999999999999999999999999999999999998.9),(84, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_38_1_data_start_index = 82
    def test_cast_to_decimal64_10_1_from_decimal128v3_38_1_data_end_index = 85
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_1_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_10_1_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_1_from_decimal128v3_38_19 values (85, 999999999.9999999999999999999),(86, 999999999.9999999999999999999),(87, 1000000000.9999999999999999999),(88, 1000000000.9999999999999999999),(89, 9999999999999999998.9999999999999999999),
      (90, 9999999999999999998.9999999999999999999),(91, 9999999999999999999.9999999999999999999),(92, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_1_from_decimal128v3_38_19_data_start_index = 85
    def test_cast_to_decimal64_10_1_from_decimal128v3_38_19_data_end_index = 93
    for (int data_index = test_cast_to_decimal64_10_1_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_10_1_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(10, 1)) from test_cast_to_decimal64_10_1_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_19_0 values (93, 100000),(94, 9999999999999999998),(95, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_19_0_data_start_index = 93
    def test_cast_to_decimal64_10_5_from_decimal128v3_19_0_data_end_index = 96
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_19_1 values (96, 100000.9),(97, 999999999999999998.9),(98, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_19_1_data_start_index = 96
    def test_cast_to_decimal64_10_5_from_decimal128v3_19_1_data_end_index = 99
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_19_9 values (99, 99999.999999999),(100, 99999.999999999),(101, 100000.999999999),(102, 100000.999999999),(103, 9999999998.999999999),
      (104, 9999999998.999999999),(105, 9999999999.999999999),(106, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_19_9_data_start_index = 99
    def test_cast_to_decimal64_10_5_from_decimal128v3_19_9_data_end_index = 107
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_37_0 values (107, 100000),(108, 9999999999999999999999999999999999998),(109, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_37_0_data_start_index = 107
    def test_cast_to_decimal64_10_5_from_decimal128v3_37_0_data_end_index = 110
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_37_1 values (110, 100000.9),(111, 999999999999999999999999999999999998.9),(112, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_37_1_data_start_index = 110
    def test_cast_to_decimal64_10_5_from_decimal128v3_37_1_data_end_index = 113
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_37_18 values (113, 99999.999999999999999999),(114, 99999.999999999999999999),(115, 100000.999999999999999999),(116, 100000.999999999999999999),(117, 9999999999999999998.999999999999999999),
      (118, 9999999999999999998.999999999999999999),(119, 9999999999999999999.999999999999999999),(120, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_37_18_data_start_index = 113
    def test_cast_to_decimal64_10_5_from_decimal128v3_37_18_data_end_index = 121
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_38_0 values (121, 100000),(122, 99999999999999999999999999999999999998),(123, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_38_0_data_start_index = 121
    def test_cast_to_decimal64_10_5_from_decimal128v3_38_0_data_end_index = 124
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_38_1 values (124, 100000.9),(125, 9999999999999999999999999999999999998.9),(126, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_38_1_data_start_index = 124
    def test_cast_to_decimal64_10_5_from_decimal128v3_38_1_data_end_index = 127
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_5_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_10_5_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_5_from_decimal128v3_38_19 values (127, 99999.9999999999999999999),(128, 99999.9999999999999999999),(129, 100000.9999999999999999999),(130, 100000.9999999999999999999),(131, 9999999999999999998.9999999999999999999),
      (132, 9999999999999999998.9999999999999999999),(133, 9999999999999999999.9999999999999999999),(134, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_5_from_decimal128v3_38_19_data_start_index = 127
    def test_cast_to_decimal64_10_5_from_decimal128v3_38_19_data_end_index = 135
    for (int data_index = test_cast_to_decimal64_10_5_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_10_5_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(10, 5)) from test_cast_to_decimal64_10_5_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_19_0 values (135, 10),(136, 9999999999999999998),(137, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_19_0_data_start_index = 135
    def test_cast_to_decimal64_10_9_from_decimal128v3_19_0_data_end_index = 138
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_19_1 values (138, 10.9),(139, 999999999999999998.9),(140, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_19_1_data_start_index = 138
    def test_cast_to_decimal64_10_9_from_decimal128v3_19_1_data_end_index = 141
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_19_9 values (141, 10.999999999),(142, 9999999998.999999999),(143, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_19_9_data_start_index = 141
    def test_cast_to_decimal64_10_9_from_decimal128v3_19_9_data_end_index = 144
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_19_18 values (144, 9.999999999999999999),(145, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_19_18_data_start_index = 144
    def test_cast_to_decimal64_10_9_from_decimal128v3_19_18_data_end_index = 146
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_37_0 values (146, 10),(147, 9999999999999999999999999999999999998),(148, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_37_0_data_start_index = 146
    def test_cast_to_decimal64_10_9_from_decimal128v3_37_0_data_end_index = 149
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_37_1 values (149, 10.9),(150, 999999999999999999999999999999999998.9),(151, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_37_1_data_start_index = 149
    def test_cast_to_decimal64_10_9_from_decimal128v3_37_1_data_end_index = 152
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_37_18 values (152, 9.999999999999999999),(153, 9.999999999999999999),(154, 10.999999999999999999),(155, 10.999999999999999999),(156, 9999999999999999998.999999999999999999),
      (157, 9999999999999999998.999999999999999999),(158, 9999999999999999999.999999999999999999),(159, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_37_18_data_start_index = 152
    def test_cast_to_decimal64_10_9_from_decimal128v3_37_18_data_end_index = 160
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_37_36 values (160, 9.999999999999999999999999999999999999),(161, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_37_36_data_start_index = 160
    def test_cast_to_decimal64_10_9_from_decimal128v3_37_36_data_end_index = 162
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_38_0 values (162, 10),(163, 99999999999999999999999999999999999998),(164, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_38_0_data_start_index = 162
    def test_cast_to_decimal64_10_9_from_decimal128v3_38_0_data_end_index = 165
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_38_1 values (165, 10.9),(166, 9999999999999999999999999999999999998.9),(167, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_38_1_data_start_index = 165
    def test_cast_to_decimal64_10_9_from_decimal128v3_38_1_data_end_index = 168
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_38_19 values (168, 9.9999999999999999999),(169, 9.9999999999999999999),(170, 10.9999999999999999999),(171, 10.9999999999999999999),(172, 9999999999999999998.9999999999999999999),
      (173, 9999999999999999998.9999999999999999999),(174, 9999999999999999999.9999999999999999999),(175, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_38_19_data_start_index = 168
    def test_cast_to_decimal64_10_9_from_decimal128v3_38_19_data_end_index = 176
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_9_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal64_10_9_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_9_from_decimal128v3_38_37 values (176, 9.9999999999999999999999999999999999999),(177, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_9_from_decimal128v3_38_37_data_start_index = 176
    def test_cast_to_decimal64_10_9_from_decimal128v3_38_37_data_end_index = 178
    for (int data_index = test_cast_to_decimal64_10_9_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal64_10_9_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(10, 9)) from test_cast_to_decimal64_10_9_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_19_0 values (178, 1),(179, 9999999999999999998),(180, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_19_0_data_start_index = 178
    def test_cast_to_decimal64_10_10_from_decimal128v3_19_0_data_end_index = 181
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_19_1 values (181, 1.9),(182, 999999999999999998.9),(183, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_19_1_data_start_index = 181
    def test_cast_to_decimal64_10_10_from_decimal128v3_19_1_data_end_index = 184
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_19_9 values (184, 1.999999999),(185, 9999999998.999999999),(186, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_19_9_data_start_index = 184
    def test_cast_to_decimal64_10_10_from_decimal128v3_19_9_data_end_index = 187
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_19_18 values (187, 0.999999999999999999),(188, 0.999999999999999999),(189, 1.999999999999999999),(190, 1.999999999999999999),(191, 8.999999999999999999),
      (192, 8.999999999999999999),(193, 9.999999999999999999),(194, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_19_18_data_start_index = 187
    def test_cast_to_decimal64_10_10_from_decimal128v3_19_18_data_end_index = 195
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_19_19 values (195, 0.9999999999999999999),(196, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_19_19_data_start_index = 195
    def test_cast_to_decimal64_10_10_from_decimal128v3_19_19_data_end_index = 197
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_19_19_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_37_0 values (197, 1),(198, 9999999999999999999999999999999999998),(199, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_37_0_data_start_index = 197
    def test_cast_to_decimal64_10_10_from_decimal128v3_37_0_data_end_index = 200
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_37_1 values (200, 1.9),(201, 999999999999999999999999999999999998.9),(202, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_37_1_data_start_index = 200
    def test_cast_to_decimal64_10_10_from_decimal128v3_37_1_data_end_index = 203
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_37_18 values (203, 0.999999999999999999),(204, 0.999999999999999999),(205, 1.999999999999999999),(206, 1.999999999999999999),(207, 9999999999999999998.999999999999999999),
      (208, 9999999999999999998.999999999999999999),(209, 9999999999999999999.999999999999999999),(210, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_37_18_data_start_index = 203
    def test_cast_to_decimal64_10_10_from_decimal128v3_37_18_data_end_index = 211
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_37_36 values (211, 0.999999999999999999999999999999999999),(212, 0.999999999999999999999999999999999999),(213, 1.999999999999999999999999999999999999),(214, 1.999999999999999999999999999999999999),(215, 8.999999999999999999999999999999999999),
      (216, 8.999999999999999999999999999999999999),(217, 9.999999999999999999999999999999999999),(218, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_37_36_data_start_index = 211
    def test_cast_to_decimal64_10_10_from_decimal128v3_37_36_data_end_index = 219
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_37_37 values (219, 0.9999999999999999999999999999999999999),(220, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_37_37_data_start_index = 219
    def test_cast_to_decimal64_10_10_from_decimal128v3_37_37_data_end_index = 221
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_37_37_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_38_0 values (221, 1),(222, 99999999999999999999999999999999999998),(223, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_38_0_data_start_index = 221
    def test_cast_to_decimal64_10_10_from_decimal128v3_38_0_data_end_index = 224
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_38_1 values (224, 1.9),(225, 9999999999999999999999999999999999998.9),(226, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_38_1_data_start_index = 224
    def test_cast_to_decimal64_10_10_from_decimal128v3_38_1_data_end_index = 227
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_38_19 values (227, 0.9999999999999999999),(228, 0.9999999999999999999),(229, 1.9999999999999999999),(230, 1.9999999999999999999),(231, 9999999999999999998.9999999999999999999),
      (232, 9999999999999999998.9999999999999999999),(233, 9999999999999999999.9999999999999999999),(234, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_38_19_data_start_index = 227
    def test_cast_to_decimal64_10_10_from_decimal128v3_38_19_data_end_index = 235
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_38_37 values (235, 0.9999999999999999999999999999999999999),(236, 0.9999999999999999999999999999999999999),(237, 1.9999999999999999999999999999999999999),(238, 1.9999999999999999999999999999999999999),(239, 8.9999999999999999999999999999999999999),
      (240, 8.9999999999999999999999999999999999999),(241, 9.9999999999999999999999999999999999999),(242, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_38_37_data_start_index = 235
    def test_cast_to_decimal64_10_10_from_decimal128v3_38_37_data_end_index = 243
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_10_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal64_10_10_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_10_from_decimal128v3_38_38 values (243, 0.99999999999999999999999999999999999999),(244, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_10_10_from_decimal128v3_38_38_data_start_index = 243
    def test_cast_to_decimal64_10_10_from_decimal128v3_38_38_data_end_index = 245
    for (int data_index = test_cast_to_decimal64_10_10_from_decimal128v3_38_38_data_start_index; data_index < test_cast_to_decimal64_10_10_from_decimal128v3_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(10, 10)) from test_cast_to_decimal64_10_10_from_decimal128v3_38_38 order by 1;'

}