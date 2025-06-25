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


suite("test_cast_to_decimal64_17_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_19_0 values (0, 100000000000000000),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_19_0_data_start_index = 0
    def test_cast_to_decimal64_17_0_from_decimal128i_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_19_1 values (3, 99999999999999999.9),(4, 99999999999999999.9),(5, 100000000000000000.9),(6, 100000000000000000.9),(7, 999999999999999998.9),
      (8, 999999999999999998.9),(9, 999999999999999999.9),(10, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_19_1_data_start_index = 3
    def test_cast_to_decimal64_17_0_from_decimal128i_19_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_37_0 values (11, 100000000000000000),(12, 9999999999999999999999999999999999998),(13, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_37_0_data_start_index = 11
    def test_cast_to_decimal64_17_0_from_decimal128i_37_0_data_end_index = 14
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_37_1 values (14, 99999999999999999.9),(15, 99999999999999999.9),(16, 100000000000000000.9),(17, 100000000000000000.9),(18, 999999999999999999999999999999999998.9),
      (19, 999999999999999999999999999999999998.9),(20, 999999999999999999999999999999999999.9),(21, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_37_1_data_start_index = 14
    def test_cast_to_decimal64_17_0_from_decimal128i_37_1_data_end_index = 22
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_37_18 values (22, 99999999999999999.999999999999999999),(23, 99999999999999999.999999999999999999),(24, 100000000000000000.999999999999999999),(25, 100000000000000000.999999999999999999),(26, 9999999999999999998.999999999999999999),
      (27, 9999999999999999998.999999999999999999),(28, 9999999999999999999.999999999999999999),(29, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_37_18_data_start_index = 22
    def test_cast_to_decimal64_17_0_from_decimal128i_37_18_data_end_index = 30
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_38_0 values (30, 100000000000000000),(31, 99999999999999999999999999999999999998),(32, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_38_0_data_start_index = 30
    def test_cast_to_decimal64_17_0_from_decimal128i_38_0_data_end_index = 33
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_38_1 values (33, 99999999999999999.9),(34, 99999999999999999.9),(35, 100000000000000000.9),(36, 100000000000000000.9),(37, 9999999999999999999999999999999999998.9),
      (38, 9999999999999999999999999999999999998.9),(39, 9999999999999999999999999999999999999.9),(40, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_38_1_data_start_index = 33
    def test_cast_to_decimal64_17_0_from_decimal128i_38_1_data_end_index = 41
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128i_38_19 values (41, 99999999999999999.9999999999999999999),(42, 99999999999999999.9999999999999999999),(43, 100000000000000000.9999999999999999999),(44, 100000000000000000.9999999999999999999),(45, 9999999999999999998.9999999999999999999),
      (46, 9999999999999999998.9999999999999999999),(47, 9999999999999999999.9999999999999999999),(48, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_0_from_decimal128i_38_19_data_start_index = 41
    def test_cast_to_decimal64_17_0_from_decimal128i_38_19_data_end_index = 49
    for (int data_index = test_cast_to_decimal64_17_0_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal64_17_0_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_19_0 values (49, 10000000000000000),(50, 9999999999999999998),(51, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_19_0_data_start_index = 49
    def test_cast_to_decimal64_17_1_from_decimal128i_19_0_data_end_index = 52
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_19_1 values (52, 10000000000000000.9),(53, 999999999999999998.9),(54, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_19_1_data_start_index = 52
    def test_cast_to_decimal64_17_1_from_decimal128i_19_1_data_end_index = 55
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_37_0 values (55, 10000000000000000),(56, 9999999999999999999999999999999999998),(57, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_37_0_data_start_index = 55
    def test_cast_to_decimal64_17_1_from_decimal128i_37_0_data_end_index = 58
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_37_1 values (58, 10000000000000000.9),(59, 999999999999999999999999999999999998.9),(60, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_37_1_data_start_index = 58
    def test_cast_to_decimal64_17_1_from_decimal128i_37_1_data_end_index = 61
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_37_18 values (61, 9999999999999999.999999999999999999),(62, 9999999999999999.999999999999999999),(63, 10000000000000000.999999999999999999),(64, 10000000000000000.999999999999999999),(65, 9999999999999999998.999999999999999999),
      (66, 9999999999999999998.999999999999999999),(67, 9999999999999999999.999999999999999999),(68, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_37_18_data_start_index = 61
    def test_cast_to_decimal64_17_1_from_decimal128i_37_18_data_end_index = 69
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_38_0 values (69, 10000000000000000),(70, 99999999999999999999999999999999999998),(71, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_38_0_data_start_index = 69
    def test_cast_to_decimal64_17_1_from_decimal128i_38_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_38_1 values (72, 10000000000000000.9),(73, 9999999999999999999999999999999999998.9),(74, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_38_1_data_start_index = 72
    def test_cast_to_decimal64_17_1_from_decimal128i_38_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_1_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_17_1_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_1_from_decimal128i_38_19 values (75, 9999999999999999.9999999999999999999),(76, 9999999999999999.9999999999999999999),(77, 10000000000000000.9999999999999999999),(78, 10000000000000000.9999999999999999999),(79, 9999999999999999998.9999999999999999999),
      (80, 9999999999999999998.9999999999999999999),(81, 9999999999999999999.9999999999999999999),(82, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_1_from_decimal128i_38_19_data_start_index = 75
    def test_cast_to_decimal64_17_1_from_decimal128i_38_19_data_end_index = 83
    for (int data_index = test_cast_to_decimal64_17_1_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal64_17_1_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(17, 1)) from test_cast_to_decimal64_17_1_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_19_0 values (83, 1000000000),(84, 9999999999999999998),(85, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_19_0_data_start_index = 83
    def test_cast_to_decimal64_17_8_from_decimal128i_19_0_data_end_index = 86
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_19_1 values (86, 1000000000.9),(87, 999999999999999998.9),(88, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_19_1_data_start_index = 86
    def test_cast_to_decimal64_17_8_from_decimal128i_19_1_data_end_index = 89
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_19_9 values (89, 999999999.999999999),(90, 999999999.999999999),(91, 1000000000.999999999),(92, 1000000000.999999999),(93, 9999999998.999999999),
      (94, 9999999998.999999999),(95, 9999999999.999999999),(96, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_19_9_data_start_index = 89
    def test_cast_to_decimal64_17_8_from_decimal128i_19_9_data_end_index = 97
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_37_0 values (97, 1000000000),(98, 9999999999999999999999999999999999998),(99, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_37_0_data_start_index = 97
    def test_cast_to_decimal64_17_8_from_decimal128i_37_0_data_end_index = 100
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_37_1 values (100, 1000000000.9),(101, 999999999999999999999999999999999998.9),(102, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_37_1_data_start_index = 100
    def test_cast_to_decimal64_17_8_from_decimal128i_37_1_data_end_index = 103
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_37_18 values (103, 999999999.999999999999999999),(104, 999999999.999999999999999999),(105, 1000000000.999999999999999999),(106, 1000000000.999999999999999999),(107, 9999999999999999998.999999999999999999),
      (108, 9999999999999999998.999999999999999999),(109, 9999999999999999999.999999999999999999),(110, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_37_18_data_start_index = 103
    def test_cast_to_decimal64_17_8_from_decimal128i_37_18_data_end_index = 111
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_38_0 values (111, 1000000000),(112, 99999999999999999999999999999999999998),(113, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_38_0_data_start_index = 111
    def test_cast_to_decimal64_17_8_from_decimal128i_38_0_data_end_index = 114
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_38_1 values (114, 1000000000.9),(115, 9999999999999999999999999999999999998.9),(116, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_38_1_data_start_index = 114
    def test_cast_to_decimal64_17_8_from_decimal128i_38_1_data_end_index = 117
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_8_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_17_8_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_8_from_decimal128i_38_19 values (117, 999999999.9999999999999999999),(118, 999999999.9999999999999999999),(119, 1000000000.9999999999999999999),(120, 1000000000.9999999999999999999),(121, 9999999999999999998.9999999999999999999),
      (122, 9999999999999999998.9999999999999999999),(123, 9999999999999999999.9999999999999999999),(124, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_8_from_decimal128i_38_19_data_start_index = 117
    def test_cast_to_decimal64_17_8_from_decimal128i_38_19_data_end_index = 125
    for (int data_index = test_cast_to_decimal64_17_8_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal64_17_8_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(17, 8)) from test_cast_to_decimal64_17_8_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_19_0 values (125, 10),(126, 9999999999999999998),(127, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_19_0_data_start_index = 125
    def test_cast_to_decimal64_17_16_from_decimal128i_19_0_data_end_index = 128
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_19_1 values (128, 10.9),(129, 999999999999999998.9),(130, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_19_1_data_start_index = 128
    def test_cast_to_decimal64_17_16_from_decimal128i_19_1_data_end_index = 131
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_19_9 values (131, 10.999999999),(132, 9999999998.999999999),(133, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_19_9_data_start_index = 131
    def test_cast_to_decimal64_17_16_from_decimal128i_19_9_data_end_index = 134
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_19_18 values (134, 9.999999999999999999),(135, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_19_18_data_start_index = 134
    def test_cast_to_decimal64_17_16_from_decimal128i_19_18_data_end_index = 136
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_37_0 values (136, 10),(137, 9999999999999999999999999999999999998),(138, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_37_0_data_start_index = 136
    def test_cast_to_decimal64_17_16_from_decimal128i_37_0_data_end_index = 139
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_37_1 values (139, 10.9),(140, 999999999999999999999999999999999998.9),(141, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_37_1_data_start_index = 139
    def test_cast_to_decimal64_17_16_from_decimal128i_37_1_data_end_index = 142
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_37_18 values (142, 9.999999999999999999),(143, 9.999999999999999999),(144, 10.999999999999999999),(145, 10.999999999999999999),(146, 9999999999999999998.999999999999999999),
      (147, 9999999999999999998.999999999999999999),(148, 9999999999999999999.999999999999999999),(149, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_37_18_data_start_index = 142
    def test_cast_to_decimal64_17_16_from_decimal128i_37_18_data_end_index = 150
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_37_36 values (150, 9.999999999999999999999999999999999999),(151, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_37_36_data_start_index = 150
    def test_cast_to_decimal64_17_16_from_decimal128i_37_36_data_end_index = 152
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_38_0 values (152, 10),(153, 99999999999999999999999999999999999998),(154, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_38_0_data_start_index = 152
    def test_cast_to_decimal64_17_16_from_decimal128i_38_0_data_end_index = 155
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_38_1 values (155, 10.9),(156, 9999999999999999999999999999999999998.9),(157, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_38_1_data_start_index = 155
    def test_cast_to_decimal64_17_16_from_decimal128i_38_1_data_end_index = 158
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_38_19 values (158, 9.9999999999999999999),(159, 9.9999999999999999999),(160, 10.9999999999999999999),(161, 10.9999999999999999999),(162, 9999999999999999998.9999999999999999999),
      (163, 9999999999999999998.9999999999999999999),(164, 9999999999999999999.9999999999999999999),(165, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_38_19_data_start_index = 158
    def test_cast_to_decimal64_17_16_from_decimal128i_38_19_data_end_index = 166
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_16_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal64_17_16_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_16_from_decimal128i_38_37 values (166, 9.9999999999999999999999999999999999999),(167, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_16_from_decimal128i_38_37_data_start_index = 166
    def test_cast_to_decimal64_17_16_from_decimal128i_38_37_data_end_index = 168
    for (int data_index = test_cast_to_decimal64_17_16_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal64_17_16_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(17, 16)) from test_cast_to_decimal64_17_16_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_19_0 values (168, 1),(169, 9999999999999999998),(170, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_19_0_data_start_index = 168
    def test_cast_to_decimal64_17_17_from_decimal128i_19_0_data_end_index = 171
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_19_1 values (171, 1.9),(172, 999999999999999998.9),(173, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_19_1_data_start_index = 171
    def test_cast_to_decimal64_17_17_from_decimal128i_19_1_data_end_index = 174
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_19_9 values (174, 1.999999999),(175, 9999999998.999999999),(176, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_19_9_data_start_index = 174
    def test_cast_to_decimal64_17_17_from_decimal128i_19_9_data_end_index = 177
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_19_18 values (177, 0.999999999999999999),(178, 0.999999999999999999),(179, 1.999999999999999999),(180, 1.999999999999999999),(181, 8.999999999999999999),
      (182, 8.999999999999999999),(183, 9.999999999999999999),(184, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_19_18_data_start_index = 177
    def test_cast_to_decimal64_17_17_from_decimal128i_19_18_data_end_index = 185
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_19_19 values (185, 0.9999999999999999999),(186, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_19_19_data_start_index = 185
    def test_cast_to_decimal64_17_17_from_decimal128i_19_19_data_end_index = 187
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_19_19_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_37_0 values (187, 1),(188, 9999999999999999999999999999999999998),(189, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_37_0_data_start_index = 187
    def test_cast_to_decimal64_17_17_from_decimal128i_37_0_data_end_index = 190
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_37_1 values (190, 1.9),(191, 999999999999999999999999999999999998.9),(192, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_37_1_data_start_index = 190
    def test_cast_to_decimal64_17_17_from_decimal128i_37_1_data_end_index = 193
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_37_18 values (193, 0.999999999999999999),(194, 0.999999999999999999),(195, 1.999999999999999999),(196, 1.999999999999999999),(197, 9999999999999999998.999999999999999999),
      (198, 9999999999999999998.999999999999999999),(199, 9999999999999999999.999999999999999999),(200, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_37_18_data_start_index = 193
    def test_cast_to_decimal64_17_17_from_decimal128i_37_18_data_end_index = 201
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_37_36 values (201, 0.999999999999999999999999999999999999),(202, 0.999999999999999999999999999999999999),(203, 1.999999999999999999999999999999999999),(204, 1.999999999999999999999999999999999999),(205, 8.999999999999999999999999999999999999),
      (206, 8.999999999999999999999999999999999999),(207, 9.999999999999999999999999999999999999),(208, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_37_36_data_start_index = 201
    def test_cast_to_decimal64_17_17_from_decimal128i_37_36_data_end_index = 209
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_37_37 values (209, 0.9999999999999999999999999999999999999),(210, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_37_37_data_start_index = 209
    def test_cast_to_decimal64_17_17_from_decimal128i_37_37_data_end_index = 211
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_37_37_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_38_0 values (211, 1),(212, 99999999999999999999999999999999999998),(213, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_38_0_data_start_index = 211
    def test_cast_to_decimal64_17_17_from_decimal128i_38_0_data_end_index = 214
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_38_1 values (214, 1.9),(215, 9999999999999999999999999999999999998.9),(216, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_38_1_data_start_index = 214
    def test_cast_to_decimal64_17_17_from_decimal128i_38_1_data_end_index = 217
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_38_19 values (217, 0.9999999999999999999),(218, 0.9999999999999999999),(219, 1.9999999999999999999),(220, 1.9999999999999999999),(221, 9999999999999999998.9999999999999999999),
      (222, 9999999999999999998.9999999999999999999),(223, 9999999999999999999.9999999999999999999),(224, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_38_19_data_start_index = 217
    def test_cast_to_decimal64_17_17_from_decimal128i_38_19_data_end_index = 225
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_38_37 values (225, 0.9999999999999999999999999999999999999),(226, 0.9999999999999999999999999999999999999),(227, 1.9999999999999999999999999999999999999),(228, 1.9999999999999999999999999999999999999),(229, 8.9999999999999999999999999999999999999),
      (230, 8.9999999999999999999999999999999999999),(231, 9.9999999999999999999999999999999999999),(232, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_38_37_data_start_index = 225
    def test_cast_to_decimal64_17_17_from_decimal128i_38_37_data_end_index = 233
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_17_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal64_17_17_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_17_from_decimal128i_38_38 values (233, 0.99999999999999999999999999999999999999),(234, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_17_17_from_decimal128i_38_38_data_start_index = 233
    def test_cast_to_decimal64_17_17_from_decimal128i_38_38_data_end_index = 235
    for (int data_index = test_cast_to_decimal64_17_17_from_decimal128i_38_38_data_start_index; data_index < test_cast_to_decimal64_17_17_from_decimal128i_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(17, 17)) from test_cast_to_decimal64_17_17_from_decimal128i_38_38 order by 1;'

}