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


suite("test_cast_to_decimal32_9_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_19_0 values (0, 1000000000),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_19_0_data_start_index = 0
    def test_cast_to_decimal32_9_0_from_decimal128i_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_19_1 values (3, 999999999.9),(4, 999999999.9),(5, 1000000000.9),(6, 1000000000.9),(7, 999999999999999998.9),
      (8, 999999999999999998.9),(9, 999999999999999999.9),(10, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_19_1_data_start_index = 3
    def test_cast_to_decimal32_9_0_from_decimal128i_19_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_19_9 values (11, 999999999.999999999),(12, 999999999.999999999),(13, 1000000000.999999999),(14, 1000000000.999999999),(15, 9999999998.999999999),
      (16, 9999999998.999999999),(17, 9999999999.999999999),(18, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_19_9_data_start_index = 11
    def test_cast_to_decimal32_9_0_from_decimal128i_19_9_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_37_0 values (19, 1000000000),(20, 9999999999999999999999999999999999998),(21, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_37_0_data_start_index = 19
    def test_cast_to_decimal32_9_0_from_decimal128i_37_0_data_end_index = 22
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_37_1 values (22, 999999999.9),(23, 999999999.9),(24, 1000000000.9),(25, 1000000000.9),(26, 999999999999999999999999999999999998.9),
      (27, 999999999999999999999999999999999998.9),(28, 999999999999999999999999999999999999.9),(29, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_37_1_data_start_index = 22
    def test_cast_to_decimal32_9_0_from_decimal128i_37_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_37_18 values (30, 999999999.999999999999999999),(31, 999999999.999999999999999999),(32, 1000000000.999999999999999999),(33, 1000000000.999999999999999999),(34, 9999999999999999998.999999999999999999),
      (35, 9999999999999999998.999999999999999999),(36, 9999999999999999999.999999999999999999),(37, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_37_18_data_start_index = 30
    def test_cast_to_decimal32_9_0_from_decimal128i_37_18_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_38_0 values (38, 1000000000),(39, 99999999999999999999999999999999999998),(40, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_38_0_data_start_index = 38
    def test_cast_to_decimal32_9_0_from_decimal128i_38_0_data_end_index = 41
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_38_1 values (41, 999999999.9),(42, 999999999.9),(43, 1000000000.9),(44, 1000000000.9),(45, 9999999999999999999999999999999999998.9),
      (46, 9999999999999999999999999999999999998.9),(47, 9999999999999999999999999999999999999.9),(48, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_38_1_data_start_index = 41
    def test_cast_to_decimal32_9_0_from_decimal128i_38_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal128i_38_19 values (49, 999999999.9999999999999999999),(50, 999999999.9999999999999999999),(51, 1000000000.9999999999999999999),(52, 1000000000.9999999999999999999),(53, 9999999999999999998.9999999999999999999),
      (54, 9999999999999999998.9999999999999999999),(55, 9999999999999999999.9999999999999999999),(56, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_0_from_decimal128i_38_19_data_start_index = 49
    def test_cast_to_decimal32_9_0_from_decimal128i_38_19_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_9_0_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_9_0_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_19_0 values (57, 100000000),(58, 9999999999999999998),(59, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_19_0_data_start_index = 57
    def test_cast_to_decimal32_9_1_from_decimal128i_19_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_19_1 values (60, 100000000.9),(61, 999999999999999998.9),(62, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_19_1_data_start_index = 60
    def test_cast_to_decimal32_9_1_from_decimal128i_19_1_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_19_9 values (63, 99999999.999999999),(64, 99999999.999999999),(65, 100000000.999999999),(66, 100000000.999999999),(67, 9999999998.999999999),
      (68, 9999999998.999999999),(69, 9999999999.999999999),(70, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_19_9_data_start_index = 63
    def test_cast_to_decimal32_9_1_from_decimal128i_19_9_data_end_index = 71
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_37_0 values (71, 100000000),(72, 9999999999999999999999999999999999998),(73, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_37_0_data_start_index = 71
    def test_cast_to_decimal32_9_1_from_decimal128i_37_0_data_end_index = 74
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_37_1 values (74, 100000000.9),(75, 999999999999999999999999999999999998.9),(76, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_37_1_data_start_index = 74
    def test_cast_to_decimal32_9_1_from_decimal128i_37_1_data_end_index = 77
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_37_18 values (77, 99999999.999999999999999999),(78, 99999999.999999999999999999),(79, 100000000.999999999999999999),(80, 100000000.999999999999999999),(81, 9999999999999999998.999999999999999999),
      (82, 9999999999999999998.999999999999999999),(83, 9999999999999999999.999999999999999999),(84, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_37_18_data_start_index = 77
    def test_cast_to_decimal32_9_1_from_decimal128i_37_18_data_end_index = 85
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_38_0 values (85, 100000000),(86, 99999999999999999999999999999999999998),(87, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_38_0_data_start_index = 85
    def test_cast_to_decimal32_9_1_from_decimal128i_38_0_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_38_1 values (88, 100000000.9),(89, 9999999999999999999999999999999999998.9),(90, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_38_1_data_start_index = 88
    def test_cast_to_decimal32_9_1_from_decimal128i_38_1_data_end_index = 91
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal128i_38_19 values (91, 99999999.9999999999999999999),(92, 99999999.9999999999999999999),(93, 100000000.9999999999999999999),(94, 100000000.9999999999999999999),(95, 9999999999999999998.9999999999999999999),
      (96, 9999999999999999998.9999999999999999999),(97, 9999999999999999999.9999999999999999999),(98, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_1_from_decimal128i_38_19_data_start_index = 91
    def test_cast_to_decimal32_9_1_from_decimal128i_38_19_data_end_index = 99
    for (int data_index = test_cast_to_decimal32_9_1_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_9_1_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_19_0 values (99, 100000),(100, 9999999999999999998),(101, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_19_0_data_start_index = 99
    def test_cast_to_decimal32_9_4_from_decimal128i_19_0_data_end_index = 102
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_19_1 values (102, 100000.9),(103, 999999999999999998.9),(104, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_19_1_data_start_index = 102
    def test_cast_to_decimal32_9_4_from_decimal128i_19_1_data_end_index = 105
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_19_9 values (105, 99999.999999999),(106, 99999.999999999),(107, 100000.999999999),(108, 100000.999999999),(109, 9999999998.999999999),
      (110, 9999999998.999999999),(111, 9999999999.999999999),(112, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_19_9_data_start_index = 105
    def test_cast_to_decimal32_9_4_from_decimal128i_19_9_data_end_index = 113
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_37_0 values (113, 100000),(114, 9999999999999999999999999999999999998),(115, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_37_0_data_start_index = 113
    def test_cast_to_decimal32_9_4_from_decimal128i_37_0_data_end_index = 116
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_37_1 values (116, 100000.9),(117, 999999999999999999999999999999999998.9),(118, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_37_1_data_start_index = 116
    def test_cast_to_decimal32_9_4_from_decimal128i_37_1_data_end_index = 119
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_37_18 values (119, 99999.999999999999999999),(120, 99999.999999999999999999),(121, 100000.999999999999999999),(122, 100000.999999999999999999),(123, 9999999999999999998.999999999999999999),
      (124, 9999999999999999998.999999999999999999),(125, 9999999999999999999.999999999999999999),(126, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_37_18_data_start_index = 119
    def test_cast_to_decimal32_9_4_from_decimal128i_37_18_data_end_index = 127
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_38_0 values (127, 100000),(128, 99999999999999999999999999999999999998),(129, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_38_0_data_start_index = 127
    def test_cast_to_decimal32_9_4_from_decimal128i_38_0_data_end_index = 130
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_38_1 values (130, 100000.9),(131, 9999999999999999999999999999999999998.9),(132, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_38_1_data_start_index = 130
    def test_cast_to_decimal32_9_4_from_decimal128i_38_1_data_end_index = 133
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_9_4_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_decimal128i_38_19 values (133, 99999.9999999999999999999),(134, 99999.9999999999999999999),(135, 100000.9999999999999999999),(136, 100000.9999999999999999999),(137, 9999999999999999998.9999999999999999999),
      (138, 9999999999999999998.9999999999999999999),(139, 9999999999999999999.9999999999999999999),(140, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_4_from_decimal128i_38_19_data_start_index = 133
    def test_cast_to_decimal32_9_4_from_decimal128i_38_19_data_end_index = 141
    for (int data_index = test_cast_to_decimal32_9_4_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_9_4_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_19_0 values (141, 10),(142, 9999999999999999998),(143, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_19_0_data_start_index = 141
    def test_cast_to_decimal32_9_8_from_decimal128i_19_0_data_end_index = 144
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_19_1 values (144, 10.9),(145, 999999999999999998.9),(146, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_19_1_data_start_index = 144
    def test_cast_to_decimal32_9_8_from_decimal128i_19_1_data_end_index = 147
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_19_9 values (147, 9.999999999),(148, 9.999999999),(149, 10.999999999),(150, 10.999999999),(151, 9999999998.999999999),
      (152, 9999999998.999999999),(153, 9999999999.999999999),(154, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_19_9_data_start_index = 147
    def test_cast_to_decimal32_9_8_from_decimal128i_19_9_data_end_index = 155
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_19_18 values (155, 9.999999999999999999),(156, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_19_18_data_start_index = 155
    def test_cast_to_decimal32_9_8_from_decimal128i_19_18_data_end_index = 157
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_37_0 values (157, 10),(158, 9999999999999999999999999999999999998),(159, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_37_0_data_start_index = 157
    def test_cast_to_decimal32_9_8_from_decimal128i_37_0_data_end_index = 160
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_37_1 values (160, 10.9),(161, 999999999999999999999999999999999998.9),(162, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_37_1_data_start_index = 160
    def test_cast_to_decimal32_9_8_from_decimal128i_37_1_data_end_index = 163
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_37_18 values (163, 9.999999999999999999),(164, 9.999999999999999999),(165, 10.999999999999999999),(166, 10.999999999999999999),(167, 9999999999999999998.999999999999999999),
      (168, 9999999999999999998.999999999999999999),(169, 9999999999999999999.999999999999999999),(170, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_37_18_data_start_index = 163
    def test_cast_to_decimal32_9_8_from_decimal128i_37_18_data_end_index = 171
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_37_36 values (171, 9.999999999999999999999999999999999999),(172, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_37_36_data_start_index = 171
    def test_cast_to_decimal32_9_8_from_decimal128i_37_36_data_end_index = 173
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_38_0 values (173, 10),(174, 99999999999999999999999999999999999998),(175, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_38_0_data_start_index = 173
    def test_cast_to_decimal32_9_8_from_decimal128i_38_0_data_end_index = 176
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_38_1 values (176, 10.9),(177, 9999999999999999999999999999999999998.9),(178, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_38_1_data_start_index = 176
    def test_cast_to_decimal32_9_8_from_decimal128i_38_1_data_end_index = 179
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_38_19 values (179, 9.9999999999999999999),(180, 9.9999999999999999999),(181, 10.9999999999999999999),(182, 10.9999999999999999999),(183, 9999999999999999998.9999999999999999999),
      (184, 9999999999999999998.9999999999999999999),(185, 9999999999999999999.9999999999999999999),(186, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_38_19_data_start_index = 179
    def test_cast_to_decimal32_9_8_from_decimal128i_38_19_data_end_index = 187
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_9_8_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_decimal128i_38_37 values (187, 9.9999999999999999999999999999999999999),(188, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_8_from_decimal128i_38_37_data_start_index = 187
    def test_cast_to_decimal32_9_8_from_decimal128i_38_37_data_end_index = 189
    for (int data_index = test_cast_to_decimal32_9_8_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal32_9_8_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_19_0 values (189, 1),(190, 9999999999999999998),(191, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_19_0_data_start_index = 189
    def test_cast_to_decimal32_9_9_from_decimal128i_19_0_data_end_index = 192
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_19_1 values (192, 1.9),(193, 999999999999999998.9),(194, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_19_1_data_start_index = 192
    def test_cast_to_decimal32_9_9_from_decimal128i_19_1_data_end_index = 195
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_19_9 values (195, 1.999999999),(196, 9999999998.999999999),(197, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_19_9_data_start_index = 195
    def test_cast_to_decimal32_9_9_from_decimal128i_19_9_data_end_index = 198
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_19_18 values (198, 0.999999999999999999),(199, 0.999999999999999999),(200, 1.999999999999999999),(201, 1.999999999999999999),(202, 8.999999999999999999),
      (203, 8.999999999999999999),(204, 9.999999999999999999),(205, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_19_18_data_start_index = 198
    def test_cast_to_decimal32_9_9_from_decimal128i_19_18_data_end_index = 206
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_19_19 values (206, 0.9999999999999999999),(207, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_19_19_data_start_index = 206
    def test_cast_to_decimal32_9_9_from_decimal128i_19_19_data_end_index = 208
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_19_19_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_37_0 values (208, 1),(209, 9999999999999999999999999999999999998),(210, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_37_0_data_start_index = 208
    def test_cast_to_decimal32_9_9_from_decimal128i_37_0_data_end_index = 211
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_37_1 values (211, 1.9),(212, 999999999999999999999999999999999998.9),(213, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_37_1_data_start_index = 211
    def test_cast_to_decimal32_9_9_from_decimal128i_37_1_data_end_index = 214
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_37_18 values (214, 0.999999999999999999),(215, 0.999999999999999999),(216, 1.999999999999999999),(217, 1.999999999999999999),(218, 9999999999999999998.999999999999999999),
      (219, 9999999999999999998.999999999999999999),(220, 9999999999999999999.999999999999999999),(221, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_37_18_data_start_index = 214
    def test_cast_to_decimal32_9_9_from_decimal128i_37_18_data_end_index = 222
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_37_36 values (222, 0.999999999999999999999999999999999999),(223, 0.999999999999999999999999999999999999),(224, 1.999999999999999999999999999999999999),(225, 1.999999999999999999999999999999999999),(226, 8.999999999999999999999999999999999999),
      (227, 8.999999999999999999999999999999999999),(228, 9.999999999999999999999999999999999999),(229, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_37_36_data_start_index = 222
    def test_cast_to_decimal32_9_9_from_decimal128i_37_36_data_end_index = 230
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_37_37 values (230, 0.9999999999999999999999999999999999999),(231, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_37_37_data_start_index = 230
    def test_cast_to_decimal32_9_9_from_decimal128i_37_37_data_end_index = 232
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_37_37_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_38_0 values (232, 1),(233, 99999999999999999999999999999999999998),(234, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_38_0_data_start_index = 232
    def test_cast_to_decimal32_9_9_from_decimal128i_38_0_data_end_index = 235
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_38_1 values (235, 1.9),(236, 9999999999999999999999999999999999998.9),(237, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_38_1_data_start_index = 235
    def test_cast_to_decimal32_9_9_from_decimal128i_38_1_data_end_index = 238
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_38_19 values (238, 0.9999999999999999999),(239, 0.9999999999999999999),(240, 1.9999999999999999999),(241, 1.9999999999999999999),(242, 9999999999999999998.9999999999999999999),
      (243, 9999999999999999998.9999999999999999999),(244, 9999999999999999999.9999999999999999999),(245, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_38_19_data_start_index = 238
    def test_cast_to_decimal32_9_9_from_decimal128i_38_19_data_end_index = 246
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_38_37 values (246, 0.9999999999999999999999999999999999999),(247, 0.9999999999999999999999999999999999999),(248, 1.9999999999999999999999999999999999999),(249, 1.9999999999999999999999999999999999999),(250, 8.9999999999999999999999999999999999999),
      (251, 8.9999999999999999999999999999999999999),(252, 9.9999999999999999999999999999999999999),(253, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_38_37_data_start_index = 246
    def test_cast_to_decimal32_9_9_from_decimal128i_38_37_data_end_index = 254
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal128i_38_38 values (254, 0.99999999999999999999999999999999999999),(255, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_9_9_from_decimal128i_38_38_data_start_index = 254
    def test_cast_to_decimal32_9_9_from_decimal128i_38_38_data_end_index = 256
    for (int data_index = test_cast_to_decimal32_9_9_from_decimal128i_38_38_data_start_index; data_index < test_cast_to_decimal32_9_9_from_decimal128i_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal128i_38_38 order by 1;'

}