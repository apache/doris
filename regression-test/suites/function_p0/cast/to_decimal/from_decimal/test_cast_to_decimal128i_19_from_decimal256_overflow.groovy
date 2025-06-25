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


suite("test_cast_to_decimal128i_19_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_38_0 values (0, 10000000000000000000),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_38_0_data_start_index = 0
    def test_cast_to_decimal128i_19_0_from_decimal256_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_38_1 values (3, 9999999999999999999.9),(4, 9999999999999999999.9),(5, 10000000000000000000.9),(6, 10000000000000000000.9),(7, 9999999999999999999999999999999999998.9),
      (8, 9999999999999999999999999999999999998.9),(9, 9999999999999999999999999999999999999.9),(10, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_38_1_data_start_index = 3
    def test_cast_to_decimal128i_19_0_from_decimal256_38_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_38_19 values (11, 9999999999999999999.9999999999999999999),(12, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_38_19_data_start_index = 11
    def test_cast_to_decimal128i_19_0_from_decimal256_38_19_data_end_index = 13
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_39_0 values (13, 10000000000000000000),(14, 999999999999999999999999999999999999998),(15, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_39_0_data_start_index = 13
    def test_cast_to_decimal128i_19_0_from_decimal256_39_0_data_end_index = 16
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_39_1 values (16, 9999999999999999999.9),(17, 9999999999999999999.9),(18, 10000000000000000000.9),(19, 10000000000000000000.9),(20, 99999999999999999999999999999999999998.9),
      (21, 99999999999999999999999999999999999998.9),(22, 99999999999999999999999999999999999999.9),(23, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_39_1_data_start_index = 16
    def test_cast_to_decimal128i_19_0_from_decimal256_39_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_39_19 values (24, 9999999999999999999.9999999999999999999),(25, 9999999999999999999.9999999999999999999),(26, 10000000000000000000.9999999999999999999),(27, 10000000000000000000.9999999999999999999),(28, 99999999999999999998.9999999999999999999),
      (29, 99999999999999999998.9999999999999999999),(30, 99999999999999999999.9999999999999999999),(31, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_39_19_data_start_index = 24
    def test_cast_to_decimal128i_19_0_from_decimal256_39_19_data_end_index = 32
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_75_0 values (32, 10000000000000000000),(33, 999999999999999999999999999999999999999999999999999999999999999999999999998),(34, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_75_0_data_start_index = 32
    def test_cast_to_decimal128i_19_0_from_decimal256_75_0_data_end_index = 35
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_75_1 values (35, 9999999999999999999.9),(36, 9999999999999999999.9),(37, 10000000000000000000.9),(38, 10000000000000000000.9),(39, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (40, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(41, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(42, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_75_1_data_start_index = 35
    def test_cast_to_decimal128i_19_0_from_decimal256_75_1_data_end_index = 43
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_75_37 values (43, 9999999999999999999.9999999999999999999999999999999999999),(44, 9999999999999999999.9999999999999999999999999999999999999),(45, 10000000000000000000.9999999999999999999999999999999999999),(46, 10000000000000000000.9999999999999999999999999999999999999),(47, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (48, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(49, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(50, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_75_37_data_start_index = 43
    def test_cast_to_decimal128i_19_0_from_decimal256_75_37_data_end_index = 51
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_76_0 values (51, 10000000000000000000),(52, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(53, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_76_0_data_start_index = 51
    def test_cast_to_decimal128i_19_0_from_decimal256_76_0_data_end_index = 54
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_76_1 values (54, 9999999999999999999.9),(55, 9999999999999999999.9),(56, 10000000000000000000.9),(57, 10000000000000000000.9),(58, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (59, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(60, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(61, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_76_1_data_start_index = 54
    def test_cast_to_decimal128i_19_0_from_decimal256_76_1_data_end_index = 62
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal256_76_38 values (62, 9999999999999999999.99999999999999999999999999999999999999),(63, 9999999999999999999.99999999999999999999999999999999999999),(64, 10000000000000000000.99999999999999999999999999999999999999),(65, 10000000000000000000.99999999999999999999999999999999999999),(66, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (67, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(68, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(69, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal256_76_38_data_start_index = 62
    def test_cast_to_decimal128i_19_0_from_decimal256_76_38_data_end_index = 70
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_38_0 values (70, 1000000000000000000),(71, 99999999999999999999999999999999999998),(72, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_38_0_data_start_index = 70
    def test_cast_to_decimal128i_19_1_from_decimal256_38_0_data_end_index = 73
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_38_1 values (73, 1000000000000000000.9),(74, 9999999999999999999999999999999999998.9),(75, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_38_1_data_start_index = 73
    def test_cast_to_decimal128i_19_1_from_decimal256_38_1_data_end_index = 76
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_38_19 values (76, 999999999999999999.9999999999999999999),(77, 999999999999999999.9999999999999999999),(78, 1000000000000000000.9999999999999999999),(79, 1000000000000000000.9999999999999999999),(80, 9999999999999999998.9999999999999999999),
      (81, 9999999999999999998.9999999999999999999),(82, 9999999999999999999.9999999999999999999),(83, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_38_19_data_start_index = 76
    def test_cast_to_decimal128i_19_1_from_decimal256_38_19_data_end_index = 84
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_39_0 values (84, 1000000000000000000),(85, 999999999999999999999999999999999999998),(86, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_39_0_data_start_index = 84
    def test_cast_to_decimal128i_19_1_from_decimal256_39_0_data_end_index = 87
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_39_1 values (87, 1000000000000000000.9),(88, 99999999999999999999999999999999999998.9),(89, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_39_1_data_start_index = 87
    def test_cast_to_decimal128i_19_1_from_decimal256_39_1_data_end_index = 90
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_39_19 values (90, 999999999999999999.9999999999999999999),(91, 999999999999999999.9999999999999999999),(92, 1000000000000000000.9999999999999999999),(93, 1000000000000000000.9999999999999999999),(94, 99999999999999999998.9999999999999999999),
      (95, 99999999999999999998.9999999999999999999),(96, 99999999999999999999.9999999999999999999),(97, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_39_19_data_start_index = 90
    def test_cast_to_decimal128i_19_1_from_decimal256_39_19_data_end_index = 98
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_75_0 values (98, 1000000000000000000),(99, 999999999999999999999999999999999999999999999999999999999999999999999999998),(100, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_75_0_data_start_index = 98
    def test_cast_to_decimal128i_19_1_from_decimal256_75_0_data_end_index = 101
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_75_1 values (101, 1000000000000000000.9),(102, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(103, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_75_1_data_start_index = 101
    def test_cast_to_decimal128i_19_1_from_decimal256_75_1_data_end_index = 104
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_75_37 values (104, 999999999999999999.9999999999999999999999999999999999999),(105, 999999999999999999.9999999999999999999999999999999999999),(106, 1000000000000000000.9999999999999999999999999999999999999),(107, 1000000000000000000.9999999999999999999999999999999999999),(108, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (109, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(110, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(111, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_75_37_data_start_index = 104
    def test_cast_to_decimal128i_19_1_from_decimal256_75_37_data_end_index = 112
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_76_0 values (112, 1000000000000000000),(113, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(114, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_76_0_data_start_index = 112
    def test_cast_to_decimal128i_19_1_from_decimal256_76_0_data_end_index = 115
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_76_1 values (115, 1000000000000000000.9),(116, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(117, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_76_1_data_start_index = 115
    def test_cast_to_decimal128i_19_1_from_decimal256_76_1_data_end_index = 118
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal256_76_38 values (118, 999999999999999999.99999999999999999999999999999999999999),(119, 999999999999999999.99999999999999999999999999999999999999),(120, 1000000000000000000.99999999999999999999999999999999999999),(121, 1000000000000000000.99999999999999999999999999999999999999),(122, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (123, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(124, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(125, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal256_76_38_data_start_index = 118
    def test_cast_to_decimal128i_19_1_from_decimal256_76_38_data_end_index = 126
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_38_0 values (126, 10000000000),(127, 99999999999999999999999999999999999998),(128, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_38_0_data_start_index = 126
    def test_cast_to_decimal128i_19_9_from_decimal256_38_0_data_end_index = 129
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_38_1 values (129, 10000000000.9),(130, 9999999999999999999999999999999999998.9),(131, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_38_1_data_start_index = 129
    def test_cast_to_decimal128i_19_9_from_decimal256_38_1_data_end_index = 132
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_38_19 values (132, 9999999999.9999999999999999999),(133, 9999999999.9999999999999999999),(134, 10000000000.9999999999999999999),(135, 10000000000.9999999999999999999),(136, 9999999999999999998.9999999999999999999),
      (137, 9999999999999999998.9999999999999999999),(138, 9999999999999999999.9999999999999999999),(139, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_38_19_data_start_index = 132
    def test_cast_to_decimal128i_19_9_from_decimal256_38_19_data_end_index = 140
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_39_0 values (140, 10000000000),(141, 999999999999999999999999999999999999998),(142, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_39_0_data_start_index = 140
    def test_cast_to_decimal128i_19_9_from_decimal256_39_0_data_end_index = 143
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_39_1 values (143, 10000000000.9),(144, 99999999999999999999999999999999999998.9),(145, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_39_1_data_start_index = 143
    def test_cast_to_decimal128i_19_9_from_decimal256_39_1_data_end_index = 146
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_39_19 values (146, 9999999999.9999999999999999999),(147, 9999999999.9999999999999999999),(148, 10000000000.9999999999999999999),(149, 10000000000.9999999999999999999),(150, 99999999999999999998.9999999999999999999),
      (151, 99999999999999999998.9999999999999999999),(152, 99999999999999999999.9999999999999999999),(153, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_39_19_data_start_index = 146
    def test_cast_to_decimal128i_19_9_from_decimal256_39_19_data_end_index = 154
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_75_0 values (154, 10000000000),(155, 999999999999999999999999999999999999999999999999999999999999999999999999998),(156, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_75_0_data_start_index = 154
    def test_cast_to_decimal128i_19_9_from_decimal256_75_0_data_end_index = 157
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_75_1 values (157, 10000000000.9),(158, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(159, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_75_1_data_start_index = 157
    def test_cast_to_decimal128i_19_9_from_decimal256_75_1_data_end_index = 160
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_75_37 values (160, 9999999999.9999999999999999999999999999999999999),(161, 9999999999.9999999999999999999999999999999999999),(162, 10000000000.9999999999999999999999999999999999999),(163, 10000000000.9999999999999999999999999999999999999),(164, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (165, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(166, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(167, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_75_37_data_start_index = 160
    def test_cast_to_decimal128i_19_9_from_decimal256_75_37_data_end_index = 168
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_76_0 values (168, 10000000000),(169, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(170, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_76_0_data_start_index = 168
    def test_cast_to_decimal128i_19_9_from_decimal256_76_0_data_end_index = 171
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_76_1 values (171, 10000000000.9),(172, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(173, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_76_1_data_start_index = 171
    def test_cast_to_decimal128i_19_9_from_decimal256_76_1_data_end_index = 174
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal256_76_38 values (174, 9999999999.99999999999999999999999999999999999999),(175, 9999999999.99999999999999999999999999999999999999),(176, 10000000000.99999999999999999999999999999999999999),(177, 10000000000.99999999999999999999999999999999999999),(178, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (179, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(180, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(181, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal256_76_38_data_start_index = 174
    def test_cast_to_decimal128i_19_9_from_decimal256_76_38_data_end_index = 182
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_38_0 values (182, 10),(183, 99999999999999999999999999999999999998),(184, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_38_0_data_start_index = 182
    def test_cast_to_decimal128i_19_18_from_decimal256_38_0_data_end_index = 185
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_38_1 values (185, 10.9),(186, 9999999999999999999999999999999999998.9),(187, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_38_1_data_start_index = 185
    def test_cast_to_decimal128i_19_18_from_decimal256_38_1_data_end_index = 188
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_38_19 values (188, 9.9999999999999999999),(189, 9.9999999999999999999),(190, 10.9999999999999999999),(191, 10.9999999999999999999),(192, 9999999999999999998.9999999999999999999),
      (193, 9999999999999999998.9999999999999999999),(194, 9999999999999999999.9999999999999999999),(195, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_38_19_data_start_index = 188
    def test_cast_to_decimal128i_19_18_from_decimal256_38_19_data_end_index = 196
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_38_37 values (196, 9.9999999999999999999999999999999999999),(197, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_38_37_data_start_index = 196
    def test_cast_to_decimal128i_19_18_from_decimal256_38_37_data_end_index = 198
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_39_0 values (198, 10),(199, 999999999999999999999999999999999999998),(200, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_39_0_data_start_index = 198
    def test_cast_to_decimal128i_19_18_from_decimal256_39_0_data_end_index = 201
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_39_1 values (201, 10.9),(202, 99999999999999999999999999999999999998.9),(203, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_39_1_data_start_index = 201
    def test_cast_to_decimal128i_19_18_from_decimal256_39_1_data_end_index = 204
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_39_19 values (204, 9.9999999999999999999),(205, 9.9999999999999999999),(206, 10.9999999999999999999),(207, 10.9999999999999999999),(208, 99999999999999999998.9999999999999999999),
      (209, 99999999999999999998.9999999999999999999),(210, 99999999999999999999.9999999999999999999),(211, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_39_19_data_start_index = 204
    def test_cast_to_decimal128i_19_18_from_decimal256_39_19_data_end_index = 212
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_39_38 values (212, 9.99999999999999999999999999999999999999),(213, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_39_38_data_start_index = 212
    def test_cast_to_decimal128i_19_18_from_decimal256_39_38_data_end_index = 214
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_75_0 values (214, 10),(215, 999999999999999999999999999999999999999999999999999999999999999999999999998),(216, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_75_0_data_start_index = 214
    def test_cast_to_decimal128i_19_18_from_decimal256_75_0_data_end_index = 217
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_75_1 values (217, 10.9),(218, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(219, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_75_1_data_start_index = 217
    def test_cast_to_decimal128i_19_18_from_decimal256_75_1_data_end_index = 220
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_75_37 values (220, 9.9999999999999999999999999999999999999),(221, 9.9999999999999999999999999999999999999),(222, 10.9999999999999999999999999999999999999),(223, 10.9999999999999999999999999999999999999),(224, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (225, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(226, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(227, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_75_37_data_start_index = 220
    def test_cast_to_decimal128i_19_18_from_decimal256_75_37_data_end_index = 228
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_75_74 values (228, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(229, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_75_74_data_start_index = 228
    def test_cast_to_decimal128i_19_18_from_decimal256_75_74_data_end_index = 230
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_76_0 values (230, 10),(231, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(232, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_76_0_data_start_index = 230
    def test_cast_to_decimal128i_19_18_from_decimal256_76_0_data_end_index = 233
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_76_1 values (233, 10.9),(234, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(235, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_76_1_data_start_index = 233
    def test_cast_to_decimal128i_19_18_from_decimal256_76_1_data_end_index = 236
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_76_38 values (236, 9.99999999999999999999999999999999999999),(237, 9.99999999999999999999999999999999999999),(238, 10.99999999999999999999999999999999999999),(239, 10.99999999999999999999999999999999999999),(240, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (241, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(242, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(243, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_76_38_data_start_index = 236
    def test_cast_to_decimal128i_19_18_from_decimal256_76_38_data_end_index = 244
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal256_76_75 values (244, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(245, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal256_76_75_data_start_index = 244
    def test_cast_to_decimal128i_19_18_from_decimal256_76_75_data_end_index = 246
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_38_0 values (246, 1),(247, 99999999999999999999999999999999999998),(248, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_38_0_data_start_index = 246
    def test_cast_to_decimal128i_19_19_from_decimal256_38_0_data_end_index = 249
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_38_1 values (249, 1.9),(250, 9999999999999999999999999999999999998.9),(251, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_38_1_data_start_index = 249
    def test_cast_to_decimal128i_19_19_from_decimal256_38_1_data_end_index = 252
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_38_19 values (252, 1.9999999999999999999),(253, 9999999999999999998.9999999999999999999),(254, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_38_19_data_start_index = 252
    def test_cast_to_decimal128i_19_19_from_decimal256_38_19_data_end_index = 255
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_38_37 values (255, 0.9999999999999999999999999999999999999),(256, 0.9999999999999999999999999999999999999),(257, 1.9999999999999999999999999999999999999),(258, 1.9999999999999999999999999999999999999),(259, 8.9999999999999999999999999999999999999),
      (260, 8.9999999999999999999999999999999999999),(261, 9.9999999999999999999999999999999999999),(262, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_38_37_data_start_index = 255
    def test_cast_to_decimal128i_19_19_from_decimal256_38_37_data_end_index = 263
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_38_38;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_38_38 values (263, 0.99999999999999999999999999999999999999),(264, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_38_38_data_start_index = 263
    def test_cast_to_decimal128i_19_19_from_decimal256_38_38_data_end_index = 265
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_38_38_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_38_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_39_0 values (265, 1),(266, 999999999999999999999999999999999999998),(267, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_39_0_data_start_index = 265
    def test_cast_to_decimal128i_19_19_from_decimal256_39_0_data_end_index = 268
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_39_1 values (268, 1.9),(269, 99999999999999999999999999999999999998.9),(270, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_39_1_data_start_index = 268
    def test_cast_to_decimal128i_19_19_from_decimal256_39_1_data_end_index = 271
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_39_19 values (271, 1.9999999999999999999),(272, 99999999999999999998.9999999999999999999),(273, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_39_19_data_start_index = 271
    def test_cast_to_decimal128i_19_19_from_decimal256_39_19_data_end_index = 274
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_39_38 values (274, 0.99999999999999999999999999999999999999),(275, 0.99999999999999999999999999999999999999),(276, 1.99999999999999999999999999999999999999),(277, 1.99999999999999999999999999999999999999),(278, 8.99999999999999999999999999999999999999),
      (279, 8.99999999999999999999999999999999999999),(280, 9.99999999999999999999999999999999999999),(281, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_39_38_data_start_index = 274
    def test_cast_to_decimal128i_19_19_from_decimal256_39_38_data_end_index = 282
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_39_39 values (282, 0.999999999999999999999999999999999999999),(283, 0.999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_39_39_data_start_index = 282
    def test_cast_to_decimal128i_19_19_from_decimal256_39_39_data_end_index = 284
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_39_39_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_39_39_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_39 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_75_0 values (284, 1),(285, 999999999999999999999999999999999999999999999999999999999999999999999999998),(286, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_75_0_data_start_index = 284
    def test_cast_to_decimal128i_19_19_from_decimal256_75_0_data_end_index = 287
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_75_1 values (287, 1.9),(288, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(289, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_75_1_data_start_index = 287
    def test_cast_to_decimal128i_19_19_from_decimal256_75_1_data_end_index = 290
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_75_37 values (290, 0.9999999999999999999999999999999999999),(291, 0.9999999999999999999999999999999999999),(292, 1.9999999999999999999999999999999999999),(293, 1.9999999999999999999999999999999999999),(294, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (295, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(296, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(297, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_75_37_data_start_index = 290
    def test_cast_to_decimal128i_19_19_from_decimal256_75_37_data_end_index = 298
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_75_74 values (298, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(299, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(300, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(301, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(302, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (303, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(304, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(305, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_75_74_data_start_index = 298
    def test_cast_to_decimal128i_19_19_from_decimal256_75_74_data_end_index = 306
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_75_75 values (306, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(307, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_75_75_data_start_index = 306
    def test_cast_to_decimal128i_19_19_from_decimal256_75_75_data_end_index = 308
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_76_0 values (308, 1),(309, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(310, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_76_0_data_start_index = 308
    def test_cast_to_decimal128i_19_19_from_decimal256_76_0_data_end_index = 311
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_76_1 values (311, 1.9),(312, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(313, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_76_1_data_start_index = 311
    def test_cast_to_decimal128i_19_19_from_decimal256_76_1_data_end_index = 314
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_76_38 values (314, 0.99999999999999999999999999999999999999),(315, 0.99999999999999999999999999999999999999),(316, 1.99999999999999999999999999999999999999),(317, 1.99999999999999999999999999999999999999),(318, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (319, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(320, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(321, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_76_38_data_start_index = 314
    def test_cast_to_decimal128i_19_19_from_decimal256_76_38_data_end_index = 322
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_76_75 values (322, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(323, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(324, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(325, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(326, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (327, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(328, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(329, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_76_75_data_start_index = 322
    def test_cast_to_decimal128i_19_19_from_decimal256_76_75_data_end_index = 330
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal256_76_76 values (330, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(331, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal256_76_76_data_start_index = 330
    def test_cast_to_decimal128i_19_19_from_decimal256_76_76_data_end_index = 332
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal256_76_76 order by 1;'

}