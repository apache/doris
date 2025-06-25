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


suite("test_cast_to_decimal128i_38_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_39_0 values (0, 100000000000000000000000000000000000000),(1, 999999999999999999999999999999999999998),(2, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_39_0_data_start_index = 0
    def test_cast_to_decimal128i_38_0_from_decimal256_39_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_39_1 values (3, 99999999999999999999999999999999999999.9),(4, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_39_1_data_start_index = 3
    def test_cast_to_decimal128i_38_0_from_decimal256_39_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_75_0 values (5, 100000000000000000000000000000000000000),(6, 999999999999999999999999999999999999999999999999999999999999999999999999998),(7, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_75_0_data_start_index = 5
    def test_cast_to_decimal128i_38_0_from_decimal256_75_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_75_1 values (8, 99999999999999999999999999999999999999.9),(9, 99999999999999999999999999999999999999.9),(10, 100000000000000000000000000000000000000.9),(11, 100000000000000000000000000000000000000.9),(12, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (13, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(14, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(15, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_75_1_data_start_index = 8
    def test_cast_to_decimal128i_38_0_from_decimal256_75_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_75_37 values (16, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(17, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_75_37_data_start_index = 16
    def test_cast_to_decimal128i_38_0_from_decimal256_75_37_data_end_index = 18
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_76_0 values (18, 100000000000000000000000000000000000000),(19, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(20, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_76_0_data_start_index = 18
    def test_cast_to_decimal128i_38_0_from_decimal256_76_0_data_end_index = 21
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_76_1 values (21, 99999999999999999999999999999999999999.9),(22, 99999999999999999999999999999999999999.9),(23, 100000000000000000000000000000000000000.9),(24, 100000000000000000000000000000000000000.9),(25, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (26, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(27, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(28, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_76_1_data_start_index = 21
    def test_cast_to_decimal128i_38_0_from_decimal256_76_1_data_end_index = 29
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_38_0_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_decimal256_76_38 values (29, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(30, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_0_from_decimal256_76_38_data_start_index = 29
    def test_cast_to_decimal128i_38_0_from_decimal256_76_38_data_end_index = 31
    for (int data_index = test_cast_to_decimal128i_38_0_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_38_0_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_38_0 values (31, 10000000000000000000000000000000000000),(32, 99999999999999999999999999999999999998),(33, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_38_0_data_start_index = 31
    def test_cast_to_decimal128i_38_1_from_decimal256_38_0_data_end_index = 34
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_39_0 values (34, 10000000000000000000000000000000000000),(35, 999999999999999999999999999999999999998),(36, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_39_0_data_start_index = 34
    def test_cast_to_decimal128i_38_1_from_decimal256_39_0_data_end_index = 37
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_39_1 values (37, 10000000000000000000000000000000000000.9),(38, 99999999999999999999999999999999999998.9),(39, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_39_1_data_start_index = 37
    def test_cast_to_decimal128i_38_1_from_decimal256_39_1_data_end_index = 40
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_75_0 values (40, 10000000000000000000000000000000000000),(41, 999999999999999999999999999999999999999999999999999999999999999999999999998),(42, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_75_0_data_start_index = 40
    def test_cast_to_decimal128i_38_1_from_decimal256_75_0_data_end_index = 43
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_75_1 values (43, 10000000000000000000000000000000000000.9),(44, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(45, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_75_1_data_start_index = 43
    def test_cast_to_decimal128i_38_1_from_decimal256_75_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_75_37 values (46, 9999999999999999999999999999999999999.9999999999999999999999999999999999999),(47, 9999999999999999999999999999999999999.9999999999999999999999999999999999999),(48, 10000000000000000000000000000000000000.9999999999999999999999999999999999999),(49, 10000000000000000000000000000000000000.9999999999999999999999999999999999999),(50, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (51, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(52, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(53, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_75_37_data_start_index = 46
    def test_cast_to_decimal128i_38_1_from_decimal256_75_37_data_end_index = 54
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_76_0 values (54, 10000000000000000000000000000000000000),(55, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(56, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_76_0_data_start_index = 54
    def test_cast_to_decimal128i_38_1_from_decimal256_76_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_76_1 values (57, 10000000000000000000000000000000000000.9),(58, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(59, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_76_1_data_start_index = 57
    def test_cast_to_decimal128i_38_1_from_decimal256_76_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal256_76_38 values (60, 9999999999999999999999999999999999999.99999999999999999999999999999999999999),(61, 9999999999999999999999999999999999999.99999999999999999999999999999999999999),(62, 10000000000000000000000000000000000000.99999999999999999999999999999999999999),(63, 10000000000000000000000000000000000000.99999999999999999999999999999999999999),(64, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (65, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(66, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(67, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal256_76_38_data_start_index = 60
    def test_cast_to_decimal128i_38_1_from_decimal256_76_38_data_end_index = 68
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_38_0 values (68, 10000000000000000000),(69, 99999999999999999999999999999999999998),(70, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_38_0_data_start_index = 68
    def test_cast_to_decimal128i_38_19_from_decimal256_38_0_data_end_index = 71
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_38_1 values (71, 10000000000000000000.9),(72, 9999999999999999999999999999999999998.9),(73, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_38_1_data_start_index = 71
    def test_cast_to_decimal128i_38_19_from_decimal256_38_1_data_end_index = 74
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_39_0 values (74, 10000000000000000000),(75, 999999999999999999999999999999999999998),(76, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_39_0_data_start_index = 74
    def test_cast_to_decimal128i_38_19_from_decimal256_39_0_data_end_index = 77
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_39_1 values (77, 10000000000000000000.9),(78, 99999999999999999999999999999999999998.9),(79, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_39_1_data_start_index = 77
    def test_cast_to_decimal128i_38_19_from_decimal256_39_1_data_end_index = 80
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_39_19 values (80, 10000000000000000000.9999999999999999999),(81, 99999999999999999998.9999999999999999999),(82, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_39_19_data_start_index = 80
    def test_cast_to_decimal128i_38_19_from_decimal256_39_19_data_end_index = 83
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_75_0 values (83, 10000000000000000000),(84, 999999999999999999999999999999999999999999999999999999999999999999999999998),(85, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_75_0_data_start_index = 83
    def test_cast_to_decimal128i_38_19_from_decimal256_75_0_data_end_index = 86
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_75_1 values (86, 10000000000000000000.9),(87, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(88, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_75_1_data_start_index = 86
    def test_cast_to_decimal128i_38_19_from_decimal256_75_1_data_end_index = 89
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_75_37 values (89, 9999999999999999999.9999999999999999999999999999999999999),(90, 9999999999999999999.9999999999999999999999999999999999999),(91, 10000000000000000000.9999999999999999999999999999999999999),(92, 10000000000000000000.9999999999999999999999999999999999999),(93, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (94, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(95, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(96, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_75_37_data_start_index = 89
    def test_cast_to_decimal128i_38_19_from_decimal256_75_37_data_end_index = 97
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_76_0 values (97, 10000000000000000000),(98, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(99, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_76_0_data_start_index = 97
    def test_cast_to_decimal128i_38_19_from_decimal256_76_0_data_end_index = 100
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_76_1 values (100, 10000000000000000000.9),(101, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(102, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_76_1_data_start_index = 100
    def test_cast_to_decimal128i_38_19_from_decimal256_76_1_data_end_index = 103
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal256_76_38 values (103, 9999999999999999999.99999999999999999999999999999999999999),(104, 9999999999999999999.99999999999999999999999999999999999999),(105, 10000000000000000000.99999999999999999999999999999999999999),(106, 10000000000000000000.99999999999999999999999999999999999999),(107, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (108, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(109, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(110, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal256_76_38_data_start_index = 103
    def test_cast_to_decimal128i_38_19_from_decimal256_76_38_data_end_index = 111
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_38_0 values (111, 10),(112, 99999999999999999999999999999999999998),(113, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_38_0_data_start_index = 111
    def test_cast_to_decimal128i_38_37_from_decimal256_38_0_data_end_index = 114
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_38_1 values (114, 10.9),(115, 9999999999999999999999999999999999998.9),(116, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_38_1_data_start_index = 114
    def test_cast_to_decimal128i_38_37_from_decimal256_38_1_data_end_index = 117
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_38_19 values (117, 10.9999999999999999999),(118, 9999999999999999998.9999999999999999999),(119, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_38_19_data_start_index = 117
    def test_cast_to_decimal128i_38_37_from_decimal256_38_19_data_end_index = 120
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_39_0 values (120, 10),(121, 999999999999999999999999999999999999998),(122, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_39_0_data_start_index = 120
    def test_cast_to_decimal128i_38_37_from_decimal256_39_0_data_end_index = 123
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_39_1 values (123, 10.9),(124, 99999999999999999999999999999999999998.9),(125, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_39_1_data_start_index = 123
    def test_cast_to_decimal128i_38_37_from_decimal256_39_1_data_end_index = 126
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_39_19 values (126, 10.9999999999999999999),(127, 99999999999999999998.9999999999999999999),(128, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_39_19_data_start_index = 126
    def test_cast_to_decimal128i_38_37_from_decimal256_39_19_data_end_index = 129
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_39_38 values (129, 9.99999999999999999999999999999999999999),(130, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_39_38_data_start_index = 129
    def test_cast_to_decimal128i_38_37_from_decimal256_39_38_data_end_index = 131
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_75_0 values (131, 10),(132, 999999999999999999999999999999999999999999999999999999999999999999999999998),(133, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_75_0_data_start_index = 131
    def test_cast_to_decimal128i_38_37_from_decimal256_75_0_data_end_index = 134
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_75_1 values (134, 10.9),(135, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(136, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_75_1_data_start_index = 134
    def test_cast_to_decimal128i_38_37_from_decimal256_75_1_data_end_index = 137
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_75_37 values (137, 10.9999999999999999999999999999999999999),(138, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(139, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_75_37_data_start_index = 137
    def test_cast_to_decimal128i_38_37_from_decimal256_75_37_data_end_index = 140
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_75_74 values (140, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(141, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_75_74_data_start_index = 140
    def test_cast_to_decimal128i_38_37_from_decimal256_75_74_data_end_index = 142
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_76_0 values (142, 10),(143, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(144, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_76_0_data_start_index = 142
    def test_cast_to_decimal128i_38_37_from_decimal256_76_0_data_end_index = 145
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_76_1 values (145, 10.9),(146, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(147, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_76_1_data_start_index = 145
    def test_cast_to_decimal128i_38_37_from_decimal256_76_1_data_end_index = 148
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_76_38 values (148, 9.99999999999999999999999999999999999999),(149, 9.99999999999999999999999999999999999999),(150, 10.99999999999999999999999999999999999999),(151, 10.99999999999999999999999999999999999999),(152, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (153, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(154, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(155, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_76_38_data_start_index = 148
    def test_cast_to_decimal128i_38_37_from_decimal256_76_38_data_end_index = 156
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal256_76_75 values (156, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(157, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal256_76_75_data_start_index = 156
    def test_cast_to_decimal128i_38_37_from_decimal256_76_75_data_end_index = 158
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_38_0 values (158, 1),(159, 99999999999999999999999999999999999998),(160, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_38_0_data_start_index = 158
    def test_cast_to_decimal128i_38_38_from_decimal256_38_0_data_end_index = 161
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_38_1 values (161, 1.9),(162, 9999999999999999999999999999999999998.9),(163, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_38_1_data_start_index = 161
    def test_cast_to_decimal128i_38_38_from_decimal256_38_1_data_end_index = 164
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_38_19 values (164, 1.9999999999999999999),(165, 9999999999999999998.9999999999999999999),(166, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_38_19_data_start_index = 164
    def test_cast_to_decimal128i_38_38_from_decimal256_38_19_data_end_index = 167
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_38_37 values (167, 1.9999999999999999999999999999999999999),(168, 8.9999999999999999999999999999999999999),(169, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_38_37_data_start_index = 167
    def test_cast_to_decimal128i_38_38_from_decimal256_38_37_data_end_index = 170
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_39_0 values (170, 1),(171, 999999999999999999999999999999999999998),(172, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_39_0_data_start_index = 170
    def test_cast_to_decimal128i_38_38_from_decimal256_39_0_data_end_index = 173
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_39_1 values (173, 1.9),(174, 99999999999999999999999999999999999998.9),(175, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_39_1_data_start_index = 173
    def test_cast_to_decimal128i_38_38_from_decimal256_39_1_data_end_index = 176
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_39_19 values (176, 1.9999999999999999999),(177, 99999999999999999998.9999999999999999999),(178, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_39_19_data_start_index = 176
    def test_cast_to_decimal128i_38_38_from_decimal256_39_19_data_end_index = 179
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_39_38 values (179, 1.99999999999999999999999999999999999999),(180, 8.99999999999999999999999999999999999999),(181, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_39_38_data_start_index = 179
    def test_cast_to_decimal128i_38_38_from_decimal256_39_38_data_end_index = 182
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_39_39 values (182, 0.999999999999999999999999999999999999999),(183, 0.999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_39_39_data_start_index = 182
    def test_cast_to_decimal128i_38_38_from_decimal256_39_39_data_end_index = 184
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_39_39_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_39_39_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_39 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_75_0 values (184, 1),(185, 999999999999999999999999999999999999999999999999999999999999999999999999998),(186, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_75_0_data_start_index = 184
    def test_cast_to_decimal128i_38_38_from_decimal256_75_0_data_end_index = 187
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_75_1 values (187, 1.9),(188, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(189, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_75_1_data_start_index = 187
    def test_cast_to_decimal128i_38_38_from_decimal256_75_1_data_end_index = 190
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_75_37 values (190, 1.9999999999999999999999999999999999999),(191, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(192, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_75_37_data_start_index = 190
    def test_cast_to_decimal128i_38_38_from_decimal256_75_37_data_end_index = 193
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_75_74 values (193, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(194, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(195, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(196, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(197, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (198, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(199, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(200, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_75_74_data_start_index = 193
    def test_cast_to_decimal128i_38_38_from_decimal256_75_74_data_end_index = 201
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_75_75 values (201, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(202, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_75_75_data_start_index = 201
    def test_cast_to_decimal128i_38_38_from_decimal256_75_75_data_end_index = 203
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_76_0 values (203, 1),(204, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(205, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_76_0_data_start_index = 203
    def test_cast_to_decimal128i_38_38_from_decimal256_76_0_data_end_index = 206
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_76_1 values (206, 1.9),(207, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(208, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_76_1_data_start_index = 206
    def test_cast_to_decimal128i_38_38_from_decimal256_76_1_data_end_index = 209
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_76_38 values (209, 1.99999999999999999999999999999999999999),(210, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(211, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_76_38_data_start_index = 209
    def test_cast_to_decimal128i_38_38_from_decimal256_76_38_data_end_index = 212
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_76_75 values (212, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(213, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(214, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(215, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(216, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (217, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(218, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(219, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_76_75_data_start_index = 212
    def test_cast_to_decimal128i_38_38_from_decimal256_76_75_data_end_index = 220
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal256_76_76 values (220, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(221, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal256_76_76_data_start_index = 220
    def test_cast_to_decimal128i_38_38_from_decimal256_76_76_data_end_index = 222
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal256_76_76 order by 1;'

}