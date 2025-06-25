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


suite("test_cast_to_decimal128i_37_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_38_0 values (0, 10000000000000000000000000000000000000),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_38_0_data_start_index = 0
    def test_cast_to_decimal128i_37_0_from_decimal256_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_38_1 values (3, 9999999999999999999999999999999999999.9),(4, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_38_1_data_start_index = 3
    def test_cast_to_decimal128i_37_0_from_decimal256_38_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_39_0 values (5, 10000000000000000000000000000000000000),(6, 999999999999999999999999999999999999998),(7, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_39_0_data_start_index = 5
    def test_cast_to_decimal128i_37_0_from_decimal256_39_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_39_1 values (8, 9999999999999999999999999999999999999.9),(9, 9999999999999999999999999999999999999.9),(10, 10000000000000000000000000000000000000.9),(11, 10000000000000000000000000000000000000.9),(12, 99999999999999999999999999999999999998.9),
      (13, 99999999999999999999999999999999999998.9),(14, 99999999999999999999999999999999999999.9),(15, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_39_1_data_start_index = 8
    def test_cast_to_decimal128i_37_0_from_decimal256_39_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_75_0 values (16, 10000000000000000000000000000000000000),(17, 999999999999999999999999999999999999999999999999999999999999999999999999998),(18, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_75_0_data_start_index = 16
    def test_cast_to_decimal128i_37_0_from_decimal256_75_0_data_end_index = 19
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_75_1 values (19, 9999999999999999999999999999999999999.9),(20, 9999999999999999999999999999999999999.9),(21, 10000000000000000000000000000000000000.9),(22, 10000000000000000000000000000000000000.9),(23, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (24, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(25, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(26, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_75_1_data_start_index = 19
    def test_cast_to_decimal128i_37_0_from_decimal256_75_1_data_end_index = 27
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_75_37 values (27, 9999999999999999999999999999999999999.9999999999999999999999999999999999999),(28, 9999999999999999999999999999999999999.9999999999999999999999999999999999999),(29, 10000000000000000000000000000000000000.9999999999999999999999999999999999999),(30, 10000000000000000000000000000000000000.9999999999999999999999999999999999999),(31, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (32, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(33, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(34, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_75_37_data_start_index = 27
    def test_cast_to_decimal128i_37_0_from_decimal256_75_37_data_end_index = 35
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_76_0 values (35, 10000000000000000000000000000000000000),(36, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(37, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_76_0_data_start_index = 35
    def test_cast_to_decimal128i_37_0_from_decimal256_76_0_data_end_index = 38
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_76_1 values (38, 9999999999999999999999999999999999999.9),(39, 9999999999999999999999999999999999999.9),(40, 10000000000000000000000000000000000000.9),(41, 10000000000000000000000000000000000000.9),(42, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (43, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(44, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(45, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_76_1_data_start_index = 38
    def test_cast_to_decimal128i_37_0_from_decimal256_76_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal256_76_38 values (46, 9999999999999999999999999999999999999.99999999999999999999999999999999999999),(47, 9999999999999999999999999999999999999.99999999999999999999999999999999999999),(48, 10000000000000000000000000000000000000.99999999999999999999999999999999999999),(49, 10000000000000000000000000000000000000.99999999999999999999999999999999999999),(50, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (51, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(52, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(53, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal256_76_38_data_start_index = 46
    def test_cast_to_decimal128i_37_0_from_decimal256_76_38_data_end_index = 54
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_38_0 values (54, 1000000000000000000000000000000000000),(55, 99999999999999999999999999999999999998),(56, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_38_0_data_start_index = 54
    def test_cast_to_decimal128i_37_1_from_decimal256_38_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_38_1 values (57, 1000000000000000000000000000000000000.9),(58, 9999999999999999999999999999999999998.9),(59, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_38_1_data_start_index = 57
    def test_cast_to_decimal128i_37_1_from_decimal256_38_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_39_0 values (60, 1000000000000000000000000000000000000),(61, 999999999999999999999999999999999999998),(62, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_39_0_data_start_index = 60
    def test_cast_to_decimal128i_37_1_from_decimal256_39_0_data_end_index = 63
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_39_1 values (63, 1000000000000000000000000000000000000.9),(64, 99999999999999999999999999999999999998.9),(65, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_39_1_data_start_index = 63
    def test_cast_to_decimal128i_37_1_from_decimal256_39_1_data_end_index = 66
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_75_0 values (66, 1000000000000000000000000000000000000),(67, 999999999999999999999999999999999999999999999999999999999999999999999999998),(68, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_75_0_data_start_index = 66
    def test_cast_to_decimal128i_37_1_from_decimal256_75_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_75_1 values (69, 1000000000000000000000000000000000000.9),(70, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(71, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_75_1_data_start_index = 69
    def test_cast_to_decimal128i_37_1_from_decimal256_75_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_75_37 values (72, 999999999999999999999999999999999999.9999999999999999999999999999999999999),(73, 999999999999999999999999999999999999.9999999999999999999999999999999999999),(74, 1000000000000000000000000000000000000.9999999999999999999999999999999999999),(75, 1000000000000000000000000000000000000.9999999999999999999999999999999999999),(76, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (77, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(78, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(79, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_75_37_data_start_index = 72
    def test_cast_to_decimal128i_37_1_from_decimal256_75_37_data_end_index = 80
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_76_0 values (80, 1000000000000000000000000000000000000),(81, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(82, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_76_0_data_start_index = 80
    def test_cast_to_decimal128i_37_1_from_decimal256_76_0_data_end_index = 83
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_76_1 values (83, 1000000000000000000000000000000000000.9),(84, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(85, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_76_1_data_start_index = 83
    def test_cast_to_decimal128i_37_1_from_decimal256_76_1_data_end_index = 86
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal256_76_38 values (86, 999999999999999999999999999999999999.99999999999999999999999999999999999999),(87, 999999999999999999999999999999999999.99999999999999999999999999999999999999),(88, 1000000000000000000000000000000000000.99999999999999999999999999999999999999),(89, 1000000000000000000000000000000000000.99999999999999999999999999999999999999),(90, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (91, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(92, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(93, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal256_76_38_data_start_index = 86
    def test_cast_to_decimal128i_37_1_from_decimal256_76_38_data_end_index = 94
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_38_0 values (94, 10000000000000000000),(95, 99999999999999999999999999999999999998),(96, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_38_0_data_start_index = 94
    def test_cast_to_decimal128i_37_18_from_decimal256_38_0_data_end_index = 97
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_38_1 values (97, 10000000000000000000.9),(98, 9999999999999999999999999999999999998.9),(99, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_38_1_data_start_index = 97
    def test_cast_to_decimal128i_37_18_from_decimal256_38_1_data_end_index = 100
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_38_19 values (100, 9999999999999999999.9999999999999999999),(101, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_38_19_data_start_index = 100
    def test_cast_to_decimal128i_37_18_from_decimal256_38_19_data_end_index = 102
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_39_0 values (102, 10000000000000000000),(103, 999999999999999999999999999999999999998),(104, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_39_0_data_start_index = 102
    def test_cast_to_decimal128i_37_18_from_decimal256_39_0_data_end_index = 105
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_39_1 values (105, 10000000000000000000.9),(106, 99999999999999999999999999999999999998.9),(107, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_39_1_data_start_index = 105
    def test_cast_to_decimal128i_37_18_from_decimal256_39_1_data_end_index = 108
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_39_19 values (108, 9999999999999999999.9999999999999999999),(109, 9999999999999999999.9999999999999999999),(110, 10000000000000000000.9999999999999999999),(111, 10000000000000000000.9999999999999999999),(112, 99999999999999999998.9999999999999999999),
      (113, 99999999999999999998.9999999999999999999),(114, 99999999999999999999.9999999999999999999),(115, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_39_19_data_start_index = 108
    def test_cast_to_decimal128i_37_18_from_decimal256_39_19_data_end_index = 116
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_75_0 values (116, 10000000000000000000),(117, 999999999999999999999999999999999999999999999999999999999999999999999999998),(118, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_75_0_data_start_index = 116
    def test_cast_to_decimal128i_37_18_from_decimal256_75_0_data_end_index = 119
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_75_1 values (119, 10000000000000000000.9),(120, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(121, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_75_1_data_start_index = 119
    def test_cast_to_decimal128i_37_18_from_decimal256_75_1_data_end_index = 122
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_75_37 values (122, 9999999999999999999.9999999999999999999999999999999999999),(123, 9999999999999999999.9999999999999999999999999999999999999),(124, 10000000000000000000.9999999999999999999999999999999999999),(125, 10000000000000000000.9999999999999999999999999999999999999),(126, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (127, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(128, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(129, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_75_37_data_start_index = 122
    def test_cast_to_decimal128i_37_18_from_decimal256_75_37_data_end_index = 130
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_76_0 values (130, 10000000000000000000),(131, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(132, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_76_0_data_start_index = 130
    def test_cast_to_decimal128i_37_18_from_decimal256_76_0_data_end_index = 133
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_76_1 values (133, 10000000000000000000.9),(134, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(135, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_76_1_data_start_index = 133
    def test_cast_to_decimal128i_37_18_from_decimal256_76_1_data_end_index = 136
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal256_76_38 values (136, 9999999999999999999.99999999999999999999999999999999999999),(137, 9999999999999999999.99999999999999999999999999999999999999),(138, 10000000000000000000.99999999999999999999999999999999999999),(139, 10000000000000000000.99999999999999999999999999999999999999),(140, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (141, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(142, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(143, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal256_76_38_data_start_index = 136
    def test_cast_to_decimal128i_37_18_from_decimal256_76_38_data_end_index = 144
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_38_0 values (144, 10),(145, 99999999999999999999999999999999999998),(146, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_38_0_data_start_index = 144
    def test_cast_to_decimal128i_37_36_from_decimal256_38_0_data_end_index = 147
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_38_1 values (147, 10.9),(148, 9999999999999999999999999999999999998.9),(149, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_38_1_data_start_index = 147
    def test_cast_to_decimal128i_37_36_from_decimal256_38_1_data_end_index = 150
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_38_19 values (150, 10.9999999999999999999),(151, 9999999999999999998.9999999999999999999),(152, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_38_19_data_start_index = 150
    def test_cast_to_decimal128i_37_36_from_decimal256_38_19_data_end_index = 153
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_38_37 values (153, 9.9999999999999999999999999999999999999),(154, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_38_37_data_start_index = 153
    def test_cast_to_decimal128i_37_36_from_decimal256_38_37_data_end_index = 155
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_39_0 values (155, 10),(156, 999999999999999999999999999999999999998),(157, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_39_0_data_start_index = 155
    def test_cast_to_decimal128i_37_36_from_decimal256_39_0_data_end_index = 158
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_39_1 values (158, 10.9),(159, 99999999999999999999999999999999999998.9),(160, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_39_1_data_start_index = 158
    def test_cast_to_decimal128i_37_36_from_decimal256_39_1_data_end_index = 161
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_39_19 values (161, 10.9999999999999999999),(162, 99999999999999999998.9999999999999999999),(163, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_39_19_data_start_index = 161
    def test_cast_to_decimal128i_37_36_from_decimal256_39_19_data_end_index = 164
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_39_38 values (164, 9.99999999999999999999999999999999999999),(165, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_39_38_data_start_index = 164
    def test_cast_to_decimal128i_37_36_from_decimal256_39_38_data_end_index = 166
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_75_0 values (166, 10),(167, 999999999999999999999999999999999999999999999999999999999999999999999999998),(168, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_75_0_data_start_index = 166
    def test_cast_to_decimal128i_37_36_from_decimal256_75_0_data_end_index = 169
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_75_1 values (169, 10.9),(170, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(171, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_75_1_data_start_index = 169
    def test_cast_to_decimal128i_37_36_from_decimal256_75_1_data_end_index = 172
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_75_37 values (172, 9.9999999999999999999999999999999999999),(173, 9.9999999999999999999999999999999999999),(174, 10.9999999999999999999999999999999999999),(175, 10.9999999999999999999999999999999999999),(176, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (177, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(178, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(179, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_75_37_data_start_index = 172
    def test_cast_to_decimal128i_37_36_from_decimal256_75_37_data_end_index = 180
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_75_74 values (180, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(181, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_75_74_data_start_index = 180
    def test_cast_to_decimal128i_37_36_from_decimal256_75_74_data_end_index = 182
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_76_0 values (182, 10),(183, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(184, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_76_0_data_start_index = 182
    def test_cast_to_decimal128i_37_36_from_decimal256_76_0_data_end_index = 185
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_76_1 values (185, 10.9),(186, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(187, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_76_1_data_start_index = 185
    def test_cast_to_decimal128i_37_36_from_decimal256_76_1_data_end_index = 188
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_76_38 values (188, 9.99999999999999999999999999999999999999),(189, 9.99999999999999999999999999999999999999),(190, 10.99999999999999999999999999999999999999),(191, 10.99999999999999999999999999999999999999),(192, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (193, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(194, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(195, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_76_38_data_start_index = 188
    def test_cast_to_decimal128i_37_36_from_decimal256_76_38_data_end_index = 196
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal256_76_75 values (196, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(197, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal256_76_75_data_start_index = 196
    def test_cast_to_decimal128i_37_36_from_decimal256_76_75_data_end_index = 198
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_38_0 values (198, 1),(199, 99999999999999999999999999999999999998),(200, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_38_0_data_start_index = 198
    def test_cast_to_decimal128i_37_37_from_decimal256_38_0_data_end_index = 201
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_38_1 values (201, 1.9),(202, 9999999999999999999999999999999999998.9),(203, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_38_1_data_start_index = 201
    def test_cast_to_decimal128i_37_37_from_decimal256_38_1_data_end_index = 204
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_38_19 values (204, 1.9999999999999999999),(205, 9999999999999999998.9999999999999999999),(206, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_38_19_data_start_index = 204
    def test_cast_to_decimal128i_37_37_from_decimal256_38_19_data_end_index = 207
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_38_37 values (207, 1.9999999999999999999999999999999999999),(208, 8.9999999999999999999999999999999999999),(209, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_38_37_data_start_index = 207
    def test_cast_to_decimal128i_37_37_from_decimal256_38_37_data_end_index = 210
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_38_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_38_38 values (210, 0.99999999999999999999999999999999999999),(211, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_38_38_data_start_index = 210
    def test_cast_to_decimal128i_37_37_from_decimal256_38_38_data_end_index = 212
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_38_38_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_84 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_38_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_0 values (212, 1),(213, 999999999999999999999999999999999999998),(214, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_39_0_data_start_index = 212
    def test_cast_to_decimal128i_37_37_from_decimal256_39_0_data_end_index = 215
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_1 values (215, 1.9),(216, 99999999999999999999999999999999999998.9),(217, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_39_1_data_start_index = 215
    def test_cast_to_decimal128i_37_37_from_decimal256_39_1_data_end_index = 218
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_19 values (218, 1.9999999999999999999),(219, 99999999999999999998.9999999999999999999),(220, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_39_19_data_start_index = 218
    def test_cast_to_decimal128i_37_37_from_decimal256_39_19_data_end_index = 221
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_38 values (221, 0.99999999999999999999999999999999999999),(222, 0.99999999999999999999999999999999999999),(223, 1.99999999999999999999999999999999999999),(224, 1.99999999999999999999999999999999999999),(225, 8.99999999999999999999999999999999999999),
      (226, 8.99999999999999999999999999999999999999),(227, 9.99999999999999999999999999999999999999),(228, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_39_38_data_start_index = 221
    def test_cast_to_decimal128i_37_37_from_decimal256_39_38_data_end_index = 229
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_39 values (229, 0.999999999999999999999999999999999999999),(230, 0.999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_39_39_data_start_index = 229
    def test_cast_to_decimal128i_37_37_from_decimal256_39_39_data_end_index = 231
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_39_39_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_39_39_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_39 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_89 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_0 values (231, 1),(232, 999999999999999999999999999999999999999999999999999999999999999999999999998),(233, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_75_0_data_start_index = 231
    def test_cast_to_decimal128i_37_37_from_decimal256_75_0_data_end_index = 234
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_1 values (234, 1.9),(235, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(236, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_75_1_data_start_index = 234
    def test_cast_to_decimal128i_37_37_from_decimal256_75_1_data_end_index = 237
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_37 values (237, 1.9999999999999999999999999999999999999),(238, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(239, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_75_37_data_start_index = 237
    def test_cast_to_decimal128i_37_37_from_decimal256_75_37_data_end_index = 240
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_74 values (240, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(241, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(242, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(243, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(244, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (245, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(246, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(247, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_75_74_data_start_index = 240
    def test_cast_to_decimal128i_37_37_from_decimal256_75_74_data_end_index = 248
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_75 values (248, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(249, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_75_75_data_start_index = 248
    def test_cast_to_decimal128i_37_37_from_decimal256_75_75_data_end_index = 250
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_0 values (250, 1),(251, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(252, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_76_0_data_start_index = 250
    def test_cast_to_decimal128i_37_37_from_decimal256_76_0_data_end_index = 253
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_1 values (253, 1.9),(254, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(255, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_76_1_data_start_index = 253
    def test_cast_to_decimal128i_37_37_from_decimal256_76_1_data_end_index = 256
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_38 values (256, 0.99999999999999999999999999999999999999),(257, 0.99999999999999999999999999999999999999),(258, 1.99999999999999999999999999999999999999),(259, 1.99999999999999999999999999999999999999),(260, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (261, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(262, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(263, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_76_38_data_start_index = 256
    def test_cast_to_decimal128i_37_37_from_decimal256_76_38_data_end_index = 264
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_75 values (264, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(265, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(266, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(267, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(268, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (269, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(270, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(271, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_76_75_data_start_index = 264
    def test_cast_to_decimal128i_37_37_from_decimal256_76_75_data_end_index = 272
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_76 values (272, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(273, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal256_76_76_data_start_index = 272
    def test_cast_to_decimal128i_37_37_from_decimal256_76_76_data_end_index = 274
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_76 order by 1;'

}