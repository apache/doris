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


suite("test_cast_to_decimal128i_19_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_37_0 values (0, 10000000000000000000),(1, 9999999999999999999999999999999999998),(2, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_37_0_data_start_index = 0
    def test_cast_to_decimal128i_19_0_from_decimal128i_37_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_37_1 values (3, 9999999999999999999.9),(4, 9999999999999999999.9),(5, 10000000000000000000.9),(6, 10000000000000000000.9),(7, 999999999999999999999999999999999998.9),
      (8, 999999999999999999999999999999999998.9),(9, 999999999999999999999999999999999999.9),(10, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_37_1_data_start_index = 3
    def test_cast_to_decimal128i_19_0_from_decimal128i_37_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_37_18 values (11, 9999999999999999999.999999999999999999),(12, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_37_18_data_start_index = 11
    def test_cast_to_decimal128i_19_0_from_decimal128i_37_18_data_end_index = 13
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_38_0 values (13, 10000000000000000000),(14, 99999999999999999999999999999999999998),(15, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_38_0_data_start_index = 13
    def test_cast_to_decimal128i_19_0_from_decimal128i_38_0_data_end_index = 16
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_38_1 values (16, 9999999999999999999.9),(17, 9999999999999999999.9),(18, 10000000000000000000.9),(19, 10000000000000000000.9),(20, 9999999999999999999999999999999999998.9),
      (21, 9999999999999999999999999999999999998.9),(22, 9999999999999999999999999999999999999.9),(23, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_38_1_data_start_index = 16
    def test_cast_to_decimal128i_19_0_from_decimal128i_38_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_0_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_decimal128i_38_19 values (24, 9999999999999999999.9999999999999999999),(25, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_0_from_decimal128i_38_19_data_start_index = 24
    def test_cast_to_decimal128i_19_0_from_decimal128i_38_19_data_end_index = 26
    for (int data_index = test_cast_to_decimal128i_19_0_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_0_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_19_0 values (26, 1000000000000000000),(27, 9999999999999999998),(28, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_19_0_data_start_index = 26
    def test_cast_to_decimal128i_19_1_from_decimal128i_19_0_data_end_index = 29
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_37_0 values (29, 1000000000000000000),(30, 9999999999999999999999999999999999998),(31, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_37_0_data_start_index = 29
    def test_cast_to_decimal128i_19_1_from_decimal128i_37_0_data_end_index = 32
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_37_1 values (32, 1000000000000000000.9),(33, 999999999999999999999999999999999998.9),(34, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_37_1_data_start_index = 32
    def test_cast_to_decimal128i_19_1_from_decimal128i_37_1_data_end_index = 35
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_37_18 values (35, 999999999999999999.999999999999999999),(36, 999999999999999999.999999999999999999),(37, 1000000000000000000.999999999999999999),(38, 1000000000000000000.999999999999999999),(39, 9999999999999999998.999999999999999999),
      (40, 9999999999999999998.999999999999999999),(41, 9999999999999999999.999999999999999999),(42, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_37_18_data_start_index = 35
    def test_cast_to_decimal128i_19_1_from_decimal128i_37_18_data_end_index = 43
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_38_0 values (43, 1000000000000000000),(44, 99999999999999999999999999999999999998),(45, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_38_0_data_start_index = 43
    def test_cast_to_decimal128i_19_1_from_decimal128i_38_0_data_end_index = 46
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_38_1 values (46, 1000000000000000000.9),(47, 9999999999999999999999999999999999998.9),(48, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_38_1_data_start_index = 46
    def test_cast_to_decimal128i_19_1_from_decimal128i_38_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_1_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_1_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_1_from_decimal128i_38_19 values (49, 999999999999999999.9999999999999999999),(50, 999999999999999999.9999999999999999999),(51, 1000000000000000000.9999999999999999999),(52, 1000000000000000000.9999999999999999999),(53, 9999999999999999998.9999999999999999999),
      (54, 9999999999999999998.9999999999999999999),(55, 9999999999999999999.9999999999999999999),(56, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_1_from_decimal128i_38_19_data_start_index = 49
    def test_cast_to_decimal128i_19_1_from_decimal128i_38_19_data_end_index = 57
    for (int data_index = test_cast_to_decimal128i_19_1_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_1_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(19, 1)) from test_cast_to_decimal128i_19_1_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_0 values (57, 10000000000),(58, 9999999999999999998),(59, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_19_0_data_start_index = 57
    def test_cast_to_decimal128i_19_9_from_decimal128i_19_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_1 values (60, 10000000000.9),(61, 999999999999999998.9),(62, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_19_1_data_start_index = 60
    def test_cast_to_decimal128i_19_9_from_decimal128i_19_1_data_end_index = 63
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_0 values (63, 10000000000),(64, 9999999999999999999999999999999999998),(65, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_37_0_data_start_index = 63
    def test_cast_to_decimal128i_19_9_from_decimal128i_37_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_1 values (66, 10000000000.9),(67, 999999999999999999999999999999999998.9),(68, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_37_1_data_start_index = 66
    def test_cast_to_decimal128i_19_9_from_decimal128i_37_1_data_end_index = 69
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_18 values (69, 9999999999.999999999999999999),(70, 9999999999.999999999999999999),(71, 10000000000.999999999999999999),(72, 10000000000.999999999999999999),(73, 9999999999999999998.999999999999999999),
      (74, 9999999999999999998.999999999999999999),(75, 9999999999999999999.999999999999999999),(76, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_37_18_data_start_index = 69
    def test_cast_to_decimal128i_19_9_from_decimal128i_37_18_data_end_index = 77
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_0 values (77, 10000000000),(78, 99999999999999999999999999999999999998),(79, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_38_0_data_start_index = 77
    def test_cast_to_decimal128i_19_9_from_decimal128i_38_0_data_end_index = 80
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_1 values (80, 10000000000.9),(81, 9999999999999999999999999999999999998.9),(82, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_38_1_data_start_index = 80
    def test_cast_to_decimal128i_19_9_from_decimal128i_38_1_data_end_index = 83
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_19 values (83, 9999999999.9999999999999999999),(84, 9999999999.9999999999999999999),(85, 10000000000.9999999999999999999),(86, 10000000000.9999999999999999999),(87, 9999999999999999998.9999999999999999999),
      (88, 9999999999999999998.9999999999999999999),(89, 9999999999999999999.9999999999999999999),(90, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_9_from_decimal128i_38_19_data_start_index = 83
    def test_cast_to_decimal128i_19_9_from_decimal128i_38_19_data_end_index = 91
    for (int data_index = test_cast_to_decimal128i_19_9_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_9_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_19_0 values (91, 10),(92, 9999999999999999998),(93, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_19_0_data_start_index = 91
    def test_cast_to_decimal128i_19_18_from_decimal128i_19_0_data_end_index = 94
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_19_1 values (94, 10.9),(95, 999999999999999998.9),(96, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_19_1_data_start_index = 94
    def test_cast_to_decimal128i_19_18_from_decimal128i_19_1_data_end_index = 97
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_19_9 values (97, 10.999999999),(98, 9999999998.999999999),(99, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_19_9_data_start_index = 97
    def test_cast_to_decimal128i_19_18_from_decimal128i_19_9_data_end_index = 100
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_37_0 values (100, 10),(101, 9999999999999999999999999999999999998),(102, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_37_0_data_start_index = 100
    def test_cast_to_decimal128i_19_18_from_decimal128i_37_0_data_end_index = 103
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_37_1 values (103, 10.9),(104, 999999999999999999999999999999999998.9),(105, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_37_1_data_start_index = 103
    def test_cast_to_decimal128i_19_18_from_decimal128i_37_1_data_end_index = 106
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_37_18 values (106, 10.999999999999999999),(107, 9999999999999999998.999999999999999999),(108, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_37_18_data_start_index = 106
    def test_cast_to_decimal128i_19_18_from_decimal128i_37_18_data_end_index = 109
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_37_36 values (109, 9.999999999999999999999999999999999999),(110, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_37_36_data_start_index = 109
    def test_cast_to_decimal128i_19_18_from_decimal128i_37_36_data_end_index = 111
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_38_0 values (111, 10),(112, 99999999999999999999999999999999999998),(113, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_38_0_data_start_index = 111
    def test_cast_to_decimal128i_19_18_from_decimal128i_38_0_data_end_index = 114
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_38_1 values (114, 10.9),(115, 9999999999999999999999999999999999998.9),(116, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_38_1_data_start_index = 114
    def test_cast_to_decimal128i_19_18_from_decimal128i_38_1_data_end_index = 117
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_38_19 values (117, 9.9999999999999999999),(118, 9.9999999999999999999),(119, 10.9999999999999999999),(120, 10.9999999999999999999),(121, 9999999999999999998.9999999999999999999),
      (122, 9999999999999999998.9999999999999999999),(123, 9999999999999999999.9999999999999999999),(124, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_38_19_data_start_index = 117
    def test_cast_to_decimal128i_19_18_from_decimal128i_38_19_data_end_index = 125
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_19_18_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_decimal128i_38_37 values (125, 9.9999999999999999999999999999999999999),(126, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_18_from_decimal128i_38_37_data_start_index = 125
    def test_cast_to_decimal128i_19_18_from_decimal128i_38_37_data_end_index = 127
    for (int data_index = test_cast_to_decimal128i_19_18_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal128i_19_18_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_19_0 values (127, 1),(128, 9999999999999999998),(129, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_19_0_data_start_index = 127
    def test_cast_to_decimal128i_19_19_from_decimal128i_19_0_data_end_index = 130
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_19_1 values (130, 1.9),(131, 999999999999999998.9),(132, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_19_1_data_start_index = 130
    def test_cast_to_decimal128i_19_19_from_decimal128i_19_1_data_end_index = 133
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_19_9 values (133, 1.999999999),(134, 9999999998.999999999),(135, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_19_9_data_start_index = 133
    def test_cast_to_decimal128i_19_19_from_decimal128i_19_9_data_end_index = 136
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_19_18 values (136, 1.999999999999999999),(137, 8.999999999999999999),(138, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_19_18_data_start_index = 136
    def test_cast_to_decimal128i_19_19_from_decimal128i_19_18_data_end_index = 139
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_37_0 values (139, 1),(140, 9999999999999999999999999999999999998),(141, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_37_0_data_start_index = 139
    def test_cast_to_decimal128i_19_19_from_decimal128i_37_0_data_end_index = 142
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_37_1 values (142, 1.9),(143, 999999999999999999999999999999999998.9),(144, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_37_1_data_start_index = 142
    def test_cast_to_decimal128i_19_19_from_decimal128i_37_1_data_end_index = 145
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_37_18 values (145, 1.999999999999999999),(146, 9999999999999999998.999999999999999999),(147, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_37_18_data_start_index = 145
    def test_cast_to_decimal128i_19_19_from_decimal128i_37_18_data_end_index = 148
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_37_36 values (148, 0.999999999999999999999999999999999999),(149, 0.999999999999999999999999999999999999),(150, 1.999999999999999999999999999999999999),(151, 1.999999999999999999999999999999999999),(152, 8.999999999999999999999999999999999999),
      (153, 8.999999999999999999999999999999999999),(154, 9.999999999999999999999999999999999999),(155, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_37_36_data_start_index = 148
    def test_cast_to_decimal128i_19_19_from_decimal128i_37_36_data_end_index = 156
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_37_37 values (156, 0.9999999999999999999999999999999999999),(157, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_37_37_data_start_index = 156
    def test_cast_to_decimal128i_19_19_from_decimal128i_37_37_data_end_index = 158
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_37_37_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_38_0 values (158, 1),(159, 99999999999999999999999999999999999998),(160, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_38_0_data_start_index = 158
    def test_cast_to_decimal128i_19_19_from_decimal128i_38_0_data_end_index = 161
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_38_1 values (161, 1.9),(162, 9999999999999999999999999999999999998.9),(163, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_38_1_data_start_index = 161
    def test_cast_to_decimal128i_19_19_from_decimal128i_38_1_data_end_index = 164
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_38_19 values (164, 1.9999999999999999999),(165, 9999999999999999998.9999999999999999999),(166, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_38_19_data_start_index = 164
    def test_cast_to_decimal128i_19_19_from_decimal128i_38_19_data_end_index = 167
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_38_37 values (167, 0.9999999999999999999999999999999999999),(168, 0.9999999999999999999999999999999999999),(169, 1.9999999999999999999999999999999999999),(170, 1.9999999999999999999999999999999999999),(171, 8.9999999999999999999999999999999999999),
      (172, 8.9999999999999999999999999999999999999),(173, 9.9999999999999999999999999999999999999),(174, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_38_37_data_start_index = 167
    def test_cast_to_decimal128i_19_19_from_decimal128i_38_37_data_end_index = 175
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_19_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal128i_19_19_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_19_from_decimal128i_38_38 values (175, 0.99999999999999999999999999999999999999),(176, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_19_19_from_decimal128i_38_38_data_start_index = 175
    def test_cast_to_decimal128i_19_19_from_decimal128i_38_38_data_end_index = 177
    for (int data_index = test_cast_to_decimal128i_19_19_from_decimal128i_38_38_data_start_index; data_index < test_cast_to_decimal128i_19_19_from_decimal128i_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(19, 19)) from test_cast_to_decimal128i_19_19_from_decimal128i_38_38 order by 1;'

}