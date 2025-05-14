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


suite("test_cast_to_decimal32_1_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_38_0 values (0, 10),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_38_0_data_start_index = 0
    def test_cast_to_decimal32_1_0_from_decimal256_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_38_1 values (3, 9.9),(4, 9.9),(5, 10.9),(6, 10.9),(7, 9999999999999999999999999999999999998.9),
      (8, 9999999999999999999999999999999999998.9),(9, 9999999999999999999999999999999999999.9),(10, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_38_1_data_start_index = 3
    def test_cast_to_decimal32_1_0_from_decimal256_38_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_38_19 values (11, 9.9999999999999999999),(12, 9.9999999999999999999),(13, 10.9999999999999999999),(14, 10.9999999999999999999),(15, 9999999999999999998.9999999999999999999),
      (16, 9999999999999999998.9999999999999999999),(17, 9999999999999999999.9999999999999999999),(18, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_38_19_data_start_index = 11
    def test_cast_to_decimal32_1_0_from_decimal256_38_19_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_38_37 values (19, 9.9999999999999999999999999999999999999),(20, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_38_37_data_start_index = 19
    def test_cast_to_decimal32_1_0_from_decimal256_38_37_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_39_0 values (21, 10),(22, 999999999999999999999999999999999999998),(23, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_39_0_data_start_index = 21
    def test_cast_to_decimal32_1_0_from_decimal256_39_0_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_39_1 values (24, 9.9),(25, 9.9),(26, 10.9),(27, 10.9),(28, 99999999999999999999999999999999999998.9),
      (29, 99999999999999999999999999999999999998.9),(30, 99999999999999999999999999999999999999.9),(31, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_39_1_data_start_index = 24
    def test_cast_to_decimal32_1_0_from_decimal256_39_1_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_39_19 values (32, 9.9999999999999999999),(33, 9.9999999999999999999),(34, 10.9999999999999999999),(35, 10.9999999999999999999),(36, 99999999999999999998.9999999999999999999),
      (37, 99999999999999999998.9999999999999999999),(38, 99999999999999999999.9999999999999999999),(39, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_39_19_data_start_index = 32
    def test_cast_to_decimal32_1_0_from_decimal256_39_19_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_39_38 values (40, 9.99999999999999999999999999999999999999),(41, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_39_38_data_start_index = 40
    def test_cast_to_decimal32_1_0_from_decimal256_39_38_data_end_index = 42
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_75_0 values (42, 10),(43, 999999999999999999999999999999999999999999999999999999999999999999999999998),(44, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_75_0_data_start_index = 42
    def test_cast_to_decimal32_1_0_from_decimal256_75_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_75_1 values (45, 9.9),(46, 9.9),(47, 10.9),(48, 10.9),(49, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (50, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(51, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(52, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_75_1_data_start_index = 45
    def test_cast_to_decimal32_1_0_from_decimal256_75_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_75_37 values (53, 9.9999999999999999999999999999999999999),(54, 9.9999999999999999999999999999999999999),(55, 10.9999999999999999999999999999999999999),(56, 10.9999999999999999999999999999999999999),(57, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (58, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(59, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(60, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_75_37_data_start_index = 53
    def test_cast_to_decimal32_1_0_from_decimal256_75_37_data_end_index = 61
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_75_74 values (61, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(62, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_75_74_data_start_index = 61
    def test_cast_to_decimal32_1_0_from_decimal256_75_74_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_76_0 values (63, 10),(64, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(65, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_76_0_data_start_index = 63
    def test_cast_to_decimal32_1_0_from_decimal256_76_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_76_1 values (66, 9.9),(67, 9.9),(68, 10.9),(69, 10.9),(70, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (71, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(72, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(73, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_76_1_data_start_index = 66
    def test_cast_to_decimal32_1_0_from_decimal256_76_1_data_end_index = 74
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_76_38 values (74, 9.99999999999999999999999999999999999999),(75, 9.99999999999999999999999999999999999999),(76, 10.99999999999999999999999999999999999999),(77, 10.99999999999999999999999999999999999999),(78, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (79, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(80, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(81, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_76_38_data_start_index = 74
    def test_cast_to_decimal32_1_0_from_decimal256_76_38_data_end_index = 82
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal256_76_75 values (82, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(83, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal256_76_75_data_start_index = 82
    def test_cast_to_decimal32_1_0_from_decimal256_76_75_data_end_index = 84
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_38_0 values (84, 1),(85, 99999999999999999999999999999999999998),(86, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_38_0_data_start_index = 84
    def test_cast_to_decimal32_1_1_from_decimal256_38_0_data_end_index = 87
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_38_1 values (87, 1.9),(88, 9999999999999999999999999999999999998.9),(89, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_38_1_data_start_index = 87
    def test_cast_to_decimal32_1_1_from_decimal256_38_1_data_end_index = 90
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_38_19 values (90, 0.9999999999999999999),(91, 0.9999999999999999999),(92, 1.9999999999999999999),(93, 1.9999999999999999999),(94, 9999999999999999998.9999999999999999999),
      (95, 9999999999999999998.9999999999999999999),(96, 9999999999999999999.9999999999999999999),(97, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_38_19_data_start_index = 90
    def test_cast_to_decimal32_1_1_from_decimal256_38_19_data_end_index = 98
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_38_37 values (98, 0.9999999999999999999999999999999999999),(99, 0.9999999999999999999999999999999999999),(100, 1.9999999999999999999999999999999999999),(101, 1.9999999999999999999999999999999999999),(102, 8.9999999999999999999999999999999999999),
      (103, 8.9999999999999999999999999999999999999),(104, 9.9999999999999999999999999999999999999),(105, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_38_37_data_start_index = 98
    def test_cast_to_decimal32_1_1_from_decimal256_38_37_data_end_index = 106
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_38_38;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_38_38 values (106, 0.99999999999999999999999999999999999999),(107, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_38_38_data_start_index = 106
    def test_cast_to_decimal32_1_1_from_decimal256_38_38_data_end_index = 108
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_38_38_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_38_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_39_0 values (108, 1),(109, 999999999999999999999999999999999999998),(110, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_39_0_data_start_index = 108
    def test_cast_to_decimal32_1_1_from_decimal256_39_0_data_end_index = 111
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_39_1 values (111, 1.9),(112, 99999999999999999999999999999999999998.9),(113, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_39_1_data_start_index = 111
    def test_cast_to_decimal32_1_1_from_decimal256_39_1_data_end_index = 114
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_39_19 values (114, 0.9999999999999999999),(115, 0.9999999999999999999),(116, 1.9999999999999999999),(117, 1.9999999999999999999),(118, 99999999999999999998.9999999999999999999),
      (119, 99999999999999999998.9999999999999999999),(120, 99999999999999999999.9999999999999999999),(121, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_39_19_data_start_index = 114
    def test_cast_to_decimal32_1_1_from_decimal256_39_19_data_end_index = 122
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_39_38 values (122, 0.99999999999999999999999999999999999999),(123, 0.99999999999999999999999999999999999999),(124, 1.99999999999999999999999999999999999999),(125, 1.99999999999999999999999999999999999999),(126, 8.99999999999999999999999999999999999999),
      (127, 8.99999999999999999999999999999999999999),(128, 9.99999999999999999999999999999999999999),(129, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_39_38_data_start_index = 122
    def test_cast_to_decimal32_1_1_from_decimal256_39_38_data_end_index = 130
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_39_39 values (130, 0.999999999999999999999999999999999999999),(131, 0.999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_39_39_data_start_index = 130
    def test_cast_to_decimal32_1_1_from_decimal256_39_39_data_end_index = 132
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_39_39_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_39_39_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_39 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_75_0 values (132, 1),(133, 999999999999999999999999999999999999999999999999999999999999999999999999998),(134, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_75_0_data_start_index = 132
    def test_cast_to_decimal32_1_1_from_decimal256_75_0_data_end_index = 135
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_75_1 values (135, 1.9),(136, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(137, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_75_1_data_start_index = 135
    def test_cast_to_decimal32_1_1_from_decimal256_75_1_data_end_index = 138
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_75_37 values (138, 0.9999999999999999999999999999999999999),(139, 0.9999999999999999999999999999999999999),(140, 1.9999999999999999999999999999999999999),(141, 1.9999999999999999999999999999999999999),(142, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (143, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(144, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(145, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_75_37_data_start_index = 138
    def test_cast_to_decimal32_1_1_from_decimal256_75_37_data_end_index = 146
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_75_74 values (146, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(147, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(148, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(149, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(150, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (151, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(152, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(153, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_75_74_data_start_index = 146
    def test_cast_to_decimal32_1_1_from_decimal256_75_74_data_end_index = 154
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_33 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_75_75 values (154, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(155, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_75_75_data_start_index = 154
    def test_cast_to_decimal32_1_1_from_decimal256_75_75_data_end_index = 156
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_34 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_76_0 values (156, 1),(157, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(158, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_76_0_data_start_index = 156
    def test_cast_to_decimal32_1_1_from_decimal256_76_0_data_end_index = 159
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_76_1 values (159, 1.9),(160, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(161, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_76_1_data_start_index = 159
    def test_cast_to_decimal32_1_1_from_decimal256_76_1_data_end_index = 162
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_76_38 values (162, 0.99999999999999999999999999999999999999),(163, 0.99999999999999999999999999999999999999),(164, 1.99999999999999999999999999999999999999),(165, 1.99999999999999999999999999999999999999),(166, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (167, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(168, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(169, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_76_38_data_start_index = 162
    def test_cast_to_decimal32_1_1_from_decimal256_76_38_data_end_index = 170
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_76_75 values (170, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(171, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(172, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(173, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(174, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (175, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(176, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(177, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_76_75_data_start_index = 170
    def test_cast_to_decimal32_1_1_from_decimal256_76_75_data_end_index = 178
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_38 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal256_76_76 values (178, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(179, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal256_76_76_data_start_index = 178
    def test_cast_to_decimal32_1_1_from_decimal256_76_76_data_end_index = 180
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_39 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal256_76_76 order by 1;'

}