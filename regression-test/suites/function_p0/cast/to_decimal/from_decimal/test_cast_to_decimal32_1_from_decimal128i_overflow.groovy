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


suite("test_cast_to_decimal32_1_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_0 values (0, 10),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_19_0_data_start_index = 0
    def test_cast_to_decimal32_1_0_from_decimal128i_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_1 values (3, 9.9),(4, 9.9),(5, 10.9),(6, 10.9),(7, 999999999999999998.9),
      (8, 999999999999999998.9),(9, 999999999999999999.9),(10, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_19_1_data_start_index = 3
    def test_cast_to_decimal32_1_0_from_decimal128i_19_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_9 values (11, 9.999999999),(12, 9.999999999),(13, 10.999999999),(14, 10.999999999),(15, 9999999998.999999999),
      (16, 9999999998.999999999),(17, 9999999999.999999999),(18, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_19_9_data_start_index = 11
    def test_cast_to_decimal32_1_0_from_decimal128i_19_9_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_18 values (19, 9.999999999999999999),(20, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_19_18_data_start_index = 19
    def test_cast_to_decimal32_1_0_from_decimal128i_19_18_data_end_index = 21
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_3 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_0 values (21, 10),(22, 9999999999999999999999999999999999998),(23, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_37_0_data_start_index = 21
    def test_cast_to_decimal32_1_0_from_decimal128i_37_0_data_end_index = 24
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_1 values (24, 9.9),(25, 9.9),(26, 10.9),(27, 10.9),(28, 999999999999999999999999999999999998.9),
      (29, 999999999999999999999999999999999998.9),(30, 999999999999999999999999999999999999.9),(31, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_37_1_data_start_index = 24
    def test_cast_to_decimal32_1_0_from_decimal128i_37_1_data_end_index = 32
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_18 values (32, 9.999999999999999999),(33, 9.999999999999999999),(34, 10.999999999999999999),(35, 10.999999999999999999),(36, 9999999999999999998.999999999999999999),
      (37, 9999999999999999998.999999999999999999),(38, 9999999999999999999.999999999999999999),(39, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_37_18_data_start_index = 32
    def test_cast_to_decimal32_1_0_from_decimal128i_37_18_data_end_index = 40
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_36 values (40, 9.999999999999999999999999999999999999),(41, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_37_36_data_start_index = 40
    def test_cast_to_decimal32_1_0_from_decimal128i_37_36_data_end_index = 42
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_8 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_0 values (42, 10),(43, 99999999999999999999999999999999999998),(44, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_38_0_data_start_index = 42
    def test_cast_to_decimal32_1_0_from_decimal128i_38_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_1 values (45, 9.9),(46, 9.9),(47, 10.9),(48, 10.9),(49, 9999999999999999999999999999999999998.9),
      (50, 9999999999999999999999999999999999998.9),(51, 9999999999999999999999999999999999999.9),(52, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_38_1_data_start_index = 45
    def test_cast_to_decimal32_1_0_from_decimal128i_38_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_19 values (53, 9.9999999999999999999),(54, 9.9999999999999999999),(55, 10.9999999999999999999),(56, 10.9999999999999999999),(57, 9999999999999999998.9999999999999999999),
      (58, 9999999999999999998.9999999999999999999),(59, 9999999999999999999.9999999999999999999),(60, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_38_19_data_start_index = 53
    def test_cast_to_decimal32_1_0_from_decimal128i_38_19_data_end_index = 61
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_37 values (61, 9.9999999999999999999999999999999999999),(62, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_0_from_decimal128i_38_37_data_start_index = 61
    def test_cast_to_decimal32_1_0_from_decimal128i_38_37_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_1_0_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal32_1_0_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_13 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_19_0 values (63, 1),(64, 9999999999999999998),(65, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_19_0_data_start_index = 63
    def test_cast_to_decimal32_1_1_from_decimal128i_19_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_19_1 values (66, 1.9),(67, 999999999999999998.9),(68, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_19_1_data_start_index = 66
    def test_cast_to_decimal32_1_1_from_decimal128i_19_1_data_end_index = 69
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_19_9 values (69, 0.999999999),(70, 0.999999999),(71, 1.999999999),(72, 1.999999999),(73, 9999999998.999999999),
      (74, 9999999998.999999999),(75, 9999999999.999999999),(76, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_19_9_data_start_index = 69
    def test_cast_to_decimal32_1_1_from_decimal128i_19_9_data_end_index = 77
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_19_18 values (77, 0.999999999999999999),(78, 0.999999999999999999),(79, 1.999999999999999999),(80, 1.999999999999999999),(81, 8.999999999999999999),
      (82, 8.999999999999999999),(83, 9.999999999999999999),(84, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_19_18_data_start_index = 77
    def test_cast_to_decimal32_1_1_from_decimal128i_19_18_data_end_index = 85
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_18 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_19_19 values (85, 0.9999999999999999999),(86, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_19_19_data_start_index = 85
    def test_cast_to_decimal32_1_1_from_decimal128i_19_19_data_end_index = 87
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_19_19_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_19 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_37_0 values (87, 1),(88, 9999999999999999999999999999999999998),(89, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_37_0_data_start_index = 87
    def test_cast_to_decimal32_1_1_from_decimal128i_37_0_data_end_index = 90
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_37_1 values (90, 1.9),(91, 999999999999999999999999999999999998.9),(92, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_37_1_data_start_index = 90
    def test_cast_to_decimal32_1_1_from_decimal128i_37_1_data_end_index = 93
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_37_18 values (93, 0.999999999999999999),(94, 0.999999999999999999),(95, 1.999999999999999999),(96, 1.999999999999999999),(97, 9999999999999999998.999999999999999999),
      (98, 9999999999999999998.999999999999999999),(99, 9999999999999999999.999999999999999999),(100, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_37_18_data_start_index = 93
    def test_cast_to_decimal32_1_1_from_decimal128i_37_18_data_end_index = 101
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_37_36 values (101, 0.999999999999999999999999999999999999),(102, 0.999999999999999999999999999999999999),(103, 1.999999999999999999999999999999999999),(104, 1.999999999999999999999999999999999999),(105, 8.999999999999999999999999999999999999),
      (106, 8.999999999999999999999999999999999999),(107, 9.999999999999999999999999999999999999),(108, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_37_36_data_start_index = 101
    def test_cast_to_decimal32_1_1_from_decimal128i_37_36_data_end_index = 109
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_23 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_37_37 values (109, 0.9999999999999999999999999999999999999),(110, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_37_37_data_start_index = 109
    def test_cast_to_decimal32_1_1_from_decimal128i_37_37_data_end_index = 111
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_37_37_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_24 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_38_0 values (111, 1),(112, 99999999999999999999999999999999999998),(113, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_38_0_data_start_index = 111
    def test_cast_to_decimal32_1_1_from_decimal128i_38_0_data_end_index = 114
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_38_1 values (114, 1.9),(115, 9999999999999999999999999999999999998.9),(116, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_38_1_data_start_index = 114
    def test_cast_to_decimal32_1_1_from_decimal128i_38_1_data_end_index = 117
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_38_19 values (117, 0.9999999999999999999),(118, 0.9999999999999999999),(119, 1.9999999999999999999),(120, 1.9999999999999999999),(121, 9999999999999999998.9999999999999999999),
      (122, 9999999999999999998.9999999999999999999),(123, 9999999999999999999.9999999999999999999),(124, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_38_19_data_start_index = 117
    def test_cast_to_decimal32_1_1_from_decimal128i_38_19_data_end_index = 125
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_38_37 values (125, 0.9999999999999999999999999999999999999),(126, 0.9999999999999999999999999999999999999),(127, 1.9999999999999999999999999999999999999),(128, 1.9999999999999999999999999999999999999),(129, 8.9999999999999999999999999999999999999),
      (130, 8.9999999999999999999999999999999999999),(131, 9.9999999999999999999999999999999999999),(132, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_38_37_data_start_index = 125
    def test_cast_to_decimal32_1_1_from_decimal128i_38_37_data_end_index = 133
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_28 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal128i_38_38 values (133, 0.99999999999999999999999999999999999999),(134, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_1_1_from_decimal128i_38_38_data_start_index = 133
    def test_cast_to_decimal32_1_1_from_decimal128i_38_38_data_end_index = 135
    for (int data_index = test_cast_to_decimal32_1_1_from_decimal128i_38_38_data_start_index; data_index < test_cast_to_decimal32_1_1_from_decimal128i_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_29 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal128i_38_38 order by 1;'

}