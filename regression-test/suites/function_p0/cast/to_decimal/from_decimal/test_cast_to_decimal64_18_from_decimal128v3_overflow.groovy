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


suite("test_cast_to_decimal64_18_from_decimal128v3_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_19_0 values (0, 1000000000000000000),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_19_0_data_start_index = 0
    def test_cast_to_decimal64_18_0_from_decimal128v3_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_19_1 values (3, 999999999999999999.9),(4, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_19_1_data_start_index = 3
    def test_cast_to_decimal64_18_0_from_decimal128v3_19_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_37_0 values (5, 1000000000000000000),(6, 9999999999999999999999999999999999998),(7, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_37_0_data_start_index = 5
    def test_cast_to_decimal64_18_0_from_decimal128v3_37_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_37_1 values (8, 999999999999999999.9),(9, 999999999999999999.9),(10, 1000000000000000000.9),(11, 1000000000000000000.9),(12, 999999999999999999999999999999999998.9),
      (13, 999999999999999999999999999999999998.9),(14, 999999999999999999999999999999999999.9),(15, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_37_1_data_start_index = 8
    def test_cast_to_decimal64_18_0_from_decimal128v3_37_1_data_end_index = 16
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_37_18 values (16, 999999999999999999.999999999999999999),(17, 999999999999999999.999999999999999999),(18, 1000000000000000000.999999999999999999),(19, 1000000000000000000.999999999999999999),(20, 9999999999999999998.999999999999999999),
      (21, 9999999999999999998.999999999999999999),(22, 9999999999999999999.999999999999999999),(23, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_37_18_data_start_index = 16
    def test_cast_to_decimal64_18_0_from_decimal128v3_37_18_data_end_index = 24
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_38_0 values (24, 1000000000000000000),(25, 99999999999999999999999999999999999998),(26, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_38_0_data_start_index = 24
    def test_cast_to_decimal64_18_0_from_decimal128v3_38_0_data_end_index = 27
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_38_1 values (27, 999999999999999999.9),(28, 999999999999999999.9),(29, 1000000000000000000.9),(30, 1000000000000000000.9),(31, 9999999999999999999999999999999999998.9),
      (32, 9999999999999999999999999999999999998.9),(33, 9999999999999999999999999999999999999.9),(34, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_38_1_data_start_index = 27
    def test_cast_to_decimal64_18_0_from_decimal128v3_38_1_data_end_index = 35
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_18_0_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_decimal128v3_38_19 values (35, 999999999999999999.9999999999999999999),(36, 999999999999999999.9999999999999999999),(37, 1000000000000000000.9999999999999999999),(38, 1000000000000000000.9999999999999999999),(39, 9999999999999999998.9999999999999999999),
      (40, 9999999999999999998.9999999999999999999),(41, 9999999999999999999.9999999999999999999),(42, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_0_from_decimal128v3_38_19_data_start_index = 35
    def test_cast_to_decimal64_18_0_from_decimal128v3_38_19_data_end_index = 43
    for (int data_index = test_cast_to_decimal64_18_0_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_18_0_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_19_0 values (43, 100000000000000000),(44, 9999999999999999998),(45, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_19_0_data_start_index = 43
    def test_cast_to_decimal64_18_1_from_decimal128v3_19_0_data_end_index = 46
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_19_1 values (46, 100000000000000000.9),(47, 999999999999999998.9),(48, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_19_1_data_start_index = 46
    def test_cast_to_decimal64_18_1_from_decimal128v3_19_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_37_0 values (49, 100000000000000000),(50, 9999999999999999999999999999999999998),(51, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_37_0_data_start_index = 49
    def test_cast_to_decimal64_18_1_from_decimal128v3_37_0_data_end_index = 52
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_37_1 values (52, 100000000000000000.9),(53, 999999999999999999999999999999999998.9),(54, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_37_1_data_start_index = 52
    def test_cast_to_decimal64_18_1_from_decimal128v3_37_1_data_end_index = 55
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_37_18 values (55, 99999999999999999.999999999999999999),(56, 99999999999999999.999999999999999999),(57, 100000000000000000.999999999999999999),(58, 100000000000000000.999999999999999999),(59, 9999999999999999998.999999999999999999),
      (60, 9999999999999999998.999999999999999999),(61, 9999999999999999999.999999999999999999),(62, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_37_18_data_start_index = 55
    def test_cast_to_decimal64_18_1_from_decimal128v3_37_18_data_end_index = 63
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_38_0 values (63, 100000000000000000),(64, 99999999999999999999999999999999999998),(65, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_38_0_data_start_index = 63
    def test_cast_to_decimal64_18_1_from_decimal128v3_38_0_data_end_index = 66
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_38_1 values (66, 100000000000000000.9),(67, 9999999999999999999999999999999999998.9),(68, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_38_1_data_start_index = 66
    def test_cast_to_decimal64_18_1_from_decimal128v3_38_1_data_end_index = 69
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_1_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_18_1_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_1_from_decimal128v3_38_19 values (69, 99999999999999999.9999999999999999999),(70, 99999999999999999.9999999999999999999),(71, 100000000000000000.9999999999999999999),(72, 100000000000000000.9999999999999999999),(73, 9999999999999999998.9999999999999999999),
      (74, 9999999999999999998.9999999999999999999),(75, 9999999999999999999.9999999999999999999),(76, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_1_from_decimal128v3_38_19_data_start_index = 69
    def test_cast_to_decimal64_18_1_from_decimal128v3_38_19_data_end_index = 77
    for (int data_index = test_cast_to_decimal64_18_1_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_18_1_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(18, 1)) from test_cast_to_decimal64_18_1_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_19_0 values (77, 1000000000),(78, 9999999999999999998),(79, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_19_0_data_start_index = 77
    def test_cast_to_decimal64_18_9_from_decimal128v3_19_0_data_end_index = 80
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_19_1 values (80, 1000000000.9),(81, 999999999999999998.9),(82, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_19_1_data_start_index = 80
    def test_cast_to_decimal64_18_9_from_decimal128v3_19_1_data_end_index = 83
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_19_9 values (83, 1000000000.999999999),(84, 9999999998.999999999),(85, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_19_9_data_start_index = 83
    def test_cast_to_decimal64_18_9_from_decimal128v3_19_9_data_end_index = 86
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_37_0 values (86, 1000000000),(87, 9999999999999999999999999999999999998),(88, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_37_0_data_start_index = 86
    def test_cast_to_decimal64_18_9_from_decimal128v3_37_0_data_end_index = 89
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_37_1 values (89, 1000000000.9),(90, 999999999999999999999999999999999998.9),(91, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_37_1_data_start_index = 89
    def test_cast_to_decimal64_18_9_from_decimal128v3_37_1_data_end_index = 92
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_37_18 values (92, 999999999.999999999999999999),(93, 999999999.999999999999999999),(94, 1000000000.999999999999999999),(95, 1000000000.999999999999999999),(96, 9999999999999999998.999999999999999999),
      (97, 9999999999999999998.999999999999999999),(98, 9999999999999999999.999999999999999999),(99, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_37_18_data_start_index = 92
    def test_cast_to_decimal64_18_9_from_decimal128v3_37_18_data_end_index = 100
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_38_0 values (100, 1000000000),(101, 99999999999999999999999999999999999998),(102, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_38_0_data_start_index = 100
    def test_cast_to_decimal64_18_9_from_decimal128v3_38_0_data_end_index = 103
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_38_1 values (103, 1000000000.9),(104, 9999999999999999999999999999999999998.9),(105, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_38_1_data_start_index = 103
    def test_cast_to_decimal64_18_9_from_decimal128v3_38_1_data_end_index = 106
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_18_9_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_decimal128v3_38_19 values (106, 999999999.9999999999999999999),(107, 999999999.9999999999999999999),(108, 1000000000.9999999999999999999),(109, 1000000000.9999999999999999999),(110, 9999999999999999998.9999999999999999999),
      (111, 9999999999999999998.9999999999999999999),(112, 9999999999999999999.9999999999999999999),(113, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_9_from_decimal128v3_38_19_data_start_index = 106
    def test_cast_to_decimal64_18_9_from_decimal128v3_38_19_data_end_index = 114
    for (int data_index = test_cast_to_decimal64_18_9_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_18_9_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_19_0 values (114, 10),(115, 9999999999999999998),(116, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_19_0_data_start_index = 114
    def test_cast_to_decimal64_18_17_from_decimal128v3_19_0_data_end_index = 117
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_19_1 values (117, 10.9),(118, 999999999999999998.9),(119, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_19_1_data_start_index = 117
    def test_cast_to_decimal64_18_17_from_decimal128v3_19_1_data_end_index = 120
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_19_9 values (120, 10.999999999),(121, 9999999998.999999999),(122, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_19_9_data_start_index = 120
    def test_cast_to_decimal64_18_17_from_decimal128v3_19_9_data_end_index = 123
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_19_18 values (123, 9.999999999999999999),(124, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_19_18_data_start_index = 123
    def test_cast_to_decimal64_18_17_from_decimal128v3_19_18_data_end_index = 125
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_37_0 values (125, 10),(126, 9999999999999999999999999999999999998),(127, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_37_0_data_start_index = 125
    def test_cast_to_decimal64_18_17_from_decimal128v3_37_0_data_end_index = 128
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_37_1 values (128, 10.9),(129, 999999999999999999999999999999999998.9),(130, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_37_1_data_start_index = 128
    def test_cast_to_decimal64_18_17_from_decimal128v3_37_1_data_end_index = 131
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_37_18 values (131, 9.999999999999999999),(132, 9.999999999999999999),(133, 10.999999999999999999),(134, 10.999999999999999999),(135, 9999999999999999998.999999999999999999),
      (136, 9999999999999999998.999999999999999999),(137, 9999999999999999999.999999999999999999),(138, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_37_18_data_start_index = 131
    def test_cast_to_decimal64_18_17_from_decimal128v3_37_18_data_end_index = 139
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_37_36 values (139, 9.999999999999999999999999999999999999),(140, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_37_36_data_start_index = 139
    def test_cast_to_decimal64_18_17_from_decimal128v3_37_36_data_end_index = 141
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_38_0 values (141, 10),(142, 99999999999999999999999999999999999998),(143, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_38_0_data_start_index = 141
    def test_cast_to_decimal64_18_17_from_decimal128v3_38_0_data_end_index = 144
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_38_1 values (144, 10.9),(145, 9999999999999999999999999999999999998.9),(146, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_38_1_data_start_index = 144
    def test_cast_to_decimal64_18_17_from_decimal128v3_38_1_data_end_index = 147
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_38_19 values (147, 9.9999999999999999999),(148, 9.9999999999999999999),(149, 10.9999999999999999999),(150, 10.9999999999999999999),(151, 9999999999999999998.9999999999999999999),
      (152, 9999999999999999998.9999999999999999999),(153, 9999999999999999999.9999999999999999999),(154, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_38_19_data_start_index = 147
    def test_cast_to_decimal64_18_17_from_decimal128v3_38_19_data_end_index = 155
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal64_18_17_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_decimal128v3_38_37 values (155, 9.9999999999999999999999999999999999999),(156, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_17_from_decimal128v3_38_37_data_start_index = 155
    def test_cast_to_decimal64_18_17_from_decimal128v3_38_37_data_end_index = 157
    for (int data_index = test_cast_to_decimal64_18_17_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal64_18_17_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_19_0 values (157, 1),(158, 9999999999999999998),(159, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_19_0_data_start_index = 157
    def test_cast_to_decimal64_18_18_from_decimal128v3_19_0_data_end_index = 160
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_19_1 values (160, 1.9),(161, 999999999999999998.9),(162, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_19_1_data_start_index = 160
    def test_cast_to_decimal64_18_18_from_decimal128v3_19_1_data_end_index = 163
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_19_9 values (163, 1.999999999),(164, 9999999998.999999999),(165, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_19_9_data_start_index = 163
    def test_cast_to_decimal64_18_18_from_decimal128v3_19_9_data_end_index = 166
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_19_18 values (166, 1.999999999999999999),(167, 8.999999999999999999),(168, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_19_18_data_start_index = 166
    def test_cast_to_decimal64_18_18_from_decimal128v3_19_18_data_end_index = 169
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_19_19 values (169, 0.9999999999999999999),(170, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_19_19_data_start_index = 169
    def test_cast_to_decimal64_18_18_from_decimal128v3_19_19_data_end_index = 171
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_19_19_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_37_0 values (171, 1),(172, 9999999999999999999999999999999999998),(173, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_37_0_data_start_index = 171
    def test_cast_to_decimal64_18_18_from_decimal128v3_37_0_data_end_index = 174
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_37_1 values (174, 1.9),(175, 999999999999999999999999999999999998.9),(176, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_37_1_data_start_index = 174
    def test_cast_to_decimal64_18_18_from_decimal128v3_37_1_data_end_index = 177
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_37_18 values (177, 1.999999999999999999),(178, 9999999999999999998.999999999999999999),(179, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_37_18_data_start_index = 177
    def test_cast_to_decimal64_18_18_from_decimal128v3_37_18_data_end_index = 180
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_37_36 values (180, 0.999999999999999999999999999999999999),(181, 0.999999999999999999999999999999999999),(182, 1.999999999999999999999999999999999999),(183, 1.999999999999999999999999999999999999),(184, 8.999999999999999999999999999999999999),
      (185, 8.999999999999999999999999999999999999),(186, 9.999999999999999999999999999999999999),(187, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_37_36_data_start_index = 180
    def test_cast_to_decimal64_18_18_from_decimal128v3_37_36_data_end_index = 188
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_37_37 values (188, 0.9999999999999999999999999999999999999),(189, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_37_37_data_start_index = 188
    def test_cast_to_decimal64_18_18_from_decimal128v3_37_37_data_end_index = 190
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_37_37_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_38_0 values (190, 1),(191, 99999999999999999999999999999999999998),(192, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_38_0_data_start_index = 190
    def test_cast_to_decimal64_18_18_from_decimal128v3_38_0_data_end_index = 193
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_38_1 values (193, 1.9),(194, 9999999999999999999999999999999999998.9),(195, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_38_1_data_start_index = 193
    def test_cast_to_decimal64_18_18_from_decimal128v3_38_1_data_end_index = 196
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_38_19 values (196, 0.9999999999999999999),(197, 0.9999999999999999999),(198, 1.9999999999999999999),(199, 1.9999999999999999999),(200, 9999999999999999998.9999999999999999999),
      (201, 9999999999999999998.9999999999999999999),(202, 9999999999999999999.9999999999999999999),(203, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_38_19_data_start_index = 196
    def test_cast_to_decimal64_18_18_from_decimal128v3_38_19_data_end_index = 204
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_38_37 values (204, 0.9999999999999999999999999999999999999),(205, 0.9999999999999999999999999999999999999),(206, 1.9999999999999999999999999999999999999),(207, 1.9999999999999999999999999999999999999),(208, 8.9999999999999999999999999999999999999),
      (209, 8.9999999999999999999999999999999999999),(210, 9.9999999999999999999999999999999999999),(211, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_38_37_data_start_index = 204
    def test_cast_to_decimal64_18_18_from_decimal128v3_38_37_data_end_index = 212
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128v3_38_38 values (212, 0.99999999999999999999999999999999999999),(213, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal64_18_18_from_decimal128v3_38_38_data_start_index = 212
    def test_cast_to_decimal64_18_18_from_decimal128v3_38_38_data_end_index = 214
    for (int data_index = test_cast_to_decimal64_18_18_from_decimal128v3_38_38_data_start_index; data_index < test_cast_to_decimal64_18_18_from_decimal128v3_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128v3_38_38 order by 1;'

}