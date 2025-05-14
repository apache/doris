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


suite("test_cast_to_decimal256_75_from_decimal128v3_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_19_0 values (0, 10),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_19_0_data_start_index = 0
    def test_cast_to_decimal256_75_74_from_decimal128v3_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_19_1 values (3, 10.9),(4, 999999999999999998.9),(5, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_19_1_data_start_index = 3
    def test_cast_to_decimal256_75_74_from_decimal128v3_19_1_data_end_index = 6
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_19_9 values (6, 10.999999999),(7, 9999999998.999999999),(8, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_19_9_data_start_index = 6
    def test_cast_to_decimal256_75_74_from_decimal128v3_19_9_data_end_index = 9
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_37_0 values (9, 10),(10, 9999999999999999999999999999999999998),(11, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_37_0_data_start_index = 9
    def test_cast_to_decimal256_75_74_from_decimal128v3_37_0_data_end_index = 12
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_37_1 values (12, 10.9),(13, 999999999999999999999999999999999998.9),(14, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_37_1_data_start_index = 12
    def test_cast_to_decimal256_75_74_from_decimal128v3_37_1_data_end_index = 15
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_37_18 values (15, 10.999999999999999999),(16, 9999999999999999998.999999999999999999),(17, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_37_18_data_start_index = 15
    def test_cast_to_decimal256_75_74_from_decimal128v3_37_18_data_end_index = 18
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_38_0 values (18, 10),(19, 99999999999999999999999999999999999998),(20, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_38_0_data_start_index = 18
    def test_cast_to_decimal256_75_74_from_decimal128v3_38_0_data_end_index = 21
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_38_1 values (21, 10.9),(22, 9999999999999999999999999999999999998.9),(23, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_38_1_data_start_index = 21
    def test_cast_to_decimal256_75_74_from_decimal128v3_38_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal128v3_38_19 values (24, 10.9999999999999999999),(25, 9999999999999999998.9999999999999999999),(26, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal128v3_38_19_data_start_index = 24
    def test_cast_to_decimal256_75_74_from_decimal128v3_38_19_data_end_index = 27
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_19_0 values (27, 1),(28, 9999999999999999998),(29, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_19_0_data_start_index = 27
    def test_cast_to_decimal256_75_75_from_decimal128v3_19_0_data_end_index = 30
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_19_1 values (30, 1.9),(31, 999999999999999998.9),(32, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_19_1_data_start_index = 30
    def test_cast_to_decimal256_75_75_from_decimal128v3_19_1_data_end_index = 33
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_19_9 values (33, 1.999999999),(34, 9999999998.999999999),(35, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_19_9_data_start_index = 33
    def test_cast_to_decimal256_75_75_from_decimal128v3_19_9_data_end_index = 36
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_19_18 values (36, 1.999999999999999999),(37, 8.999999999999999999),(38, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_19_18_data_start_index = 36
    def test_cast_to_decimal256_75_75_from_decimal128v3_19_18_data_end_index = 39
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_37_0 values (39, 1),(40, 9999999999999999999999999999999999998),(41, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_37_0_data_start_index = 39
    def test_cast_to_decimal256_75_75_from_decimal128v3_37_0_data_end_index = 42
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_37_1 values (42, 1.9),(43, 999999999999999999999999999999999998.9),(44, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_37_1_data_start_index = 42
    def test_cast_to_decimal256_75_75_from_decimal128v3_37_1_data_end_index = 45
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_37_18 values (45, 1.999999999999999999),(46, 9999999999999999998.999999999999999999),(47, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_37_18_data_start_index = 45
    def test_cast_to_decimal256_75_75_from_decimal128v3_37_18_data_end_index = 48
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_37_36 values (48, 1.999999999999999999999999999999999999),(49, 8.999999999999999999999999999999999999),(50, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_37_36_data_start_index = 48
    def test_cast_to_decimal256_75_75_from_decimal128v3_37_36_data_end_index = 51
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_38_0 values (51, 1),(52, 99999999999999999999999999999999999998),(53, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_38_0_data_start_index = 51
    def test_cast_to_decimal256_75_75_from_decimal128v3_38_0_data_end_index = 54
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_38_1 values (54, 1.9),(55, 9999999999999999999999999999999999998.9),(56, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_38_1_data_start_index = 54
    def test_cast_to_decimal256_75_75_from_decimal128v3_38_1_data_end_index = 57
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_38_19 values (57, 1.9999999999999999999),(58, 9999999999999999998.9999999999999999999),(59, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_38_19_data_start_index = 57
    def test_cast_to_decimal256_75_75_from_decimal128v3_38_19_data_end_index = 60
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal128v3_38_37 values (60, 1.9999999999999999999999999999999999999),(61, 8.9999999999999999999999999999999999999),(62, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal128v3_38_37_data_start_index = 60
    def test_cast_to_decimal256_75_75_from_decimal128v3_38_37_data_end_index = 63
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal128v3_38_37 order by 1;'

}