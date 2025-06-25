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


suite("test_cast_to_decimal128i_38_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128i_38_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_38_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_1_from_decimal128i_38_0 values (0, 10000000000000000000000000000000000000),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_1_from_decimal128i_38_0_data_start_index = 0
    def test_cast_to_decimal128i_38_1_from_decimal128i_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_38_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal128i_38_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal128i_37_0 values (3, 10000000000000000000),(4, 9999999999999999999999999999999999998),(5, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal128i_37_0_data_start_index = 3
    def test_cast_to_decimal128i_38_19_from_decimal128i_37_0_data_end_index = 6
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal128i_37_1 values (6, 10000000000000000000.9),(7, 999999999999999999999999999999999998.9),(8, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal128i_37_1_data_start_index = 6
    def test_cast_to_decimal128i_38_19_from_decimal128i_37_1_data_end_index = 9
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal128i_38_0 values (9, 10000000000000000000),(10, 99999999999999999999999999999999999998),(11, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal128i_38_0_data_start_index = 9
    def test_cast_to_decimal128i_38_19_from_decimal128i_38_0_data_end_index = 12
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_38_19_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_decimal128i_38_1 values (12, 10000000000000000000.9),(13, 9999999999999999999999999999999999998.9),(14, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_19_from_decimal128i_38_1_data_start_index = 12
    def test_cast_to_decimal128i_38_19_from_decimal128i_38_1_data_end_index = 15
    for (int data_index = test_cast_to_decimal128i_38_19_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_19_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_19_0 values (15, 10),(16, 9999999999999999998),(17, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_19_0_data_start_index = 15
    def test_cast_to_decimal128i_38_37_from_decimal128i_19_0_data_end_index = 18
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_19_1 values (18, 10.9),(19, 999999999999999998.9),(20, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_19_1_data_start_index = 18
    def test_cast_to_decimal128i_38_37_from_decimal128i_19_1_data_end_index = 21
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_19_9 values (21, 10.999999999),(22, 9999999998.999999999),(23, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_19_9_data_start_index = 21
    def test_cast_to_decimal128i_38_37_from_decimal128i_19_9_data_end_index = 24
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_37_0 values (24, 10),(25, 9999999999999999999999999999999999998),(26, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_37_0_data_start_index = 24
    def test_cast_to_decimal128i_38_37_from_decimal128i_37_0_data_end_index = 27
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_37_1 values (27, 10.9),(28, 999999999999999999999999999999999998.9),(29, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_37_1_data_start_index = 27
    def test_cast_to_decimal128i_38_37_from_decimal128i_37_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_37_18 values (30, 10.999999999999999999),(31, 9999999999999999998.999999999999999999),(32, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_37_18_data_start_index = 30
    def test_cast_to_decimal128i_38_37_from_decimal128i_37_18_data_end_index = 33
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_38_0 values (33, 10),(34, 99999999999999999999999999999999999998),(35, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_38_0_data_start_index = 33
    def test_cast_to_decimal128i_38_37_from_decimal128i_38_0_data_end_index = 36
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_38_1 values (36, 10.9),(37, 9999999999999999999999999999999999998.9),(38, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_38_1_data_start_index = 36
    def test_cast_to_decimal128i_38_37_from_decimal128i_38_1_data_end_index = 39
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_38_37_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_decimal128i_38_19 values (39, 10.9999999999999999999),(40, 9999999999999999998.9999999999999999999),(41, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_37_from_decimal128i_38_19_data_start_index = 39
    def test_cast_to_decimal128i_38_37_from_decimal128i_38_19_data_end_index = 42
    for (int data_index = test_cast_to_decimal128i_38_37_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_38_37_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_19_0 values (42, 1),(43, 9999999999999999998),(44, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_19_0_data_start_index = 42
    def test_cast_to_decimal128i_38_38_from_decimal128i_19_0_data_end_index = 45
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_19_1 values (45, 1.9),(46, 999999999999999998.9),(47, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_19_1_data_start_index = 45
    def test_cast_to_decimal128i_38_38_from_decimal128i_19_1_data_end_index = 48
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_19_9 values (48, 1.999999999),(49, 9999999998.999999999),(50, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_19_9_data_start_index = 48
    def test_cast_to_decimal128i_38_38_from_decimal128i_19_9_data_end_index = 51
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_19_18 values (51, 1.999999999999999999),(52, 8.999999999999999999),(53, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_19_18_data_start_index = 51
    def test_cast_to_decimal128i_38_38_from_decimal128i_19_18_data_end_index = 54
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_37_0 values (54, 1),(55, 9999999999999999999999999999999999998),(56, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_37_0_data_start_index = 54
    def test_cast_to_decimal128i_38_38_from_decimal128i_37_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_37_1 values (57, 1.9),(58, 999999999999999999999999999999999998.9),(59, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_37_1_data_start_index = 57
    def test_cast_to_decimal128i_38_38_from_decimal128i_37_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_37_18 values (60, 1.999999999999999999),(61, 9999999999999999998.999999999999999999),(62, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_37_18_data_start_index = 60
    def test_cast_to_decimal128i_38_38_from_decimal128i_37_18_data_end_index = 63
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_37_36 values (63, 1.999999999999999999999999999999999999),(64, 8.999999999999999999999999999999999999),(65, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_37_36_data_start_index = 63
    def test_cast_to_decimal128i_38_38_from_decimal128i_37_36_data_end_index = 66
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_38_0 values (66, 1),(67, 99999999999999999999999999999999999998),(68, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_38_0_data_start_index = 66
    def test_cast_to_decimal128i_38_38_from_decimal128i_38_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_38_1 values (69, 1.9),(70, 9999999999999999999999999999999999998.9),(71, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_38_1_data_start_index = 69
    def test_cast_to_decimal128i_38_38_from_decimal128i_38_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_38_19 values (72, 1.9999999999999999999),(73, 9999999999999999998.9999999999999999999),(74, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_38_19_data_start_index = 72
    def test_cast_to_decimal128i_38_38_from_decimal128i_38_19_data_end_index = 75
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_38_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_38_38_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_38_from_decimal128i_38_37 values (75, 1.9999999999999999999999999999999999999),(76, 8.9999999999999999999999999999999999999),(77, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_38_38_from_decimal128i_38_37_data_start_index = 75
    def test_cast_to_decimal128i_38_38_from_decimal128i_38_37_data_end_index = 78
    for (int data_index = test_cast_to_decimal128i_38_38_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal128i_38_38_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(38, 38)) from test_cast_to_decimal128i_38_38_from_decimal128i_38_37 order by 1;'

}