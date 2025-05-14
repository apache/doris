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


suite("test_cast_to_decimal256_76_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_76_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_76_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_1_from_decimal256_76_0 values (0, 1000000000000000000000000000000000000000000000000000000000000000000000000000),(1, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(2, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_1_from_decimal256_76_0_data_start_index = 0
    def test_cast_to_decimal256_76_1_from_decimal256_76_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal256_76_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_76_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_76_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(76, 1)) from test_cast_to_decimal256_76_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_76_38_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_decimal256_39_0 values (3, 100000000000000000000000000000000000000),(4, 999999999999999999999999999999999999998),(5, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_decimal256_39_0_data_start_index = 3
    def test_cast_to_decimal256_76_38_from_decimal256_39_0_data_end_index = 6
    for (int data_index = test_cast_to_decimal256_76_38_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_76_38_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_76_38_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_decimal256_75_0 values (6, 100000000000000000000000000000000000000),(7, 999999999999999999999999999999999999999999999999999999999999999999999999998),(8, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_decimal256_75_0_data_start_index = 6
    def test_cast_to_decimal256_76_38_from_decimal256_75_0_data_end_index = 9
    for (int data_index = test_cast_to_decimal256_76_38_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_76_38_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_76_38_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_decimal256_75_1 values (9, 100000000000000000000000000000000000000.9),(10, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(11, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_decimal256_75_1_data_start_index = 9
    def test_cast_to_decimal256_76_38_from_decimal256_75_1_data_end_index = 12
    for (int data_index = test_cast_to_decimal256_76_38_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_76_38_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_76_38_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_decimal256_76_0 values (12, 100000000000000000000000000000000000000),(13, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(14, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_decimal256_76_0_data_start_index = 12
    def test_cast_to_decimal256_76_38_from_decimal256_76_0_data_end_index = 15
    for (int data_index = test_cast_to_decimal256_76_38_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_76_38_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_76_38_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_decimal256_76_1 values (15, 100000000000000000000000000000000000000.9),(16, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(17, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_38_from_decimal256_76_1_data_start_index = 15
    def test_cast_to_decimal256_76_38_from_decimal256_76_1_data_end_index = 18
    for (int data_index = test_cast_to_decimal256_76_38_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_76_38_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_38_0 values (18, 10),(19, 99999999999999999999999999999999999998),(20, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_38_0_data_start_index = 18
    def test_cast_to_decimal256_76_75_from_decimal256_38_0_data_end_index = 21
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_38_1 values (21, 10.9),(22, 9999999999999999999999999999999999998.9),(23, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_38_1_data_start_index = 21
    def test_cast_to_decimal256_76_75_from_decimal256_38_1_data_end_index = 24
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_38_19 values (24, 10.9999999999999999999),(25, 9999999999999999998.9999999999999999999),(26, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_38_19_data_start_index = 24
    def test_cast_to_decimal256_76_75_from_decimal256_38_19_data_end_index = 27
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_39_0 values (27, 10),(28, 999999999999999999999999999999999999998),(29, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_39_0_data_start_index = 27
    def test_cast_to_decimal256_76_75_from_decimal256_39_0_data_end_index = 30
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_39_1 values (30, 10.9),(31, 99999999999999999999999999999999999998.9),(32, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_39_1_data_start_index = 30
    def test_cast_to_decimal256_76_75_from_decimal256_39_1_data_end_index = 33
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_39_19 values (33, 10.9999999999999999999),(34, 99999999999999999998.9999999999999999999),(35, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_39_19_data_start_index = 33
    def test_cast_to_decimal256_76_75_from_decimal256_39_19_data_end_index = 36
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_75_0 values (36, 10),(37, 999999999999999999999999999999999999999999999999999999999999999999999999998),(38, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_75_0_data_start_index = 36
    def test_cast_to_decimal256_76_75_from_decimal256_75_0_data_end_index = 39
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_75_1 values (39, 10.9),(40, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(41, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_75_1_data_start_index = 39
    def test_cast_to_decimal256_76_75_from_decimal256_75_1_data_end_index = 42
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_75_37 values (42, 10.9999999999999999999999999999999999999),(43, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(44, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_75_37_data_start_index = 42
    def test_cast_to_decimal256_76_75_from_decimal256_75_37_data_end_index = 45
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_76_0 values (45, 10),(46, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(47, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_76_0_data_start_index = 45
    def test_cast_to_decimal256_76_75_from_decimal256_76_0_data_end_index = 48
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_76_1 values (48, 10.9),(49, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(50, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_76_1_data_start_index = 48
    def test_cast_to_decimal256_76_75_from_decimal256_76_1_data_end_index = 51
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_76_75_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_decimal256_76_38 values (51, 10.99999999999999999999999999999999999999),(52, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(53, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_75_from_decimal256_76_38_data_start_index = 51
    def test_cast_to_decimal256_76_75_from_decimal256_76_38_data_end_index = 54
    for (int data_index = test_cast_to_decimal256_76_75_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_76_75_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_38_0 values (54, 1),(55, 99999999999999999999999999999999999998),(56, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_38_0_data_start_index = 54
    def test_cast_to_decimal256_76_76_from_decimal256_38_0_data_end_index = 57
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_38_1 values (57, 1.9),(58, 9999999999999999999999999999999999998.9),(59, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_38_1_data_start_index = 57
    def test_cast_to_decimal256_76_76_from_decimal256_38_1_data_end_index = 60
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_38_19 values (60, 1.9999999999999999999),(61, 9999999999999999998.9999999999999999999),(62, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_38_19_data_start_index = 60
    def test_cast_to_decimal256_76_76_from_decimal256_38_19_data_end_index = 63
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_38_37 values (63, 1.9999999999999999999999999999999999999),(64, 8.9999999999999999999999999999999999999),(65, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_38_37_data_start_index = 63
    def test_cast_to_decimal256_76_76_from_decimal256_38_37_data_end_index = 66
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_39_0 values (66, 1),(67, 999999999999999999999999999999999999998),(68, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_39_0_data_start_index = 66
    def test_cast_to_decimal256_76_76_from_decimal256_39_0_data_end_index = 69
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_39_1 values (69, 1.9),(70, 99999999999999999999999999999999999998.9),(71, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_39_1_data_start_index = 69
    def test_cast_to_decimal256_76_76_from_decimal256_39_1_data_end_index = 72
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_39_19 values (72, 1.9999999999999999999),(73, 99999999999999999998.9999999999999999999),(74, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_39_19_data_start_index = 72
    def test_cast_to_decimal256_76_76_from_decimal256_39_19_data_end_index = 75
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_39_38 values (75, 1.99999999999999999999999999999999999999),(76, 8.99999999999999999999999999999999999999),(77, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_39_38_data_start_index = 75
    def test_cast_to_decimal256_76_76_from_decimal256_39_38_data_end_index = 78
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_75_0 values (78, 1),(79, 999999999999999999999999999999999999999999999999999999999999999999999999998),(80, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_75_0_data_start_index = 78
    def test_cast_to_decimal256_76_76_from_decimal256_75_0_data_end_index = 81
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_75_1 values (81, 1.9),(82, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(83, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_75_1_data_start_index = 81
    def test_cast_to_decimal256_76_76_from_decimal256_75_1_data_end_index = 84
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_75_37 values (84, 1.9999999999999999999999999999999999999),(85, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(86, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_75_37_data_start_index = 84
    def test_cast_to_decimal256_76_76_from_decimal256_75_37_data_end_index = 87
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_75_74 values (87, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(88, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(89, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_75_74_data_start_index = 87
    def test_cast_to_decimal256_76_76_from_decimal256_75_74_data_end_index = 90
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_76_0 values (90, 1),(91, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(92, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_76_0_data_start_index = 90
    def test_cast_to_decimal256_76_76_from_decimal256_76_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_76_1 values (93, 1.9),(94, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(95, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_76_1_data_start_index = 93
    def test_cast_to_decimal256_76_76_from_decimal256_76_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_76_38 values (96, 1.99999999999999999999999999999999999999),(97, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(98, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_76_38_data_start_index = 96
    def test_cast_to_decimal256_76_76_from_decimal256_76_38_data_end_index = 99
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal256_76_75 values (99, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(100, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(101, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_76_76_from_decimal256_76_75_data_start_index = 99
    def test_cast_to_decimal256_76_76_from_decimal256_76_75_data_end_index = 102
    for (int data_index = test_cast_to_decimal256_76_76_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal256_76_76_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal256_76_75 order by 1;'

}