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


suite("test_cast_to_decimal256_75_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_75_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_75_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_0_from_decimal256_76_0 values (0, 1000000000000000000000000000000000000000000000000000000000000000000000000000),(1, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(2, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_0_from_decimal256_76_0_data_start_index = 0
    def test_cast_to_decimal256_75_0_from_decimal256_76_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal256_75_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_75_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 0)) from test_cast_to_decimal256_75_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(75, 0)) from test_cast_to_decimal256_75_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_75_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_0_from_decimal256_76_1 values (3, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(4, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_0_from_decimal256_76_1_data_start_index = 3
    def test_cast_to_decimal256_75_0_from_decimal256_76_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal256_75_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_75_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 0)) from test_cast_to_decimal256_75_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(75, 0)) from test_cast_to_decimal256_75_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_75_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_1_from_decimal256_75_0 values (5, 100000000000000000000000000000000000000000000000000000000000000000000000000),(6, 999999999999999999999999999999999999999999999999999999999999999999999999998),(7, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_1_from_decimal256_75_0_data_start_index = 5
    def test_cast_to_decimal256_75_1_from_decimal256_75_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal256_75_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_75_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_75_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_1_from_decimal256_76_0 values (8, 100000000000000000000000000000000000000000000000000000000000000000000000000),(9, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(10, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_1_from_decimal256_76_0_data_start_index = 8
    def test_cast_to_decimal256_75_1_from_decimal256_76_0_data_end_index = 11
    for (int data_index = test_cast_to_decimal256_75_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_75_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_75_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_1_from_decimal256_76_1 values (11, 100000000000000000000000000000000000000000000000000000000000000000000000000.9),(12, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(13, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_1_from_decimal256_76_1_data_start_index = 11
    def test_cast_to_decimal256_75_1_from_decimal256_76_1_data_end_index = 14
    for (int data_index = test_cast_to_decimal256_75_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_75_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(75, 1)) from test_cast_to_decimal256_75_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_39_0 values (14, 100000000000000000000000000000000000000),(15, 999999999999999999999999999999999999998),(16, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_39_0_data_start_index = 14
    def test_cast_to_decimal256_75_37_from_decimal256_39_0_data_end_index = 17
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_75_0 values (17, 100000000000000000000000000000000000000),(18, 999999999999999999999999999999999999999999999999999999999999999999999999998),(19, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_75_0_data_start_index = 17
    def test_cast_to_decimal256_75_37_from_decimal256_75_0_data_end_index = 20
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_75_1 values (20, 100000000000000000000000000000000000000.9),(21, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(22, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_75_1_data_start_index = 20
    def test_cast_to_decimal256_75_37_from_decimal256_75_1_data_end_index = 23
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_76_0 values (23, 100000000000000000000000000000000000000),(24, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(25, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_76_0_data_start_index = 23
    def test_cast_to_decimal256_75_37_from_decimal256_76_0_data_end_index = 26
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_76_1 values (26, 100000000000000000000000000000000000000.9),(27, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(28, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_76_1_data_start_index = 26
    def test_cast_to_decimal256_75_37_from_decimal256_76_1_data_end_index = 29
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_37_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_75_37_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_37_from_decimal256_76_38 values (29, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(30, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_37_from_decimal256_76_38_data_start_index = 29
    def test_cast_to_decimal256_75_37_from_decimal256_76_38_data_end_index = 31
    for (int data_index = test_cast_to_decimal256_75_37_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_75_37_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(75, 37)) from test_cast_to_decimal256_75_37_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_38_0 values (31, 10),(32, 99999999999999999999999999999999999998),(33, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_38_0_data_start_index = 31
    def test_cast_to_decimal256_75_74_from_decimal256_38_0_data_end_index = 34
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_38_1 values (34, 10.9),(35, 9999999999999999999999999999999999998.9),(36, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_38_1_data_start_index = 34
    def test_cast_to_decimal256_75_74_from_decimal256_38_1_data_end_index = 37
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_38_19 values (37, 10.9999999999999999999),(38, 9999999999999999998.9999999999999999999),(39, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_38_19_data_start_index = 37
    def test_cast_to_decimal256_75_74_from_decimal256_38_19_data_end_index = 40
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_39_0 values (40, 10),(41, 999999999999999999999999999999999999998),(42, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_39_0_data_start_index = 40
    def test_cast_to_decimal256_75_74_from_decimal256_39_0_data_end_index = 43
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_39_1 values (43, 10.9),(44, 99999999999999999999999999999999999998.9),(45, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_39_1_data_start_index = 43
    def test_cast_to_decimal256_75_74_from_decimal256_39_1_data_end_index = 46
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_39_19 values (46, 10.9999999999999999999),(47, 99999999999999999998.9999999999999999999),(48, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_39_19_data_start_index = 46
    def test_cast_to_decimal256_75_74_from_decimal256_39_19_data_end_index = 49
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_75_0 values (49, 10),(50, 999999999999999999999999999999999999999999999999999999999999999999999999998),(51, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_75_0_data_start_index = 49
    def test_cast_to_decimal256_75_74_from_decimal256_75_0_data_end_index = 52
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_75_1 values (52, 10.9),(53, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(54, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_75_1_data_start_index = 52
    def test_cast_to_decimal256_75_74_from_decimal256_75_1_data_end_index = 55
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_75_37 values (55, 10.9999999999999999999999999999999999999),(56, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(57, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_75_37_data_start_index = 55
    def test_cast_to_decimal256_75_74_from_decimal256_75_37_data_end_index = 58
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_76_0 values (58, 10),(59, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(60, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_76_0_data_start_index = 58
    def test_cast_to_decimal256_75_74_from_decimal256_76_0_data_end_index = 61
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_76_1 values (61, 10.9),(62, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(63, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_76_1_data_start_index = 61
    def test_cast_to_decimal256_75_74_from_decimal256_76_1_data_end_index = 64
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_76_38 values (64, 10.99999999999999999999999999999999999999),(65, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(66, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_76_38_data_start_index = 64
    def test_cast_to_decimal256_75_74_from_decimal256_76_38_data_end_index = 67
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_74_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal256_75_74_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_74_from_decimal256_76_75 values (67, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(68, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_74_from_decimal256_76_75_data_start_index = 67
    def test_cast_to_decimal256_75_74_from_decimal256_76_75_data_end_index = 69
    for (int data_index = test_cast_to_decimal256_75_74_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal256_75_74_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(75, 74)) from test_cast_to_decimal256_75_74_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_38_0 values (69, 1),(70, 99999999999999999999999999999999999998),(71, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_38_0_data_start_index = 69
    def test_cast_to_decimal256_75_75_from_decimal256_38_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_38_1 values (72, 1.9),(73, 9999999999999999999999999999999999998.9),(74, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_38_1_data_start_index = 72
    def test_cast_to_decimal256_75_75_from_decimal256_38_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_38_19 values (75, 1.9999999999999999999),(76, 9999999999999999998.9999999999999999999),(77, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_38_19_data_start_index = 75
    def test_cast_to_decimal256_75_75_from_decimal256_38_19_data_end_index = 78
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_38_37 values (78, 1.9999999999999999999999999999999999999),(79, 8.9999999999999999999999999999999999999),(80, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_38_37_data_start_index = 78
    def test_cast_to_decimal256_75_75_from_decimal256_38_37_data_end_index = 81
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_39_0 values (81, 1),(82, 999999999999999999999999999999999999998),(83, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_39_0_data_start_index = 81
    def test_cast_to_decimal256_75_75_from_decimal256_39_0_data_end_index = 84
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_39_1 values (84, 1.9),(85, 99999999999999999999999999999999999998.9),(86, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_39_1_data_start_index = 84
    def test_cast_to_decimal256_75_75_from_decimal256_39_1_data_end_index = 87
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_39_19 values (87, 1.9999999999999999999),(88, 99999999999999999998.9999999999999999999),(89, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_39_19_data_start_index = 87
    def test_cast_to_decimal256_75_75_from_decimal256_39_19_data_end_index = 90
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_39_38 values (90, 1.99999999999999999999999999999999999999),(91, 8.99999999999999999999999999999999999999),(92, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_39_38_data_start_index = 90
    def test_cast_to_decimal256_75_75_from_decimal256_39_38_data_end_index = 93
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_75_0 values (93, 1),(94, 999999999999999999999999999999999999999999999999999999999999999999999999998),(95, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_75_0_data_start_index = 93
    def test_cast_to_decimal256_75_75_from_decimal256_75_0_data_end_index = 96
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_75_1 values (96, 1.9),(97, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(98, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_75_1_data_start_index = 96
    def test_cast_to_decimal256_75_75_from_decimal256_75_1_data_end_index = 99
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_75_37 values (99, 1.9999999999999999999999999999999999999),(100, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(101, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_75_37_data_start_index = 99
    def test_cast_to_decimal256_75_75_from_decimal256_75_37_data_end_index = 102
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_75_74 values (102, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(103, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(104, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_75_74_data_start_index = 102
    def test_cast_to_decimal256_75_75_from_decimal256_75_74_data_end_index = 105
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_76_0 values (105, 1),(106, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(107, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_76_0_data_start_index = 105
    def test_cast_to_decimal256_75_75_from_decimal256_76_0_data_end_index = 108
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_76_1 values (108, 1.9),(109, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(110, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_76_1_data_start_index = 108
    def test_cast_to_decimal256_75_75_from_decimal256_76_1_data_end_index = 111
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_76_38 values (111, 1.99999999999999999999999999999999999999),(112, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(113, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_76_38_data_start_index = 111
    def test_cast_to_decimal256_75_75_from_decimal256_76_38_data_end_index = 114
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_76_75 values (114, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(115, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(116, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_76_75_data_start_index = 114
    def test_cast_to_decimal256_75_75_from_decimal256_76_75_data_end_index = 117
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_75_75_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal256_75_75_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_75_75_from_decimal256_76_76 values (117, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(118, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_75_75_from_decimal256_76_76_data_start_index = 117
    def test_cast_to_decimal256_75_75_from_decimal256_76_76_data_end_index = 119
    for (int data_index = test_cast_to_decimal256_75_75_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal256_75_75_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(75, 75)) from test_cast_to_decimal256_75_75_from_decimal256_76_76 order by 1;'

}