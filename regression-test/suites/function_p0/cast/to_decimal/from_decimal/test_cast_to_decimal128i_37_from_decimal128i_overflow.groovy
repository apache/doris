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


suite("test_cast_to_decimal128i_37_from_decimal128i_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal128i_38_0 values (0, 10000000000000000000000000000000000000),(1, 99999999999999999999999999999999999998),(2, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal128i_38_0_data_start_index = 0
    def test_cast_to_decimal128i_37_0_from_decimal128i_38_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_37_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_0_from_decimal128i_38_1 values (3, 9999999999999999999999999999999999999.9),(4, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_0_from_decimal128i_38_1_data_start_index = 3
    def test_cast_to_decimal128i_37_0_from_decimal128i_38_1_data_end_index = 5
    for (int data_index = test_cast_to_decimal128i_37_0_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_0_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(37, 0)) from test_cast_to_decimal128i_37_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal128i_37_0 values (5, 1000000000000000000000000000000000000),(6, 9999999999999999999999999999999999998),(7, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal128i_37_0_data_start_index = 5
    def test_cast_to_decimal128i_37_1_from_decimal128i_37_0_data_end_index = 8
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal128i_38_0 values (8, 1000000000000000000000000000000000000),(9, 99999999999999999999999999999999999998),(10, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal128i_38_0_data_start_index = 8
    def test_cast_to_decimal128i_37_1_from_decimal128i_38_0_data_end_index = 11
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_1_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_37_1_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_1_from_decimal128i_38_1 values (11, 1000000000000000000000000000000000000.9),(12, 9999999999999999999999999999999999998.9),(13, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_1_from_decimal128i_38_1_data_start_index = 11
    def test_cast_to_decimal128i_37_1_from_decimal128i_38_1_data_end_index = 14
    for (int data_index = test_cast_to_decimal128i_37_1_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_1_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128i_37_1_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal128i_37_0 values (14, 10000000000000000000),(15, 9999999999999999999999999999999999998),(16, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal128i_37_0_data_start_index = 14
    def test_cast_to_decimal128i_37_18_from_decimal128i_37_0_data_end_index = 17
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal128i_37_1 values (17, 10000000000000000000.9),(18, 999999999999999999999999999999999998.9),(19, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal128i_37_1_data_start_index = 17
    def test_cast_to_decimal128i_37_18_from_decimal128i_37_1_data_end_index = 20
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal128i_38_0 values (20, 10000000000000000000),(21, 99999999999999999999999999999999999998),(22, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal128i_38_0_data_start_index = 20
    def test_cast_to_decimal128i_37_18_from_decimal128i_38_0_data_end_index = 23
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal128i_38_1 values (23, 10000000000000000000.9),(24, 9999999999999999999999999999999999998.9),(25, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal128i_38_1_data_start_index = 23
    def test_cast_to_decimal128i_37_18_from_decimal128i_38_1_data_end_index = 26
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_18_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_37_18_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_18_from_decimal128i_38_19 values (26, 9999999999999999999.9999999999999999999),(27, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_18_from_decimal128i_38_19_data_start_index = 26
    def test_cast_to_decimal128i_37_18_from_decimal128i_38_19_data_end_index = 28
    for (int data_index = test_cast_to_decimal128i_37_18_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_18_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(37, 18)) from test_cast_to_decimal128i_37_18_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_19_0 values (28, 10),(29, 9999999999999999998),(30, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_19_0_data_start_index = 28
    def test_cast_to_decimal128i_37_36_from_decimal128i_19_0_data_end_index = 31
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_19_1 values (31, 10.9),(32, 999999999999999998.9),(33, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_19_1_data_start_index = 31
    def test_cast_to_decimal128i_37_36_from_decimal128i_19_1_data_end_index = 34
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_19_9 values (34, 10.999999999),(35, 9999999998.999999999),(36, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_19_9_data_start_index = 34
    def test_cast_to_decimal128i_37_36_from_decimal128i_19_9_data_end_index = 37
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_37_0 values (37, 10),(38, 9999999999999999999999999999999999998),(39, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_37_0_data_start_index = 37
    def test_cast_to_decimal128i_37_36_from_decimal128i_37_0_data_end_index = 40
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_37_1 values (40, 10.9),(41, 999999999999999999999999999999999998.9),(42, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_37_1_data_start_index = 40
    def test_cast_to_decimal128i_37_36_from_decimal128i_37_1_data_end_index = 43
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_37_18 values (43, 10.999999999999999999),(44, 9999999999999999998.999999999999999999),(45, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_37_18_data_start_index = 43
    def test_cast_to_decimal128i_37_36_from_decimal128i_37_18_data_end_index = 46
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_38_0 values (46, 10),(47, 99999999999999999999999999999999999998),(48, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_38_0_data_start_index = 46
    def test_cast_to_decimal128i_37_36_from_decimal128i_38_0_data_end_index = 49
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_38_1 values (49, 10.9),(50, 9999999999999999999999999999999999998.9),(51, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_38_1_data_start_index = 49
    def test_cast_to_decimal128i_37_36_from_decimal128i_38_1_data_end_index = 52
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_38_19 values (52, 10.9999999999999999999),(53, 9999999999999999998.9999999999999999999),(54, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_38_19_data_start_index = 52
    def test_cast_to_decimal128i_37_36_from_decimal128i_38_19_data_end_index = 55
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_36_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_37_36_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_36_from_decimal128i_38_37 values (55, 9.9999999999999999999999999999999999999),(56, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_36_from_decimal128i_38_37_data_start_index = 55
    def test_cast_to_decimal128i_37_36_from_decimal128i_38_37_data_end_index = 57
    for (int data_index = test_cast_to_decimal128i_37_36_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal128i_37_36_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(37, 36)) from test_cast_to_decimal128i_37_36_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_19_0 values (57, 1),(58, 9999999999999999998),(59, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_19_0_data_start_index = 57
    def test_cast_to_decimal128i_37_37_from_decimal128i_19_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_19_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_19_1 values (60, 1.9),(61, 999999999999999998.9),(62, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_19_1_data_start_index = 60
    def test_cast_to_decimal128i_37_37_from_decimal128i_19_1_data_end_index = 63
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_19_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_19_9 values (63, 1.999999999),(64, 9999999998.999999999),(65, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_19_9_data_start_index = 63
    def test_cast_to_decimal128i_37_37_from_decimal128i_19_9_data_end_index = 66
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_19_9_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_19_18 values (66, 1.999999999999999999),(67, 8.999999999999999999),(68, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_19_18_data_start_index = 66
    def test_cast_to_decimal128i_37_37_from_decimal128i_19_18_data_end_index = 69
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_19_18_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_37_0 values (69, 1),(70, 9999999999999999999999999999999999998),(71, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_37_0_data_start_index = 69
    def test_cast_to_decimal128i_37_37_from_decimal128i_37_0_data_end_index = 72
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_37_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_37_1 values (72, 1.9),(73, 999999999999999999999999999999999998.9),(74, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_37_1_data_start_index = 72
    def test_cast_to_decimal128i_37_37_from_decimal128i_37_1_data_end_index = 75
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_37_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_37_18 values (75, 1.999999999999999999),(76, 9999999999999999998.999999999999999999),(77, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_37_18_data_start_index = 75
    def test_cast_to_decimal128i_37_37_from_decimal128i_37_18_data_end_index = 78
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_37_18_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_37_36 values (78, 1.999999999999999999999999999999999999),(79, 8.999999999999999999999999999999999999),(80, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_37_36_data_start_index = 78
    def test_cast_to_decimal128i_37_37_from_decimal128i_37_36_data_end_index = 81
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_37_36_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_38_0 values (81, 1),(82, 99999999999999999999999999999999999998),(83, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_38_0_data_start_index = 81
    def test_cast_to_decimal128i_37_37_from_decimal128i_38_0_data_end_index = 84
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_38_0_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_38_1 values (84, 1.9),(85, 9999999999999999999999999999999999998.9),(86, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_38_1_data_start_index = 84
    def test_cast_to_decimal128i_37_37_from_decimal128i_38_1_data_end_index = 87
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_38_1_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_38_19 values (87, 1.9999999999999999999),(88, 9999999999999999998.9999999999999999999),(89, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_38_19_data_start_index = 87
    def test_cast_to_decimal128i_37_37_from_decimal128i_38_19_data_end_index = 90
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_38_19_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_38_37 values (90, 1.9999999999999999999999999999999999999),(91, 8.9999999999999999999999999999999999999),(92, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_38_37_data_start_index = 90
    def test_cast_to_decimal128i_37_37_from_decimal128i_38_37_data_end_index = 93
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_38_37_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal128i_38_38 values (93, 0.99999999999999999999999999999999999999),(94, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal128i_37_37_from_decimal128i_38_38_data_start_index = 93
    def test_cast_to_decimal128i_37_37_from_decimal128i_38_38_data_end_index = 95
    for (int data_index = test_cast_to_decimal128i_37_37_from_decimal128i_38_38_data_start_index; data_index < test_cast_to_decimal128i_37_37_from_decimal128i_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal128i_38_38 order by 1;'

}