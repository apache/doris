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


suite("test_cast_to_decimal256_39_from_decimal256_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_39_0_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_39_0_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_0_from_decimal256_75_0 values (0, 1000000000000000000000000000000000000000),(1, 999999999999999999999999999999999999999999999999999999999999999999999999998),(2, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_0_from_decimal256_75_0_data_start_index = 0
    def test_cast_to_decimal256_39_0_from_decimal256_75_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal256_39_0_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_39_0_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_0_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_39_0_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_0_from_decimal256_75_1 values (3, 999999999999999999999999999999999999999.9),(4, 999999999999999999999999999999999999999.9),(5, 1000000000000000000000000000000000000000.9),(6, 1000000000000000000000000000000000000000.9),(7, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (8, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(9, 99999999999999999999999999999999999999999999999999999999999999999999999999.9),(10, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_0_from_decimal256_75_1_data_start_index = 3
    def test_cast_to_decimal256_39_0_from_decimal256_75_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal256_39_0_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_39_0_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_0_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_39_0_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_0_from_decimal256_76_0 values (11, 1000000000000000000000000000000000000000),(12, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(13, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_0_from_decimal256_76_0_data_start_index = 11
    def test_cast_to_decimal256_39_0_from_decimal256_76_0_data_end_index = 14
    for (int data_index = test_cast_to_decimal256_39_0_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_39_0_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_0_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_39_0_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_0_from_decimal256_76_1 values (14, 999999999999999999999999999999999999999.9),(15, 999999999999999999999999999999999999999.9),(16, 1000000000000000000000000000000000000000.9),(17, 1000000000000000000000000000000000000000.9),(18, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),
      (19, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(20, 999999999999999999999999999999999999999999999999999999999999999999999999999.9),(21, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_0_from_decimal256_76_1_data_start_index = 14
    def test_cast_to_decimal256_39_0_from_decimal256_76_1_data_end_index = 22
    for (int data_index = test_cast_to_decimal256_39_0_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_39_0_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(39, 0)) from test_cast_to_decimal256_39_0_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_39_0 values (22, 100000000000000000000000000000000000000),(23, 999999999999999999999999999999999999998),(24, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_39_0_data_start_index = 22
    def test_cast_to_decimal256_39_1_from_decimal256_39_0_data_end_index = 25
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_75_0 values (25, 100000000000000000000000000000000000000),(26, 999999999999999999999999999999999999999999999999999999999999999999999999998),(27, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_75_0_data_start_index = 25
    def test_cast_to_decimal256_39_1_from_decimal256_75_0_data_end_index = 28
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_75_1 values (28, 100000000000000000000000000000000000000.9),(29, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(30, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_75_1_data_start_index = 28
    def test_cast_to_decimal256_39_1_from_decimal256_75_1_data_end_index = 31
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_75_37 values (31, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(32, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_75_37_data_start_index = 31
    def test_cast_to_decimal256_39_1_from_decimal256_75_37_data_end_index = 33
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_76_0 values (33, 100000000000000000000000000000000000000),(34, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(35, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_76_0_data_start_index = 33
    def test_cast_to_decimal256_39_1_from_decimal256_76_0_data_end_index = 36
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_76_1 values (36, 100000000000000000000000000000000000000.9),(37, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(38, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_76_1_data_start_index = 36
    def test_cast_to_decimal256_39_1_from_decimal256_76_1_data_end_index = 39
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_1_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_39_1_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_1_from_decimal256_76_38 values (39, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(40, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_1_from_decimal256_76_38_data_start_index = 39
    def test_cast_to_decimal256_39_1_from_decimal256_76_38_data_end_index = 41
    for (int data_index = test_cast_to_decimal256_39_1_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_39_1_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(39, 1)) from test_cast_to_decimal256_39_1_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_38_0 values (41, 100000000000000000000),(42, 99999999999999999999999999999999999998),(43, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_38_0_data_start_index = 41
    def test_cast_to_decimal256_39_19_from_decimal256_38_0_data_end_index = 44
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_38_1 values (44, 100000000000000000000.9),(45, 9999999999999999999999999999999999998.9),(46, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_38_1_data_start_index = 44
    def test_cast_to_decimal256_39_19_from_decimal256_38_1_data_end_index = 47
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_39_0 values (47, 100000000000000000000),(48, 999999999999999999999999999999999999998),(49, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_39_0_data_start_index = 47
    def test_cast_to_decimal256_39_19_from_decimal256_39_0_data_end_index = 50
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_39_1 values (50, 100000000000000000000.9),(51, 99999999999999999999999999999999999998.9),(52, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_39_1_data_start_index = 50
    def test_cast_to_decimal256_39_19_from_decimal256_39_1_data_end_index = 53
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_75_0 values (53, 100000000000000000000),(54, 999999999999999999999999999999999999999999999999999999999999999999999999998),(55, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_75_0_data_start_index = 53
    def test_cast_to_decimal256_39_19_from_decimal256_75_0_data_end_index = 56
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_75_1 values (56, 100000000000000000000.9),(57, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(58, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_75_1_data_start_index = 56
    def test_cast_to_decimal256_39_19_from_decimal256_75_1_data_end_index = 59
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_75_37 values (59, 99999999999999999999.9999999999999999999999999999999999999),(60, 99999999999999999999.9999999999999999999999999999999999999),(61, 100000000000000000000.9999999999999999999999999999999999999),(62, 100000000000000000000.9999999999999999999999999999999999999),(63, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),
      (64, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(65, 99999999999999999999999999999999999999.9999999999999999999999999999999999999),(66, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_75_37_data_start_index = 59
    def test_cast_to_decimal256_39_19_from_decimal256_75_37_data_end_index = 67
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_76_0 values (67, 100000000000000000000),(68, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(69, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_76_0_data_start_index = 67
    def test_cast_to_decimal256_39_19_from_decimal256_76_0_data_end_index = 70
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_76_1 values (70, 100000000000000000000.9),(71, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(72, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_76_1_data_start_index = 70
    def test_cast_to_decimal256_39_19_from_decimal256_76_1_data_end_index = 73
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_19_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_39_19_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_19_from_decimal256_76_38 values (73, 99999999999999999999.99999999999999999999999999999999999999),(74, 99999999999999999999.99999999999999999999999999999999999999),(75, 100000000000000000000.99999999999999999999999999999999999999),(76, 100000000000000000000.99999999999999999999999999999999999999),(77, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),
      (78, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(79, 99999999999999999999999999999999999999.99999999999999999999999999999999999999),(80, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_19_from_decimal256_76_38_data_start_index = 73
    def test_cast_to_decimal256_39_19_from_decimal256_76_38_data_end_index = 81
    for (int data_index = test_cast_to_decimal256_39_19_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_39_19_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(39, 19)) from test_cast_to_decimal256_39_19_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_38_0 values (81, 10),(82, 99999999999999999999999999999999999998),(83, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_38_0_data_start_index = 81
    def test_cast_to_decimal256_39_38_from_decimal256_38_0_data_end_index = 84
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_38_1 values (84, 10.9),(85, 9999999999999999999999999999999999998.9),(86, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_38_1_data_start_index = 84
    def test_cast_to_decimal256_39_38_from_decimal256_38_1_data_end_index = 87
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_38_19 values (87, 10.9999999999999999999),(88, 9999999999999999998.9999999999999999999),(89, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_38_19_data_start_index = 87
    def test_cast_to_decimal256_39_38_from_decimal256_38_19_data_end_index = 90
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_39_0 values (90, 10),(91, 999999999999999999999999999999999999998),(92, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_39_0_data_start_index = 90
    def test_cast_to_decimal256_39_38_from_decimal256_39_0_data_end_index = 93
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_39_1 values (93, 10.9),(94, 99999999999999999999999999999999999998.9),(95, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_39_1_data_start_index = 93
    def test_cast_to_decimal256_39_38_from_decimal256_39_1_data_end_index = 96
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_39_19 values (96, 10.9999999999999999999),(97, 99999999999999999998.9999999999999999999),(98, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_39_19_data_start_index = 96
    def test_cast_to_decimal256_39_38_from_decimal256_39_19_data_end_index = 99
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_75_0 values (99, 10),(100, 999999999999999999999999999999999999999999999999999999999999999999999999998),(101, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_75_0_data_start_index = 99
    def test_cast_to_decimal256_39_38_from_decimal256_75_0_data_end_index = 102
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_75_1 values (102, 10.9),(103, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(104, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_75_1_data_start_index = 102
    def test_cast_to_decimal256_39_38_from_decimal256_75_1_data_end_index = 105
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_75_37 values (105, 10.9999999999999999999999999999999999999),(106, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(107, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_75_37_data_start_index = 105
    def test_cast_to_decimal256_39_38_from_decimal256_75_37_data_end_index = 108
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_75_74 values (108, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(109, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_75_74_data_start_index = 108
    def test_cast_to_decimal256_39_38_from_decimal256_75_74_data_end_index = 110
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_76_0 values (110, 10),(111, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(112, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_76_0_data_start_index = 110
    def test_cast_to_decimal256_39_38_from_decimal256_76_0_data_end_index = 113
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_75 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_76_1 values (113, 10.9),(114, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(115, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_76_1_data_start_index = 113
    def test_cast_to_decimal256_39_38_from_decimal256_76_1_data_end_index = 116
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_76 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_76_38 values (116, 10.99999999999999999999999999999999999999),(117, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(118, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_76_38_data_start_index = 116
    def test_cast_to_decimal256_39_38_from_decimal256_76_38_data_end_index = 119
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_77 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_38_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal256_39_38_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_38_from_decimal256_76_75 values (119, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(120, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_38_from_decimal256_76_75_data_start_index = 119
    def test_cast_to_decimal256_39_38_from_decimal256_76_75_data_end_index = 121
    for (int data_index = test_cast_to_decimal256_39_38_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal256_39_38_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_78 'select f1, cast(f2 as decimalv3(39, 38)) from test_cast_to_decimal256_39_38_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_38_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_38_0 values (121, 1),(122, 99999999999999999999999999999999999998),(123, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_38_0_data_start_index = 121
    def test_cast_to_decimal256_39_39_from_decimal256_38_0_data_end_index = 124
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_38_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_80 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_38_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_38_1 values (124, 1.9),(125, 9999999999999999999999999999999999998.9),(126, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_38_1_data_start_index = 124
    def test_cast_to_decimal256_39_39_from_decimal256_38_1_data_end_index = 127
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_38_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_81 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_38_19;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_38_19 values (127, 1.9999999999999999999),(128, 9999999999999999998.9999999999999999999),(129, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_38_19_data_start_index = 127
    def test_cast_to_decimal256_39_39_from_decimal256_38_19_data_end_index = 130
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_38_19_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_82 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_38_37;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_38_37 values (130, 1.9999999999999999999999999999999999999),(131, 8.9999999999999999999999999999999999999),(132, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_38_37_data_start_index = 130
    def test_cast_to_decimal256_39_39_from_decimal256_38_37_data_end_index = 133
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_38_37_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_83 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_39_0 values (133, 1),(134, 999999999999999999999999999999999999998),(135, 999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_39_0_data_start_index = 133
    def test_cast_to_decimal256_39_39_from_decimal256_39_0_data_end_index = 136
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_39_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_39_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_85 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_39_1 values (136, 1.9),(137, 99999999999999999999999999999999999998.9),(138, 99999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_39_1_data_start_index = 136
    def test_cast_to_decimal256_39_39_from_decimal256_39_1_data_end_index = 139
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_39_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_39_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_86 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_39_19 values (139, 1.9999999999999999999),(140, 99999999999999999998.9999999999999999999),(141, 99999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_39_19_data_start_index = 139
    def test_cast_to_decimal256_39_39_from_decimal256_39_19_data_end_index = 142
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_39_19_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_39_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_87 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_39_38 values (142, 1.99999999999999999999999999999999999999),(143, 8.99999999999999999999999999999999999999),(144, 9.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_39_38_data_start_index = 142
    def test_cast_to_decimal256_39_39_from_decimal256_39_38_data_end_index = 145
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_39_38_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_39_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_88 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_75_0 values (145, 1),(146, 999999999999999999999999999999999999999999999999999999999999999999999999998),(147, 999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_75_0_data_start_index = 145
    def test_cast_to_decimal256_39_39_from_decimal256_75_0_data_end_index = 148
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_75_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_75_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_90 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_75_1 values (148, 1.9),(149, 99999999999999999999999999999999999999999999999999999999999999999999999998.9),(150, 99999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_75_1_data_start_index = 148
    def test_cast_to_decimal256_39_39_from_decimal256_75_1_data_end_index = 151
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_75_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_75_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_91 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_75_37 values (151, 1.9999999999999999999999999999999999999),(152, 99999999999999999999999999999999999998.9999999999999999999999999999999999999),(153, 99999999999999999999999999999999999999.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_75_37_data_start_index = 151
    def test_cast_to_decimal256_39_39_from_decimal256_75_37_data_end_index = 154
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_75_37_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_75_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_92 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_75_74 values (154, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(155, 0.99999999999999999999999999999999999999999999999999999999999999999999999999),(156, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(157, 1.99999999999999999999999999999999999999999999999999999999999999999999999999),(158, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),
      (159, 8.99999999999999999999999999999999999999999999999999999999999999999999999999),(160, 9.99999999999999999999999999999999999999999999999999999999999999999999999999),(161, 9.99999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_75_74_data_start_index = 154
    def test_cast_to_decimal256_39_39_from_decimal256_75_74_data_end_index = 162
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_75_74_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_75_74_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_74 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_93 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_75_75 values (162, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(163, 0.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_75_75_data_start_index = 162
    def test_cast_to_decimal256_39_39_from_decimal256_75_75_data_end_index = 164
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_75_75_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_75_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_94 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_76_0 values (164, 1),(165, 9999999999999999999999999999999999999999999999999999999999999999999999999998),(166, 9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_76_0_data_start_index = 164
    def test_cast_to_decimal256_39_39_from_decimal256_76_0_data_end_index = 167
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_76_0_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_76_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_95 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_76_1 values (167, 1.9),(168, 999999999999999999999999999999999999999999999999999999999999999999999999998.9),(169, 999999999999999999999999999999999999999999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_76_1_data_start_index = 167
    def test_cast_to_decimal256_39_39_from_decimal256_76_1_data_end_index = 170
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_76_1_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_76_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_96 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_76_38 values (170, 1.99999999999999999999999999999999999999),(171, 99999999999999999999999999999999999998.99999999999999999999999999999999999999),(172, 99999999999999999999999999999999999999.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_76_38_data_start_index = 170
    def test_cast_to_decimal256_39_39_from_decimal256_76_38_data_end_index = 173
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_76_38_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_76_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_97 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_76_75 values (173, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(174, 0.999999999999999999999999999999999999999999999999999999999999999999999999999),(175, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(176, 1.999999999999999999999999999999999999999999999999999999999999999999999999999),(177, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),
      (178, 8.999999999999999999999999999999999999999999999999999999999999999999999999999),(179, 9.999999999999999999999999999999999999999999999999999999999999999999999999999),(180, 9.999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_76_75_data_start_index = 173
    def test_cast_to_decimal256_39_39_from_decimal256_76_75_data_end_index = 181
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_76_75_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_76_75_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_75 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_98 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal256_76_76 values (181, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999),(182, 0.9999999999999999999999999999999999999999999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal256_39_39_from_decimal256_76_76_data_start_index = 181
    def test_cast_to_decimal256_39_39_from_decimal256_76_76_data_end_index = 183
    for (int data_index = test_cast_to_decimal256_39_39_from_decimal256_76_76_data_start_index; data_index < test_cast_to_decimal256_39_39_from_decimal256_76_76_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_76 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_99 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal256_76_76 order by 1;'

}