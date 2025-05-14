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


suite("test_cast_to_decimal32_8_from_decimal128v3_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_19_0 values (0, 100000000),(1, 9999999999999999998),(2, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_19_0_data_start_index = 0
    def test_cast_to_decimal32_8_0_from_decimal128v3_19_0_data_end_index = 3
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_19_1 values (3, 99999999.9),(4, 99999999.9),(5, 100000000.9),(6, 100000000.9),(7, 999999999999999998.9),
      (8, 999999999999999998.9),(9, 999999999999999999.9),(10, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_19_1_data_start_index = 3
    def test_cast_to_decimal32_8_0_from_decimal128v3_19_1_data_end_index = 11
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_1 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_19_9 values (11, 99999999.999999999),(12, 99999999.999999999),(13, 100000000.999999999),(14, 100000000.999999999),(15, 9999999998.999999999),
      (16, 9999999998.999999999),(17, 9999999999.999999999),(18, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_19_9_data_start_index = 11
    def test_cast_to_decimal32_8_0_from_decimal128v3_19_9_data_end_index = 19
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_2 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_37_0 values (19, 100000000),(20, 9999999999999999999999999999999999998),(21, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_37_0_data_start_index = 19
    def test_cast_to_decimal32_8_0_from_decimal128v3_37_0_data_end_index = 22
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_5 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_37_1 values (22, 99999999.9),(23, 99999999.9),(24, 100000000.9),(25, 100000000.9),(26, 999999999999999999999999999999999998.9),
      (27, 999999999999999999999999999999999998.9),(28, 999999999999999999999999999999999999.9),(29, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_37_1_data_start_index = 22
    def test_cast_to_decimal32_8_0_from_decimal128v3_37_1_data_end_index = 30
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_6 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_37_18 values (30, 99999999.999999999999999999),(31, 99999999.999999999999999999),(32, 100000000.999999999999999999),(33, 100000000.999999999999999999),(34, 9999999999999999998.999999999999999999),
      (35, 9999999999999999998.999999999999999999),(36, 9999999999999999999.999999999999999999),(37, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_37_18_data_start_index = 30
    def test_cast_to_decimal32_8_0_from_decimal128v3_37_18_data_end_index = 38
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_7 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_38_0 values (38, 100000000),(39, 99999999999999999999999999999999999998),(40, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_38_0_data_start_index = 38
    def test_cast_to_decimal32_8_0_from_decimal128v3_38_0_data_end_index = 41
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_10 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_38_1 values (41, 99999999.9),(42, 99999999.9),(43, 100000000.9),(44, 100000000.9),(45, 9999999999999999999999999999999999998.9),
      (46, 9999999999999999999999999999999999998.9),(47, 9999999999999999999999999999999999999.9),(48, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_38_1_data_start_index = 41
    def test_cast_to_decimal32_8_0_from_decimal128v3_38_1_data_end_index = 49
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_11 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_0_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_8_0_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_0_from_decimal128v3_38_19 values (49, 99999999.9999999999999999999),(50, 99999999.9999999999999999999),(51, 100000000.9999999999999999999),(52, 100000000.9999999999999999999),(53, 9999999999999999998.9999999999999999999),
      (54, 9999999999999999998.9999999999999999999),(55, 9999999999999999999.9999999999999999999),(56, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_0_from_decimal128v3_38_19_data_start_index = 49
    def test_cast_to_decimal32_8_0_from_decimal128v3_38_19_data_end_index = 57
    for (int data_index = test_cast_to_decimal32_8_0_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal32_8_0_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_12 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal32_8_0_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_19_0 values (57, 10000000),(58, 9999999999999999998),(59, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_19_0_data_start_index = 57
    def test_cast_to_decimal32_8_1_from_decimal128v3_19_0_data_end_index = 60
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_15 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_19_1 values (60, 10000000.9),(61, 999999999999999998.9),(62, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_19_1_data_start_index = 60
    def test_cast_to_decimal32_8_1_from_decimal128v3_19_1_data_end_index = 63
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_16 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_19_9 values (63, 9999999.999999999),(64, 9999999.999999999),(65, 10000000.999999999),(66, 10000000.999999999),(67, 9999999998.999999999),
      (68, 9999999998.999999999),(69, 9999999999.999999999),(70, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_19_9_data_start_index = 63
    def test_cast_to_decimal32_8_1_from_decimal128v3_19_9_data_end_index = 71
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_17 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_37_0 values (71, 10000000),(72, 9999999999999999999999999999999999998),(73, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_37_0_data_start_index = 71
    def test_cast_to_decimal32_8_1_from_decimal128v3_37_0_data_end_index = 74
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_20 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_37_1 values (74, 10000000.9),(75, 999999999999999999999999999999999998.9),(76, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_37_1_data_start_index = 74
    def test_cast_to_decimal32_8_1_from_decimal128v3_37_1_data_end_index = 77
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_21 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_37_18 values (77, 9999999.999999999999999999),(78, 9999999.999999999999999999),(79, 10000000.999999999999999999),(80, 10000000.999999999999999999),(81, 9999999999999999998.999999999999999999),
      (82, 9999999999999999998.999999999999999999),(83, 9999999999999999999.999999999999999999),(84, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_37_18_data_start_index = 77
    def test_cast_to_decimal32_8_1_from_decimal128v3_37_18_data_end_index = 85
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_22 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_38_0 values (85, 10000000),(86, 99999999999999999999999999999999999998),(87, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_38_0_data_start_index = 85
    def test_cast_to_decimal32_8_1_from_decimal128v3_38_0_data_end_index = 88
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_25 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_38_1 values (88, 10000000.9),(89, 9999999999999999999999999999999999998.9),(90, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_38_1_data_start_index = 88
    def test_cast_to_decimal32_8_1_from_decimal128v3_38_1_data_end_index = 91
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_26 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_1_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_8_1_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_1_from_decimal128v3_38_19 values (91, 9999999.9999999999999999999),(92, 9999999.9999999999999999999),(93, 10000000.9999999999999999999),(94, 10000000.9999999999999999999),(95, 9999999999999999998.9999999999999999999),
      (96, 9999999999999999998.9999999999999999999),(97, 9999999999999999999.9999999999999999999),(98, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_1_from_decimal128v3_38_19_data_start_index = 91
    def test_cast_to_decimal32_8_1_from_decimal128v3_38_19_data_end_index = 99
    for (int data_index = test_cast_to_decimal32_8_1_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal32_8_1_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_27 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal32_8_1_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_19_0 values (99, 10000),(100, 9999999999999999998),(101, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_19_0_data_start_index = 99
    def test_cast_to_decimal32_8_4_from_decimal128v3_19_0_data_end_index = 102
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_30 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_19_1 values (102, 10000.9),(103, 999999999999999998.9),(104, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_19_1_data_start_index = 102
    def test_cast_to_decimal32_8_4_from_decimal128v3_19_1_data_end_index = 105
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_31 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_19_9 values (105, 9999.999999999),(106, 9999.999999999),(107, 10000.999999999),(108, 10000.999999999),(109, 9999999998.999999999),
      (110, 9999999998.999999999),(111, 9999999999.999999999),(112, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_19_9_data_start_index = 105
    def test_cast_to_decimal32_8_4_from_decimal128v3_19_9_data_end_index = 113
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_32 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_37_0 values (113, 10000),(114, 9999999999999999999999999999999999998),(115, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_37_0_data_start_index = 113
    def test_cast_to_decimal32_8_4_from_decimal128v3_37_0_data_end_index = 116
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_35 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_37_1 values (116, 10000.9),(117, 999999999999999999999999999999999998.9),(118, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_37_1_data_start_index = 116
    def test_cast_to_decimal32_8_4_from_decimal128v3_37_1_data_end_index = 119
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_36 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_37_18 values (119, 9999.999999999999999999),(120, 9999.999999999999999999),(121, 10000.999999999999999999),(122, 10000.999999999999999999),(123, 9999999999999999998.999999999999999999),
      (124, 9999999999999999998.999999999999999999),(125, 9999999999999999999.999999999999999999),(126, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_37_18_data_start_index = 119
    def test_cast_to_decimal32_8_4_from_decimal128v3_37_18_data_end_index = 127
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_37 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_38_0 values (127, 10000),(128, 99999999999999999999999999999999999998),(129, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_38_0_data_start_index = 127
    def test_cast_to_decimal32_8_4_from_decimal128v3_38_0_data_end_index = 130
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_40 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_38_1 values (130, 10000.9),(131, 9999999999999999999999999999999999998.9),(132, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_38_1_data_start_index = 130
    def test_cast_to_decimal32_8_4_from_decimal128v3_38_1_data_end_index = 133
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_41 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128v3_38_19 values (133, 9999.9999999999999999999),(134, 9999.9999999999999999999),(135, 10000.9999999999999999999),(136, 10000.9999999999999999999),(137, 9999999999999999998.9999999999999999999),
      (138, 9999999999999999998.9999999999999999999),(139, 9999999999999999999.9999999999999999999),(140, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_4_from_decimal128v3_38_19_data_start_index = 133
    def test_cast_to_decimal32_8_4_from_decimal128v3_38_19_data_end_index = 141
    for (int data_index = test_cast_to_decimal32_8_4_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal32_8_4_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_42 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_19_0 values (141, 10),(142, 9999999999999999998),(143, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_19_0_data_start_index = 141
    def test_cast_to_decimal32_8_7_from_decimal128v3_19_0_data_end_index = 144
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_45 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_19_1 values (144, 10.9),(145, 999999999999999998.9),(146, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_19_1_data_start_index = 144
    def test_cast_to_decimal32_8_7_from_decimal128v3_19_1_data_end_index = 147
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_46 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_19_9 values (147, 9.999999999),(148, 9.999999999),(149, 10.999999999),(150, 10.999999999),(151, 9999999998.999999999),
      (152, 9999999998.999999999),(153, 9999999999.999999999),(154, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_19_9_data_start_index = 147
    def test_cast_to_decimal32_8_7_from_decimal128v3_19_9_data_end_index = 155
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_47 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_19_18 values (155, 9.999999999999999999),(156, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_19_18_data_start_index = 155
    def test_cast_to_decimal32_8_7_from_decimal128v3_19_18_data_end_index = 157
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_48 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_37_0 values (157, 10),(158, 9999999999999999999999999999999999998),(159, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_37_0_data_start_index = 157
    def test_cast_to_decimal32_8_7_from_decimal128v3_37_0_data_end_index = 160
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_50 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_37_1 values (160, 10.9),(161, 999999999999999999999999999999999998.9),(162, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_37_1_data_start_index = 160
    def test_cast_to_decimal32_8_7_from_decimal128v3_37_1_data_end_index = 163
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_51 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_37_18 values (163, 9.999999999999999999),(164, 9.999999999999999999),(165, 10.999999999999999999),(166, 10.999999999999999999),(167, 9999999999999999998.999999999999999999),
      (168, 9999999999999999998.999999999999999999),(169, 9999999999999999999.999999999999999999),(170, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_37_18_data_start_index = 163
    def test_cast_to_decimal32_8_7_from_decimal128v3_37_18_data_end_index = 171
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_52 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_37_36 values (171, 9.999999999999999999999999999999999999),(172, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_37_36_data_start_index = 171
    def test_cast_to_decimal32_8_7_from_decimal128v3_37_36_data_end_index = 173
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_53 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_38_0 values (173, 10),(174, 99999999999999999999999999999999999998),(175, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_38_0_data_start_index = 173
    def test_cast_to_decimal32_8_7_from_decimal128v3_38_0_data_end_index = 176
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_55 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_38_1 values (176, 10.9),(177, 9999999999999999999999999999999999998.9),(178, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_38_1_data_start_index = 176
    def test_cast_to_decimal32_8_7_from_decimal128v3_38_1_data_end_index = 179
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_56 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_38_19 values (179, 9.9999999999999999999),(180, 9.9999999999999999999),(181, 10.9999999999999999999),(182, 10.9999999999999999999),(183, 9999999999999999998.9999999999999999999),
      (184, 9999999999999999998.9999999999999999999),(185, 9999999999999999999.9999999999999999999),(186, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_38_19_data_start_index = 179
    def test_cast_to_decimal32_8_7_from_decimal128v3_38_19_data_end_index = 187
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_57 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128v3_38_37 values (187, 9.9999999999999999999999999999999999999),(188, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_7_from_decimal128v3_38_37_data_start_index = 187
    def test_cast_to_decimal32_8_7_from_decimal128v3_38_37_data_end_index = 189
    for (int data_index = test_cast_to_decimal32_8_7_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal32_8_7_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_58 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_19_0 values (189, 1),(190, 9999999999999999998),(191, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_19_0_data_start_index = 189
    def test_cast_to_decimal32_8_8_from_decimal128v3_19_0_data_end_index = 192
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_19_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_19_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_60 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_19_1 values (192, 1.9),(193, 999999999999999998.9),(194, 999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_19_1_data_start_index = 192
    def test_cast_to_decimal32_8_8_from_decimal128v3_19_1_data_end_index = 195
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_19_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_19_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_61 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_19_9 values (195, 0.999999999),(196, 0.999999999),(197, 1.999999999),(198, 1.999999999),(199, 9999999998.999999999),
      (200, 9999999998.999999999),(201, 9999999999.999999999),(202, 9999999999.999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_19_9_data_start_index = 195
    def test_cast_to_decimal32_8_8_from_decimal128v3_19_9_data_end_index = 203
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_19_9_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_19_9_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_9 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_62 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_19_18 values (203, 0.999999999999999999),(204, 0.999999999999999999),(205, 1.999999999999999999),(206, 1.999999999999999999),(207, 8.999999999999999999),
      (208, 8.999999999999999999),(209, 9.999999999999999999),(210, 9.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_19_18_data_start_index = 203
    def test_cast_to_decimal32_8_8_from_decimal128v3_19_18_data_end_index = 211
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_19_18_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_19_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_63 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_19_19 values (211, 0.9999999999999999999),(212, 0.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_19_19_data_start_index = 211
    def test_cast_to_decimal32_8_8_from_decimal128v3_19_19_data_end_index = 213
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_19_19_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_19_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_64 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_37_0 values (213, 1),(214, 9999999999999999999999999999999999998),(215, 9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_37_0_data_start_index = 213
    def test_cast_to_decimal32_8_8_from_decimal128v3_37_0_data_end_index = 216
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_37_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_37_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_65 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_37_1 values (216, 1.9),(217, 999999999999999999999999999999999998.9),(218, 999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_37_1_data_start_index = 216
    def test_cast_to_decimal32_8_8_from_decimal128v3_37_1_data_end_index = 219
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_37_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_37_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_66 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_37_18 values (219, 0.999999999999999999),(220, 0.999999999999999999),(221, 1.999999999999999999),(222, 1.999999999999999999),(223, 9999999999999999998.999999999999999999),
      (224, 9999999999999999998.999999999999999999),(225, 9999999999999999999.999999999999999999),(226, 9999999999999999999.999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_37_18_data_start_index = 219
    def test_cast_to_decimal32_8_8_from_decimal128v3_37_18_data_end_index = 227
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_37_18_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_37_18_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_18 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_67 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_37_36 values (227, 0.999999999999999999999999999999999999),(228, 0.999999999999999999999999999999999999),(229, 1.999999999999999999999999999999999999),(230, 1.999999999999999999999999999999999999),(231, 8.999999999999999999999999999999999999),
      (232, 8.999999999999999999999999999999999999),(233, 9.999999999999999999999999999999999999),(234, 9.999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_37_36_data_start_index = 227
    def test_cast_to_decimal32_8_8_from_decimal128v3_37_36_data_end_index = 235
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_37_36_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_37_36_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_36 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_68 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_37_37 values (235, 0.9999999999999999999999999999999999999),(236, 0.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_37_37_data_start_index = 235
    def test_cast_to_decimal32_8_8_from_decimal128v3_37_37_data_end_index = 237
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_37_37_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_37_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_69 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_38_0 values (237, 1),(238, 99999999999999999999999999999999999998),(239, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_38_0_data_start_index = 237
    def test_cast_to_decimal32_8_8_from_decimal128v3_38_0_data_end_index = 240
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_38_0_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_38_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_70 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_38_1 values (240, 1.9),(241, 9999999999999999999999999999999999998.9),(242, 9999999999999999999999999999999999999.9);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_38_1_data_start_index = 240
    def test_cast_to_decimal32_8_8_from_decimal128v3_38_1_data_end_index = 243
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_38_1_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_38_1_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_1 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_71 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_38_19 values (243, 0.9999999999999999999),(244, 0.9999999999999999999),(245, 1.9999999999999999999),(246, 1.9999999999999999999),(247, 9999999999999999998.9999999999999999999),
      (248, 9999999999999999998.9999999999999999999),(249, 9999999999999999999.9999999999999999999),(250, 9999999999999999999.9999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_38_19_data_start_index = 243
    def test_cast_to_decimal32_8_8_from_decimal128v3_38_19_data_end_index = 251
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_38_19_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_38_19_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_19 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_72 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_38_37 values (251, 0.9999999999999999999999999999999999999),(252, 0.9999999999999999999999999999999999999),(253, 1.9999999999999999999999999999999999999),(254, 1.9999999999999999999999999999999999999),(255, 8.9999999999999999999999999999999999999),
      (256, 8.9999999999999999999999999999999999999),(257, 9.9999999999999999999999999999999999999),(258, 9.9999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_38_37_data_start_index = 251
    def test_cast_to_decimal32_8_8_from_decimal128v3_38_37_data_end_index = 259
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_38_37_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_38_37_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_37 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_73 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_8_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal32_8_8_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_8_from_decimal128v3_38_38 values (259, 0.99999999999999999999999999999999999999),(260, 0.99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal32_8_8_from_decimal128v3_38_38_data_start_index = 259
    def test_cast_to_decimal32_8_8_from_decimal128v3_38_38_data_end_index = 261
    for (int data_index = test_cast_to_decimal32_8_8_from_decimal128v3_38_38_data_start_index; data_index < test_cast_to_decimal32_8_8_from_decimal128v3_38_38_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_38 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_74 'select f1, cast(f2 as decimalv3(8, 8)) from test_cast_to_decimal32_8_8_from_decimal128v3_38_38 order by 1;'

}