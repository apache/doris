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


suite("test_cast_to_decimal32_9_9_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_5 values (5, "0.00000"),(6, "0.00001"),(7, "0.00009"),(8, "0.09999"),(9, "0.90000"),(10, "0.90001"),(11, "0.99998"),(12, "0.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_9 values (13, "0.000000000"),(14, "0.000000001"),(15, "0.000000009"),(16, "0.099999999"),(17, "0.900000000"),(18, "0.900000001"),(19, "0.999999998"),(20, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_10_10 values (21, "0.0000000000"),(22, "0.0000000001"),(23, "0.0000000009"),(24, "0.0999999999"),(25, "0.9000000000"),(26, "0.9000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_0 values (27, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_1 values (28, "0.0"),(29, "0.1"),(30, "0.8"),(31, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_8 values (32, "0.00000000"),(33, "0.00000001"),(34, "0.00000009"),(35, "0.09999999"),(36, "0.90000000"),(37, "0.90000001"),(38, "0.99999998"),(39, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_16 values (40, "0.0000000000000000"),(41, "0.0000000000000001"),(42, "0.0000000000000009"),(43, "0.0999999999999999"),(44, "0.9000000000000000"),(45, "0.9000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_17_17 values (46, "0.00000000000000000"),(47, "0.00000000000000001"),(48, "0.00000000000000009"),(49, "0.09999999999999999"),(50, "0.90000000000000000"),(51, "0.90000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_0 values (52, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_1 values (53, "0.0"),(54, "0.1"),(55, "0.8"),(56, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_9 values (57, "0.000000000"),(58, "0.000000001"),(59, "0.000000009"),(60, "0.099999999"),(61, "0.900000000"),(62, "0.900000001"),(63, "0.999999998"),(64, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_17 values (65, "0.00000000000000000"),(66, "0.00000000000000001"),(67, "0.00000000000000009"),(68, "0.09999999999999999"),(69, "0.90000000000000000"),(70, "0.90000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_9_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_9_9_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_decimal64_18_18 values (71, "0.000000000000000000"),(72, "0.000000000000000001"),(73, "0.000000000000000009"),(74, "0.099999999999999999"),(75, "0.900000000000000000"),(76, "0.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_decimal64_18_18 order by 1;'

}