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


suite("test_cast_to_decimal32_1_1_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_1_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_1_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_0 values (5, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_2 values (10, "0.00"),(11, "0.01"),(12, "0.09"),(13, "0.90"),(14, "0.91");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_3 values (15, "0.000"),(16, "0.001"),(17, "0.009"),(18, "0.099"),(19, "0.900"),(20, "0.901");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_4_4 values (21, "0.0000"),(22, "0.0001"),(23, "0.0009"),(24, "0.0999"),(25, "0.9000"),(26, "0.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_0 values (27, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_1 values (28, "0.0"),(29, "0.1"),(30, "0.8"),(31, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_4 values (32, "0.0000"),(33, "0.0001"),(34, "0.0009"),(35, "0.0999"),(36, "0.9000"),(37, "0.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_7 values (38, "0.0000000"),(39, "0.0000001"),(40, "0.0000009"),(41, "0.0999999"),(42, "0.9000000"),(43, "0.9000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_8_8 values (44, "0.00000000"),(45, "0.00000001"),(46, "0.00000009"),(47, "0.09999999"),(48, "0.90000000"),(49, "0.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_0 values (50, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_1 values (51, "0.0"),(52, "0.1"),(53, "0.8"),(54, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_4 values (55, "0.0000"),(56, "0.0001"),(57, "0.0009"),(58, "0.0999"),(59, "0.9000"),(60, "0.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_8 values (61, "0.00000000"),(62, "0.00000001"),(63, "0.00000009"),(64, "0.09999999"),(65, "0.90000000"),(66, "0.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_1_1_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_decimal32_9_9 values (67, "0.000000000"),(68, "0.000000001"),(69, "0.000000009"),(70, "0.099999999"),(71, "0.900000000"),(72, "0.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_decimal32_9_9 order by 1;'

}