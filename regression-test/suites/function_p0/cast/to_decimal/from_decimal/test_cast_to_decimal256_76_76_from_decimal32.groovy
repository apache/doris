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


suite("test_cast_to_decimal256_76_76_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_1_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_1_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_4_0 values (5, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_4_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_4_2 values (10, "0.00"),(11, "0.01"),(12, "0.09"),(13, "0.90"),(14, "0.91"),(15, "0.98"),(16, "0.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_4_3 values (17, "0.000"),(18, "0.001"),(19, "0.009"),(20, "0.099"),(21, "0.900"),(22, "0.901"),(23, "0.998"),(24, "0.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_4_4 values (25, "0.0000"),(26, "0.0001"),(27, "0.0009"),(28, "0.0999"),(29, "0.9000"),(30, "0.9001"),(31, "0.9998"),(32, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_8_0 values (33, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_8_1 values (34, "0.0"),(35, "0.1"),(36, "0.8"),(37, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_8_4 values (38, "0.0000"),(39, "0.0001"),(40, "0.0009"),(41, "0.0999"),(42, "0.9000"),(43, "0.9001"),(44, "0.9998"),(45, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_8_7 values (46, "0.0000000"),(47, "0.0000001"),(48, "0.0000009"),(49, "0.0999999"),(50, "0.9000000"),(51, "0.9000001"),(52, "0.9999998"),(53, "0.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_8_8 values (54, "0.00000000"),(55, "0.00000001"),(56, "0.00000009"),(57, "0.09999999"),(58, "0.90000000"),(59, "0.90000001"),(60, "0.99999998"),(61, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_9_0 values (62, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_9_1 values (63, "0.0"),(64, "0.1"),(65, "0.8"),(66, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_9_4 values (67, "0.0000"),(68, "0.0001"),(69, "0.0009"),(70, "0.0999"),(71, "0.9000"),(72, "0.9001"),(73, "0.9998"),(74, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_9_8 values (75, "0.00000000"),(76, "0.00000001"),(77, "0.00000009"),(78, "0.09999999"),(79, "0.90000000"),(80, "0.90000001"),(81, "0.99999998"),(82, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_76_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal256_76_76_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_76_from_decimal32_9_9 values (83, "0.000000000"),(84, "0.000000001"),(85, "0.000000009"),(86, "0.099999999"),(87, "0.900000000"),(88, "0.900000001"),(89, "0.999999998"),(90, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(76, 76)) from test_cast_to_decimal256_76_76_from_decimal32_9_9 order by 1;'

}