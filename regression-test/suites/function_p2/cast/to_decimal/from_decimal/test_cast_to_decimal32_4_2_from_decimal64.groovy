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


suite("test_cast_to_decimal32_4_2_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_0_0_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_0_0_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_0_0_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_0_0_not_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_0_0_not_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_1_1_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_1_1_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_1_1_not_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_5_2_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_5_2_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_5_2_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "9.00000"),(9, "9.00001"),(10, "9.00009"),(11, "9.09999"),(12, "9.90000"),(13, "9.90001"),(14, "9.99998"),(15, "9.99999"),(16, "90.00000"),(17, "90.00001"),(18, "90.00009"),(19, "90.09999"),
      (20, "90.90000"),(21, "90.90001"),(22, "90.99998"),(23, "90.99999"),(24, "91.00000"),(25, "91.00001"),(26, "91.00009"),(27, "91.09999"),(28, "91.90000"),(29, "91.90001"),(30, "91.99998"),(31, "91.99999"),(32, "98.00000"),(33, "98.00001"),(34, "98.00009"),(35, "98.09999"),(36, "98.90000"),(37, "98.90001"),(38, "98.99998"),(39, "98.99999"),
      (40, "99.00000"),(41, "99.00001"),(42, "99.00009"),(43, "99.09999"),(44, "99.90000"),(45, "99.90001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_5_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_5_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_5_2_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_5_2_not_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_5_2_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "9.00000"),(9, "9.00001"),(10, "9.00009"),(11, "9.09999"),(12, "9.90000"),(13, "9.90001"),(14, "9.99998"),(15, "9.99999"),(16, "90.00000"),(17, "90.00001"),(18, "90.00009"),(19, "90.09999"),
      (20, "90.90000"),(21, "90.90001"),(22, "90.99998"),(23, "90.99999"),(24, "91.00000"),(25, "91.00001"),(26, "91.00009"),(27, "91.09999"),(28, "91.90000"),(29, "91.90001"),(30, "91.99998"),(31, "91.99999"),(32, "98.00000"),(33, "98.00001"),(34, "98.00009"),(35, "98.09999"),(36, "98.90000"),(37, "98.90001"),(38, "98.99998"),(39, "98.99999"),
      (40, "99.00000"),(41, "99.00001"),(42, "99.00009"),(43, "99.09999"),(44, "99.90000"),(45, "99.90001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_5_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_5_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_9_3_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_9_3_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_9_3_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_9_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_9_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_9_3_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_9_3_not_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_9_3_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_9_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_9_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_10_4_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_10_4_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_10_4_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_10_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_10_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_10_10_4_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_10_10_4_not_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_10_10_4_not_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_10_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_10_10_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_0_5_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_0_5_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_0_5_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_0_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_0_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_0_5_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_0_5_not_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_0_5_not_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_0_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_0_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_1_6_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_1_6_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_1_6_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_1_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_1_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_1_6_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_1_6_not_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_1_6_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_1_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_1_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_8_7_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_8_7_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_8_7_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "9.00000000"),(9, "9.00000001"),(10, "9.00000009"),(11, "9.09999999"),(12, "9.90000000"),(13, "9.90000001"),(14, "9.99999998"),(15, "9.99999999"),(16, "90.00000000"),(17, "90.00000001"),(18, "90.00000009"),(19, "90.09999999"),
      (20, "90.90000000"),(21, "90.90000001"),(22, "90.99999998"),(23, "90.99999999"),(24, "91.00000000"),(25, "91.00000001"),(26, "91.00000009"),(27, "91.09999999"),(28, "91.90000000"),(29, "91.90000001"),(30, "91.99999998"),(31, "91.99999999"),(32, "98.00000000"),(33, "98.00000001"),(34, "98.00000009"),(35, "98.09999999"),(36, "98.90000000"),(37, "98.90000001"),(38, "98.99999998"),(39, "98.99999999"),
      (40, "99.00000000"),(41, "99.00000001"),(42, "99.00000009"),(43, "99.09999999"),(44, "99.90000000"),(45, "99.90000001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_8_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_8_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_8_7_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_8_7_not_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_8_7_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "9.00000000"),(9, "9.00000001"),(10, "9.00000009"),(11, "9.09999999"),(12, "9.90000000"),(13, "9.90000001"),(14, "9.99999998"),(15, "9.99999999"),(16, "90.00000000"),(17, "90.00000001"),(18, "90.00000009"),(19, "90.09999999"),
      (20, "90.90000000"),(21, "90.90000001"),(22, "90.99999998"),(23, "90.99999999"),(24, "91.00000000"),(25, "91.00000001"),(26, "91.00000009"),(27, "91.09999999"),(28, "91.90000000"),(29, "91.90000001"),(30, "91.99999998"),(31, "91.99999999"),(32, "98.00000000"),(33, "98.00000001"),(34, "98.00000009"),(35, "98.09999999"),(36, "98.90000000"),(37, "98.90000001"),(38, "98.99999998"),(39, "98.99999999"),
      (40, "99.00000000"),(41, "99.00000001"),(42, "99.00000009"),(43, "99.09999999"),(44, "99.90000000"),(45, "99.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_8_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_8_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_16_8_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_16_8_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_16_8_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001"),(6, "0.9999999999999998"),(7, "0.9999999999999999"),(8, "8.0000000000000000"),(9, "8.0000000000000001"),(10, "8.0000000000000009"),(11, "8.0999999999999999"),(12, "8.9000000000000000"),(13, "8.9000000000000001"),(14, "8.9999999999999998"),(15, "8.9999999999999999"),(16, "9.0000000000000000"),(17, "9.0000000000000001"),(18, "9.0000000000000009"),(19, "9.0999999999999999"),
      (20, "9.9000000000000000"),(21, "9.9000000000000001"),(22, "9.9999999999999998"),(23, "9.9999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_16_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_16_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_16_8_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_16_8_not_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_16_8_not_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001"),(6, "0.9999999999999998"),(7, "0.9999999999999999"),(8, "8.0000000000000000"),(9, "8.0000000000000001"),(10, "8.0000000000000009"),(11, "8.0999999999999999"),(12, "8.9000000000000000"),(13, "8.9000000000000001"),(14, "8.9999999999999998"),(15, "8.9999999999999999"),(16, "9.0000000000000000"),(17, "9.0000000000000001"),(18, "9.0000000000000009"),(19, "9.0999999999999999"),
      (20, "9.9000000000000000"),(21, "9.9000000000000001"),(22, "9.9999999999999998"),(23, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_16_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_16_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_17_9_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_17_9_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_17_9_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_17_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_17_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_17_17_9_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_17_17_9_not_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_17_17_9_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_17_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_17_17_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_0_10_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_0_10_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_0_10_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_0_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_0_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_0_10_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_0_10_not_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_0_10_not_nullable values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_0_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_0_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_1_11_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_1_11_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_1_11_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_1_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_1_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_1_11_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_1_11_not_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_1_11_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9.0"),(5, "9.1"),(6, "9.8"),(7, "9.9"),(8, "90.0"),(9, "90.1"),(10, "90.8"),(11, "90.9"),(12, "91.0"),(13, "91.1"),(14, "91.8"),(15, "91.9"),(16, "98.0"),(17, "98.1"),(18, "98.8"),(19, "98.9"),
      (20, "99.0"),(21, "99.1"),(22, "99.8"),(23, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_1_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_1_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_9_12_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_9_12_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_9_12_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "9.000000000"),(9, "9.000000001"),(10, "9.000000009"),(11, "9.099999999"),(12, "9.900000000"),(13, "9.900000001"),(14, "9.999999998"),(15, "9.999999999"),(16, "90.000000000"),(17, "90.000000001"),(18, "90.000000009"),(19, "90.099999999"),
      (20, "90.900000000"),(21, "90.900000001"),(22, "90.999999998"),(23, "90.999999999"),(24, "91.000000000"),(25, "91.000000001"),(26, "91.000000009"),(27, "91.099999999"),(28, "91.900000000"),(29, "91.900000001"),(30, "91.999999998"),(31, "91.999999999"),(32, "98.000000000"),(33, "98.000000001"),(34, "98.000000009"),(35, "98.099999999"),(36, "98.900000000"),(37, "98.900000001"),(38, "98.999999998"),(39, "98.999999999"),
      (40, "99.000000000"),(41, "99.000000001"),(42, "99.000000009"),(43, "99.099999999"),(44, "99.900000000"),(45, "99.900000001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_9_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_9_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_9_12_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_9_12_not_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_9_12_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "9.000000000"),(9, "9.000000001"),(10, "9.000000009"),(11, "9.099999999"),(12, "9.900000000"),(13, "9.900000001"),(14, "9.999999998"),(15, "9.999999999"),(16, "90.000000000"),(17, "90.000000001"),(18, "90.000000009"),(19, "90.099999999"),
      (20, "90.900000000"),(21, "90.900000001"),(22, "90.999999998"),(23, "90.999999999"),(24, "91.000000000"),(25, "91.000000001"),(26, "91.000000009"),(27, "91.099999999"),(28, "91.900000000"),(29, "91.900000001"),(30, "91.999999998"),(31, "91.999999999"),(32, "98.000000000"),(33, "98.000000001"),(34, "98.000000009"),(35, "98.099999999"),(36, "98.900000000"),(37, "98.900000001"),(38, "98.999999998"),(39, "98.999999999"),
      (40, "99.000000000"),(41, "99.000000001"),(42, "99.000000009"),(43, "99.099999999"),(44, "99.900000000"),(45, "99.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_9_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_9_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_17_13_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_17_13_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_17_13_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_17_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_17_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_17_13_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_17_13_not_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_17_13_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_17_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_17_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_18_14_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_18_14_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_18_14_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_18_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_18_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_2_from_decimal_18_18_14_not_nullable;"
    sql "create table test_cast_to_decimal_4_2_from_decimal_18_18_14_not_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_2_from_decimal_18_18_14_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_18_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal_4_2_from_decimal_18_18_14_not_nullable order by 1;'

}