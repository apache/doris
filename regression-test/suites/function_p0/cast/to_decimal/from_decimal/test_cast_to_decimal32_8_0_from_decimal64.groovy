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


suite("test_cast_to_decimal32_8_0_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_0_0_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_0_0_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_0_0_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_0_0_not_nullable(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_0_0_not_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_1_1_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_1_1_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_1_1_not_nullable(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_5_2_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_5_2_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_5_2_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "9999.00000"),(9, "9999.00001"),(10, "9999.00009"),(11, "9999.09999"),(12, "9999.90000"),(13, "9999.90001"),(14, "9999.99998"),(15, "9999.99999"),(16, "90000.00000"),(17, "90000.00001"),(18, "90000.00009"),(19, "90000.09999"),
      (20, "90000.90000"),(21, "90000.90001"),(22, "90000.99998"),(23, "90000.99999"),(24, "90001.00000"),(25, "90001.00001"),(26, "90001.00009"),(27, "90001.09999"),(28, "90001.90000"),(29, "90001.90001"),(30, "90001.99998"),(31, "90001.99999"),(32, "99998.00000"),(33, "99998.00001"),(34, "99998.00009"),(35, "99998.09999"),(36, "99998.90000"),(37, "99998.90001"),(38, "99998.99998"),(39, "99998.99999"),
      (40, "99999.00000"),(41, "99999.00001"),(42, "99999.00009"),(43, "99999.09999"),(44, "99999.90000"),(45, "99999.90001"),(46, "99999.99998"),(47, "99999.99999")
      ,(48, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_5_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_5_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_5_2_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_5_2_not_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_5_2_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "9999.00000"),(9, "9999.00001"),(10, "9999.00009"),(11, "9999.09999"),(12, "9999.90000"),(13, "9999.90001"),(14, "9999.99998"),(15, "9999.99999"),(16, "90000.00000"),(17, "90000.00001"),(18, "90000.00009"),(19, "90000.09999"),
      (20, "90000.90000"),(21, "90000.90001"),(22, "90000.99998"),(23, "90000.99999"),(24, "90001.00000"),(25, "90001.00001"),(26, "90001.00009"),(27, "90001.09999"),(28, "90001.90000"),(29, "90001.90001"),(30, "90001.99998"),(31, "90001.99999"),(32, "99998.00000"),(33, "99998.00001"),(34, "99998.00009"),(35, "99998.09999"),(36, "99998.90000"),(37, "99998.90001"),(38, "99998.99998"),(39, "99998.99999"),
      (40, "99999.00000"),(41, "99999.00001"),(42, "99999.00009"),(43, "99999.09999"),(44, "99999.90000"),(45, "99999.90001"),(46, "99999.99998"),(47, "99999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_5_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_5_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_9_3_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_9_3_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_9_3_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_9_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_9_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_9_3_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_9_3_not_nullable(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_9_3_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_9_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_9_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_10_4_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_10_4_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_10_4_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_10_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_10_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_10_10_4_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_10_10_4_not_nullable(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_10_10_4_not_nullable values (0, "0.0000000000"),(1, "0.0000000001"),(2, "0.0000000009"),(3, "0.0999999999"),(4, "0.9000000000"),(5, "0.9000000001"),(6, "0.9999999998"),(7, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_10_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_10_10_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_0_5_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_0_5_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_0_5_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_0_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_0_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_0_5_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_0_5_not_nullable(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_0_5_not_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_0_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_0_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_1_6_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_1_6_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_1_6_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_1_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_1_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_1_6_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_1_6_not_nullable(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_1_6_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_1_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_1_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_8_7_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_8_7_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_8_7_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "9999999.00000000"),(9, "9999999.00000001"),(10, "9999999.00000009"),(11, "9999999.09999999"),(12, "9999999.90000000"),(13, "9999999.90000001"),(14, "9999999.99999998"),(15, "9999999.99999999"),(16, "90000000.00000000"),(17, "90000000.00000001"),(18, "90000000.00000009"),(19, "90000000.09999999"),
      (20, "90000000.90000000"),(21, "90000000.90000001"),(22, "90000000.99999998"),(23, "90000000.99999999"),(24, "90000001.00000000"),(25, "90000001.00000001"),(26, "90000001.00000009"),(27, "90000001.09999999"),(28, "90000001.90000000"),(29, "90000001.90000001"),(30, "90000001.99999998"),(31, "90000001.99999999"),(32, "99999998.00000000"),(33, "99999998.00000001"),(34, "99999998.00000009"),(35, "99999998.09999999"),(36, "99999998.90000000"),(37, "99999998.90000001"),(38, "99999998.99999998"),(39, "99999998.99999999"),
      (40, "99999999.00000000"),(41, "99999999.00000001"),(42, "99999999.00000009"),(43, "99999999.09999999")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_8_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_8_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_8_7_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_8_7_not_nullable(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_8_7_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "9999999.00000000"),(9, "9999999.00000001"),(10, "9999999.00000009"),(11, "9999999.09999999"),(12, "9999999.90000000"),(13, "9999999.90000001"),(14, "9999999.99999998"),(15, "9999999.99999999"),(16, "90000000.00000000"),(17, "90000000.00000001"),(18, "90000000.00000009"),(19, "90000000.09999999"),
      (20, "90000000.90000000"),(21, "90000000.90000001"),(22, "90000000.99999998"),(23, "90000000.99999999"),(24, "90000001.00000000"),(25, "90000001.00000001"),(26, "90000001.00000009"),(27, "90000001.09999999"),(28, "90000001.90000000"),(29, "90000001.90000001"),(30, "90000001.99999998"),(31, "90000001.99999999"),(32, "99999998.00000000"),(33, "99999998.00000001"),(34, "99999998.00000009"),(35, "99999998.09999999"),(36, "99999998.90000000"),(37, "99999998.90000001"),(38, "99999998.99999998"),(39, "99999998.99999999"),
      (40, "99999999.00000000"),(41, "99999999.00000001"),(42, "99999999.00000009"),(43, "99999999.09999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_8_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_8_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_16_8_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_16_8_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_16_8_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001"),(6, "0.9999999999999998"),(7, "0.9999999999999999"),(8, "8.0000000000000000"),(9, "8.0000000000000001"),(10, "8.0000000000000009"),(11, "8.0999999999999999"),(12, "8.9000000000000000"),(13, "8.9000000000000001"),(14, "8.9999999999999998"),(15, "8.9999999999999999"),(16, "9.0000000000000000"),(17, "9.0000000000000001"),(18, "9.0000000000000009"),(19, "9.0999999999999999"),
      (20, "9.9000000000000000"),(21, "9.9000000000000001"),(22, "9.9999999999999998"),(23, "9.9999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_16_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_16_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_16_8_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_16_8_not_nullable(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_16_8_not_nullable values (0, "0.0000000000000000"),(1, "0.0000000000000001"),(2, "0.0000000000000009"),(3, "0.0999999999999999"),(4, "0.9000000000000000"),(5, "0.9000000000000001"),(6, "0.9999999999999998"),(7, "0.9999999999999999"),(8, "8.0000000000000000"),(9, "8.0000000000000001"),(10, "8.0000000000000009"),(11, "8.0999999999999999"),(12, "8.9000000000000000"),(13, "8.9000000000000001"),(14, "8.9999999999999998"),(15, "8.9999999999999999"),(16, "9.0000000000000000"),(17, "9.0000000000000001"),(18, "9.0000000000000009"),(19, "9.0999999999999999"),
      (20, "9.9000000000000000"),(21, "9.9000000000000001"),(22, "9.9999999999999998"),(23, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_16_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_16_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_17_9_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_17_9_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_17_9_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_17_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_17_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_17_17_9_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_17_17_9_not_nullable(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_17_17_9_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_17_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_17_17_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_0_10_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_0_10_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_0_10_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_0_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_0_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_0_10_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_0_10_not_nullable(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_0_10_not_nullable values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_0_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_0_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_1_11_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_1_11_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_1_11_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_1_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_1_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_1_11_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_1_11_not_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_1_11_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "9999999.0"),(5, "9999999.1"),(6, "9999999.8"),(7, "9999999.9"),(8, "90000000.0"),(9, "90000000.1"),(10, "90000000.8"),(11, "90000000.9"),(12, "90000001.0"),(13, "90000001.1"),(14, "90000001.8"),(15, "90000001.9"),(16, "99999998.0"),(17, "99999998.1"),(18, "99999998.8"),(19, "99999998.9"),
      (20, "99999999.0"),(21, "99999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_1_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_1_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_9_12_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_9_12_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_9_12_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "9999999.000000000"),(9, "9999999.000000001"),(10, "9999999.000000009"),(11, "9999999.099999999"),(12, "9999999.900000000"),(13, "9999999.900000001"),(14, "9999999.999999998"),(15, "9999999.999999999"),(16, "90000000.000000000"),(17, "90000000.000000001"),(18, "90000000.000000009"),(19, "90000000.099999999"),
      (20, "90000000.900000000"),(21, "90000000.900000001"),(22, "90000000.999999998"),(23, "90000000.999999999"),(24, "90000001.000000000"),(25, "90000001.000000001"),(26, "90000001.000000009"),(27, "90000001.099999999"),(28, "90000001.900000000"),(29, "90000001.900000001"),(30, "90000001.999999998"),(31, "90000001.999999999"),(32, "99999998.000000000"),(33, "99999998.000000001"),(34, "99999998.000000009"),(35, "99999998.099999999"),(36, "99999998.900000000"),(37, "99999998.900000001"),(38, "99999998.999999998"),(39, "99999998.999999999"),
      (40, "99999999.000000000"),(41, "99999999.000000001"),(42, "99999999.000000009"),(43, "99999999.099999999")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_9_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_9_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_9_12_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_9_12_not_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_9_12_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "9999999.000000000"),(9, "9999999.000000001"),(10, "9999999.000000009"),(11, "9999999.099999999"),(12, "9999999.900000000"),(13, "9999999.900000001"),(14, "9999999.999999998"),(15, "9999999.999999999"),(16, "90000000.000000000"),(17, "90000000.000000001"),(18, "90000000.000000009"),(19, "90000000.099999999"),
      (20, "90000000.900000000"),(21, "90000000.900000001"),(22, "90000000.999999998"),(23, "90000000.999999999"),(24, "90000001.000000000"),(25, "90000001.000000001"),(26, "90000001.000000009"),(27, "90000001.099999999"),(28, "90000001.900000000"),(29, "90000001.900000001"),(30, "90000001.999999998"),(31, "90000001.999999999"),(32, "99999998.000000000"),(33, "99999998.000000001"),(34, "99999998.000000009"),(35, "99999998.099999999"),(36, "99999998.900000000"),(37, "99999998.900000001"),(38, "99999998.999999998"),(39, "99999998.999999999"),
      (40, "99999999.000000000"),(41, "99999999.000000001"),(42, "99999999.000000009"),(43, "99999999.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_9_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_9_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_17_13_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_17_13_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_17_13_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_17_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_17_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_17_13_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_17_13_not_nullable(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_17_13_not_nullable values (0, "0.00000000000000000"),(1, "0.00000000000000001"),(2, "0.00000000000000009"),(3, "0.09999999999999999"),(4, "0.90000000000000000"),(5, "0.90000000000000001"),(6, "0.99999999999999998"),(7, "0.99999999999999999"),(8, "8.00000000000000000"),(9, "8.00000000000000001"),(10, "8.00000000000000009"),(11, "8.09999999999999999"),(12, "8.90000000000000000"),(13, "8.90000000000000001"),(14, "8.99999999999999998"),(15, "8.99999999999999999"),(16, "9.00000000000000000"),(17, "9.00000000000000001"),(18, "9.00000000000000009"),(19, "9.09999999999999999"),
      (20, "9.90000000000000000"),(21, "9.90000000000000001"),(22, "9.99999999999999998"),(23, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_17_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_17_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_18_14_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_18_14_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_18_14_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_18_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_18_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_0_from_decimal_18_18_14_not_nullable;"
    sql "create table test_cast_to_decimal_8_0_from_decimal_18_18_14_not_nullable(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_0_from_decimal_18_18_14_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_18_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 0)) from test_cast_to_decimal_8_0_from_decimal_18_18_14_not_nullable order by 1;'

}