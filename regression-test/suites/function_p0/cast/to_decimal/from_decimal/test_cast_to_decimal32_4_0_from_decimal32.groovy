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


suite("test_cast_to_decimal32_4_0_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_1_0_0_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_1_0_0_nullable(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_1_0_0_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_1_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_1_0_0_not_nullable(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_1_0_0_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_1_1_1_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_1_1_1_nullable(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_1_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_1_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_1_1_1_not_nullable(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_1_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_1_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_0_2_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_0_2_nullable(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_0_2_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_0_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_0_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_0_2_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_0_2_not_nullable(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_0_2_not_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_0_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_0_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_1_3_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_1_3_nullable(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_1_3_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "99.0"),(5, "99.1"),(6, "99.8"),(7, "99.9"),(8, "900.0"),(9, "900.1"),(10, "900.8"),(11, "900.9"),(12, "901.0"),(13, "901.1"),(14, "901.8"),(15, "901.9"),(16, "998.0"),(17, "998.1"),(18, "998.8"),(19, "998.9"),
      (20, "999.0"),(21, "999.1"),(22, "999.8"),(23, "999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_1_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_1_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_1_3_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_1_3_not_nullable(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_1_3_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "99.0"),(5, "99.1"),(6, "99.8"),(7, "99.9"),(8, "900.0"),(9, "900.1"),(10, "900.8"),(11, "900.9"),(12, "901.0"),(13, "901.1"),(14, "901.8"),(15, "901.9"),(16, "998.0"),(17, "998.1"),(18, "998.8"),(19, "998.9"),
      (20, "999.0"),(21, "999.1"),(22, "999.8"),(23, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_1_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_1_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_2_4_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_2_4_nullable(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_2_4_nullable values (0, "0.00"),(1, "0.01"),(2, "0.09"),(3, "0.90"),(4, "0.91"),(5, "0.98"),(6, "0.99"),(7, "9.00"),(8, "9.01"),(9, "9.09"),(10, "9.90"),(11, "9.91"),(12, "9.98"),(13, "9.99"),(14, "90.00"),(15, "90.01"),(16, "90.09"),(17, "90.90"),(18, "90.91"),(19, "90.98"),
      (20, "90.99"),(21, "91.00"),(22, "91.01"),(23, "91.09"),(24, "91.90"),(25, "91.91"),(26, "91.98"),(27, "91.99"),(28, "98.00"),(29, "98.01"),(30, "98.09"),(31, "98.90"),(32, "98.91"),(33, "98.98"),(34, "98.99"),(35, "99.00"),(36, "99.01"),(37, "99.09"),(38, "99.90"),(39, "99.91"),
      (40, "99.98"),(41, "99.99")
      ,(42, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_2_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_2_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_2_4_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_2_4_not_nullable(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_2_4_not_nullable values (0, "0.00"),(1, "0.01"),(2, "0.09"),(3, "0.90"),(4, "0.91"),(5, "0.98"),(6, "0.99"),(7, "9.00"),(8, "9.01"),(9, "9.09"),(10, "9.90"),(11, "9.91"),(12, "9.98"),(13, "9.99"),(14, "90.00"),(15, "90.01"),(16, "90.09"),(17, "90.90"),(18, "90.91"),(19, "90.98"),
      (20, "90.99"),(21, "91.00"),(22, "91.01"),(23, "91.09"),(24, "91.90"),(25, "91.91"),(26, "91.98"),(27, "91.99"),(28, "98.00"),(29, "98.01"),(30, "98.09"),(31, "98.90"),(32, "98.91"),(33, "98.98"),(34, "98.99"),(35, "99.00"),(36, "99.01"),(37, "99.09"),(38, "99.90"),(39, "99.91"),
      (40, "99.98"),(41, "99.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_2_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_2_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_3_5_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_3_5_nullable(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_3_5_nullable values (0, "0.000"),(1, "0.001"),(2, "0.009"),(3, "0.099"),(4, "0.900"),(5, "0.901"),(6, "0.998"),(7, "0.999"),(8, "8.000"),(9, "8.001"),(10, "8.009"),(11, "8.099"),(12, "8.900"),(13, "8.901"),(14, "8.998"),(15, "8.999"),(16, "9.000"),(17, "9.001"),(18, "9.009"),(19, "9.099"),
      (20, "9.900"),(21, "9.901"),(22, "9.998"),(23, "9.999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_3_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_3_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_3_5_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_3_5_not_nullable(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_3_5_not_nullable values (0, "0.000"),(1, "0.001"),(2, "0.009"),(3, "0.099"),(4, "0.900"),(5, "0.901"),(6, "0.998"),(7, "0.999"),(8, "8.000"),(9, "8.001"),(10, "8.009"),(11, "8.099"),(12, "8.900"),(13, "8.901"),(14, "8.998"),(15, "8.999"),(16, "9.000"),(17, "9.001"),(18, "9.009"),(19, "9.099"),
      (20, "9.900"),(21, "9.901"),(22, "9.998"),(23, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_3_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_3_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_4_6_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_4_6_nullable(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_4_6_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_4_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_4_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_4_4_6_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_4_4_6_not_nullable(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_4_4_6_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_4_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_4_4_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_0_7_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_0_7_nullable(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_0_7_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_0_7_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_0_7_not_nullable(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_0_7_not_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_0_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_1_8_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_1_8_nullable(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_1_8_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999.0"),(5, "999.1"),(6, "999.8"),(7, "999.9"),(8, "9000.0"),(9, "9000.1"),(10, "9000.8"),(11, "9000.9"),(12, "9001.0"),(13, "9001.1"),(14, "9001.8"),(15, "9001.9"),(16, "9998.0"),(17, "9998.1"),(18, "9998.8"),(19, "9998.9"),
      (20, "9999.0"),(21, "9999.1")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_1_8_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_1_8_not_nullable(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_1_8_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999.0"),(5, "999.1"),(6, "999.8"),(7, "999.9"),(8, "9000.0"),(9, "9000.1"),(10, "9000.8"),(11, "9000.9"),(12, "9001.0"),(13, "9001.1"),(14, "9001.8"),(15, "9001.9"),(16, "9998.0"),(17, "9998.1"),(18, "9998.8"),(19, "9998.9"),
      (20, "9999.0"),(21, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_1_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_4_9_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_4_9_nullable(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_4_9_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "999.0000"),(9, "999.0001"),(10, "999.0009"),(11, "999.0999"),(12, "999.9000"),(13, "999.9001"),(14, "999.9998"),(15, "999.9999"),(16, "9000.0000"),(17, "9000.0001"),(18, "9000.0009"),(19, "9000.0999"),
      (20, "9000.9000"),(21, "9000.9001"),(22, "9000.9998"),(23, "9000.9999"),(24, "9001.0000"),(25, "9001.0001"),(26, "9001.0009"),(27, "9001.0999"),(28, "9001.9000"),(29, "9001.9001"),(30, "9001.9998"),(31, "9001.9999"),(32, "9998.0000"),(33, "9998.0001"),(34, "9998.0009"),(35, "9998.0999"),(36, "9998.9000"),(37, "9998.9001"),(38, "9998.9998"),(39, "9998.9999"),
      (40, "9999.0000"),(41, "9999.0001"),(42, "9999.0009"),(43, "9999.0999")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_4_9_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_4_9_not_nullable(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_4_9_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "999.0000"),(9, "999.0001"),(10, "999.0009"),(11, "999.0999"),(12, "999.9000"),(13, "999.9001"),(14, "999.9998"),(15, "999.9999"),(16, "9000.0000"),(17, "9000.0001"),(18, "9000.0009"),(19, "9000.0999"),
      (20, "9000.9000"),(21, "9000.9001"),(22, "9000.9998"),(23, "9000.9999"),(24, "9001.0000"),(25, "9001.0001"),(26, "9001.0009"),(27, "9001.0999"),(28, "9001.9000"),(29, "9001.9001"),(30, "9001.9998"),(31, "9001.9999"),(32, "9998.0000"),(33, "9998.0001"),(34, "9998.0009"),(35, "9998.0999"),(36, "9998.9000"),(37, "9998.9001"),(38, "9998.9998"),(39, "9998.9999"),
      (40, "9999.0000"),(41, "9999.0001"),(42, "9999.0009"),(43, "9999.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_4_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_7_10_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_7_10_nullable(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_7_10_nullable values (0, "0.0000000"),(1, "0.0000001"),(2, "0.0000009"),(3, "0.0999999"),(4, "0.9000000"),(5, "0.9000001"),(6, "0.9999998"),(7, "0.9999999"),(8, "8.0000000"),(9, "8.0000001"),(10, "8.0000009"),(11, "8.0999999"),(12, "8.9000000"),(13, "8.9000001"),(14, "8.9999998"),(15, "8.9999999"),(16, "9.0000000"),(17, "9.0000001"),(18, "9.0000009"),(19, "9.0999999"),
      (20, "9.9000000"),(21, "9.9000001"),(22, "9.9999998"),(23, "9.9999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_7_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_7_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_7_10_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_7_10_not_nullable(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_7_10_not_nullable values (0, "0.0000000"),(1, "0.0000001"),(2, "0.0000009"),(3, "0.0999999"),(4, "0.9000000"),(5, "0.9000001"),(6, "0.9999998"),(7, "0.9999999"),(8, "8.0000000"),(9, "8.0000001"),(10, "8.0000009"),(11, "8.0999999"),(12, "8.9000000"),(13, "8.9000001"),(14, "8.9999998"),(15, "8.9999999"),(16, "9.0000000"),(17, "9.0000001"),(18, "9.0000009"),(19, "9.0999999"),
      (20, "9.9000000"),(21, "9.9000001"),(22, "9.9999998"),(23, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_7_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_7_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_8_11_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_8_11_nullable(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_8_11_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_8_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_8_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_8_8_11_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_8_8_11_not_nullable(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_8_8_11_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_8_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_8_8_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_0_12_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_0_12_nullable(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_0_12_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_0_12_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_0_12_not_nullable(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_0_12_not_nullable values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_0_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_1_13_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_1_13_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_1_13_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999.0"),(5, "999.1"),(6, "999.8"),(7, "999.9"),(8, "9000.0"),(9, "9000.1"),(10, "9000.8"),(11, "9000.9"),(12, "9001.0"),(13, "9001.1"),(14, "9001.8"),(15, "9001.9"),(16, "9998.0"),(17, "9998.1"),(18, "9998.8"),(19, "9998.9"),
      (20, "9999.0"),(21, "9999.1")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_1_13_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_1_13_not_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_1_13_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999.0"),(5, "999.1"),(6, "999.8"),(7, "999.9"),(8, "9000.0"),(9, "9000.1"),(10, "9000.8"),(11, "9000.9"),(12, "9001.0"),(13, "9001.1"),(14, "9001.8"),(15, "9001.9"),(16, "9998.0"),(17, "9998.1"),(18, "9998.8"),(19, "9998.9"),
      (20, "9999.0"),(21, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_1_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_4_14_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_4_14_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_4_14_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "999.0000"),(9, "999.0001"),(10, "999.0009"),(11, "999.0999"),(12, "999.9000"),(13, "999.9001"),(14, "999.9998"),(15, "999.9999"),(16, "9000.0000"),(17, "9000.0001"),(18, "9000.0009"),(19, "9000.0999"),
      (20, "9000.9000"),(21, "9000.9001"),(22, "9000.9998"),(23, "9000.9999"),(24, "9001.0000"),(25, "9001.0001"),(26, "9001.0009"),(27, "9001.0999"),(28, "9001.9000"),(29, "9001.9001"),(30, "9001.9998"),(31, "9001.9999"),(32, "9998.0000"),(33, "9998.0001"),(34, "9998.0009"),(35, "9998.0999"),(36, "9998.9000"),(37, "9998.9001"),(38, "9998.9998"),(39, "9998.9999"),
      (40, "9999.0000"),(41, "9999.0001"),(42, "9999.0009"),(43, "9999.0999")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_4_14_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_4_14_not_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_4_14_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "999.0000"),(9, "999.0001"),(10, "999.0009"),(11, "999.0999"),(12, "999.9000"),(13, "999.9001"),(14, "999.9998"),(15, "999.9999"),(16, "9000.0000"),(17, "9000.0001"),(18, "9000.0009"),(19, "9000.0999"),
      (20, "9000.9000"),(21, "9000.9001"),(22, "9000.9998"),(23, "9000.9999"),(24, "9001.0000"),(25, "9001.0001"),(26, "9001.0009"),(27, "9001.0999"),(28, "9001.9000"),(29, "9001.9001"),(30, "9001.9998"),(31, "9001.9999"),(32, "9998.0000"),(33, "9998.0001"),(34, "9998.0009"),(35, "9998.0999"),(36, "9998.9000"),(37, "9998.9001"),(38, "9998.9998"),(39, "9998.9999"),
      (40, "9999.0000"),(41, "9999.0001"),(42, "9999.0009"),(43, "9999.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_4_14_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_8_15_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_8_15_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_8_15_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "8.00000000"),(9, "8.00000001"),(10, "8.00000009"),(11, "8.09999999"),(12, "8.90000000"),(13, "8.90000001"),(14, "8.99999998"),(15, "8.99999999"),(16, "9.00000000"),(17, "9.00000001"),(18, "9.00000009"),(19, "9.09999999"),
      (20, "9.90000000"),(21, "9.90000001"),(22, "9.99999998"),(23, "9.99999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_8_15_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_8_15_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_8_15_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_8_15_not_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_8_15_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "8.00000000"),(9, "8.00000001"),(10, "8.00000009"),(11, "8.09999999"),(12, "8.90000000"),(13, "8.90000001"),(14, "8.99999998"),(15, "8.99999999"),(16, "9.00000000"),(17, "9.00000001"),(18, "9.00000009"),(19, "9.09999999"),
      (20, "9.90000000"),(21, "9.90000001"),(22, "9.99999998"),(23, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_8_15_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_8_15_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_9_16_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_9_16_nullable(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_9_16_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_9_16_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_9_16_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_4_0_from_decimal_9_9_16_not_nullable;"
    sql "create table test_cast_to_decimal_4_0_from_decimal_9_9_16_not_nullable(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_4_0_from_decimal_9_9_16_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_9_16_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal_4_0_from_decimal_9_9_16_not_nullable order by 1;'

}