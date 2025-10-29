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


suite("test_cast_to_decimal32_8_7_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_1_0_0_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_1_0_0_nullable(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_1_0_0_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_1_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_1_0_0_not_nullable(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_1_0_0_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_1_1_1_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_1_1_1_nullable(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_1_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_1_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_1_1_1_not_nullable(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_1_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_1_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_0_2_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_0_2_nullable(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_0_2_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_0_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_0_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_0_2_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_0_2_not_nullable(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_0_2_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_0_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_0_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_1_3_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_1_3_nullable(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_1_3_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9")
      ,(12, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_1_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_1_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_1_3_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_1_3_not_nullable(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_1_3_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_1_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_1_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_2_4_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_2_4_nullable(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_2_4_nullable values (0, "0.00"),(1, "0.01"),(2, "0.09"),(3, "0.90"),(4, "0.91"),(5, "0.98"),(6, "0.99"),(7, "8.00"),(8, "8.01"),(9, "8.09"),(10, "8.90"),(11, "8.91"),(12, "8.98"),(13, "8.99"),(14, "9.00"),(15, "9.01"),(16, "9.09"),(17, "9.90"),(18, "9.91"),(19, "9.98"),
      (20, "9.99")
      ,(21, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_2_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_2_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_2_4_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_2_4_not_nullable(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_2_4_not_nullable values (0, "0.00"),(1, "0.01"),(2, "0.09"),(3, "0.90"),(4, "0.91"),(5, "0.98"),(6, "0.99"),(7, "8.00"),(8, "8.01"),(9, "8.09"),(10, "8.90"),(11, "8.91"),(12, "8.98"),(13, "8.99"),(14, "9.00"),(15, "9.01"),(16, "9.09"),(17, "9.90"),(18, "9.91"),(19, "9.98"),
      (20, "9.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_2_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_2_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_3_5_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_3_5_nullable(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_3_5_nullable values (0, "0.000"),(1, "0.001"),(2, "0.009"),(3, "0.099"),(4, "0.900"),(5, "0.901"),(6, "0.998"),(7, "0.999"),(8, "8.000"),(9, "8.001"),(10, "8.009"),(11, "8.099"),(12, "8.900"),(13, "8.901"),(14, "8.998"),(15, "8.999"),(16, "9.000"),(17, "9.001"),(18, "9.009"),(19, "9.099"),
      (20, "9.900"),(21, "9.901"),(22, "9.998"),(23, "9.999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_3_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_3_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_3_5_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_3_5_not_nullable(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_3_5_not_nullable values (0, "0.000"),(1, "0.001"),(2, "0.009"),(3, "0.099"),(4, "0.900"),(5, "0.901"),(6, "0.998"),(7, "0.999"),(8, "8.000"),(9, "8.001"),(10, "8.009"),(11, "8.099"),(12, "8.900"),(13, "8.901"),(14, "8.998"),(15, "8.999"),(16, "9.000"),(17, "9.001"),(18, "9.009"),(19, "9.099"),
      (20, "9.900"),(21, "9.901"),(22, "9.998"),(23, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_3_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_3_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_4_6_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_4_6_nullable(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_4_6_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_4_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_4_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_4_4_6_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_4_4_6_not_nullable(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_4_4_6_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_4_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_4_4_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_0_7_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_0_7_nullable(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_0_7_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_0_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_0_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_0_7_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_0_7_not_nullable(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_0_7_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_0_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_0_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_1_8_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_1_8_nullable(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_1_8_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9")
      ,(12, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_1_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_1_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_1_8_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_1_8_not_nullable(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_1_8_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_1_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_1_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_4_9_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_4_9_nullable(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_4_9_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "8.0000"),(9, "8.0001"),(10, "8.0009"),(11, "8.0999"),(12, "8.9000"),(13, "8.9001"),(14, "8.9998"),(15, "8.9999"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.0999"),
      (20, "9.9000"),(21, "9.9001"),(22, "9.9998"),(23, "9.9999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_4_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_4_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_4_9_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_4_9_not_nullable(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_4_9_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "8.0000"),(9, "8.0001"),(10, "8.0009"),(11, "8.0999"),(12, "8.9000"),(13, "8.9001"),(14, "8.9998"),(15, "8.9999"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.0999"),
      (20, "9.9000"),(21, "9.9001"),(22, "9.9998"),(23, "9.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_4_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_4_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_7_10_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_7_10_nullable(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_7_10_nullable values (0, "0.0000000"),(1, "0.0000001"),(2, "0.0000009"),(3, "0.0999999"),(4, "0.9000000"),(5, "0.9000001"),(6, "0.9999998"),(7, "0.9999999"),(8, "8.0000000"),(9, "8.0000001"),(10, "8.0000009"),(11, "8.0999999"),(12, "8.9000000"),(13, "8.9000001"),(14, "8.9999998"),(15, "8.9999999"),(16, "9.0000000"),(17, "9.0000001"),(18, "9.0000009"),(19, "9.0999999"),
      (20, "9.9000000"),(21, "9.9000001"),(22, "9.9999998"),(23, "9.9999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_7_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_7_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_7_10_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_7_10_not_nullable(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_7_10_not_nullable values (0, "0.0000000"),(1, "0.0000001"),(2, "0.0000009"),(3, "0.0999999"),(4, "0.9000000"),(5, "0.9000001"),(6, "0.9999998"),(7, "0.9999999"),(8, "8.0000000"),(9, "8.0000001"),(10, "8.0000009"),(11, "8.0999999"),(12, "8.9000000"),(13, "8.9000001"),(14, "8.9999998"),(15, "8.9999999"),(16, "9.0000000"),(17, "9.0000001"),(18, "9.0000009"),(19, "9.0999999"),
      (20, "9.9000000"),(21, "9.9000001"),(22, "9.9999998"),(23, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_7_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_7_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_8_11_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_8_11_nullable(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_8_11_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_8_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_8_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_8_8_11_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_8_8_11_not_nullable(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_8_8_11_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_8_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_8_8_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_0_12_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_0_12_nullable(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_0_12_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_0_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_0_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_0_12_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_0_12_not_nullable(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_0_12_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_0_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_0_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_1_13_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_1_13_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_1_13_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9")
      ,(12, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_1_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_1_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_1_13_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_1_13_not_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_1_13_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "8.0"),(5, "8.1"),(6, "8.8"),(7, "8.9"),(8, "9.0"),(9, "9.1"),(10, "9.8"),(11, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_1_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_1_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_4_14_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_4_14_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_4_14_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "8.0000"),(9, "8.0001"),(10, "8.0009"),(11, "8.0999"),(12, "8.9000"),(13, "8.9001"),(14, "8.9998"),(15, "8.9999"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.0999"),
      (20, "9.9000"),(21, "9.9001"),(22, "9.9998"),(23, "9.9999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_4_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_4_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_4_14_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_4_14_not_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_4_14_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.0999"),(4, "0.9000"),(5, "0.9001"),(6, "0.9998"),(7, "0.9999"),(8, "8.0000"),(9, "8.0001"),(10, "8.0009"),(11, "8.0999"),(12, "8.9000"),(13, "8.9001"),(14, "8.9998"),(15, "8.9999"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.0999"),
      (20, "9.9000"),(21, "9.9001"),(22, "9.9998"),(23, "9.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_4_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_4_14_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_8_15_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_8_15_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_8_15_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "8.00000000"),(9, "8.00000001"),(10, "8.00000009"),(11, "8.09999999"),(12, "8.90000000"),(13, "8.90000001"),(14, "8.99999998"),(15, "8.99999999"),(16, "9.00000000"),(17, "9.00000001"),(18, "9.00000009"),(19, "9.09999999"),
      (20, "9.90000000"),(21, "9.90000001")
      ,(22, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_8_15_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_8_15_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_8_15_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_8_15_not_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_8_15_not_nullable values (0, "0.00000000"),(1, "0.00000001"),(2, "0.00000009"),(3, "0.09999999"),(4, "0.90000000"),(5, "0.90000001"),(6, "0.99999998"),(7, "0.99999999"),(8, "8.00000000"),(9, "8.00000001"),(10, "8.00000009"),(11, "8.09999999"),(12, "8.90000000"),(13, "8.90000001"),(14, "8.99999998"),(15, "8.99999999"),(16, "9.00000000"),(17, "9.00000001"),(18, "9.00000009"),(19, "9.09999999"),
      (20, "9.90000000"),(21, "9.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_8_15_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_8_15_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_9_16_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_9_16_nullable(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_9_16_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_9_16_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_9_16_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_7_from_decimal_9_9_16_not_nullable;"
    sql "create table test_cast_to_decimal_8_7_from_decimal_9_9_16_not_nullable(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_7_from_decimal_9_9_16_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_9_16_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal_8_7_from_decimal_9_9_16_not_nullable order by 1;'

}