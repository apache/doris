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


suite("test_cast_to_decimal32_8_1_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_0_0_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_0_0_nullable(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_0_0_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_0_0_not_nullable(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_0_0_not_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_1_1_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_1_1_nullable(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_1_1_not_nullable(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_9_2_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_9_2_nullable(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_9_2_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "999999.000000000"),(9, "999999.000000001"),(10, "999999.000000009"),(11, "999999.099999999"),(12, "999999.900000000"),(13, "999999.900000001"),(14, "999999.999999998"),(15, "999999.999999999"),(16, "9000000.000000000"),(17, "9000000.000000001"),(18, "9000000.000000009"),(19, "9000000.099999999"),
      (20, "9000000.900000000"),(21, "9000000.900000001"),(22, "9000000.999999998"),(23, "9000000.999999999"),(24, "9000001.000000000"),(25, "9000001.000000001"),(26, "9000001.000000009"),(27, "9000001.099999999"),(28, "9000001.900000000"),(29, "9000001.900000001"),(30, "9000001.999999998"),(31, "9000001.999999999"),(32, "9999998.000000000"),(33, "9999998.000000001"),(34, "9999998.000000009"),(35, "9999998.099999999"),(36, "9999998.900000000"),(37, "9999998.900000001"),(38, "9999998.999999998"),(39, "9999998.999999999"),
      (40, "9999999.000000000"),(41, "9999999.000000001"),(42, "9999999.000000009"),(43, "9999999.099999999"),(44, "9999999.900000000"),(45, "9999999.900000001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_9_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_9_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_9_2_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_9_2_not_nullable(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_9_2_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "999999.000000000"),(9, "999999.000000001"),(10, "999999.000000009"),(11, "999999.099999999"),(12, "999999.900000000"),(13, "999999.900000001"),(14, "999999.999999998"),(15, "999999.999999999"),(16, "9000000.000000000"),(17, "9000000.000000001"),(18, "9000000.000000009"),(19, "9000000.099999999"),
      (20, "9000000.900000000"),(21, "9000000.900000001"),(22, "9000000.999999998"),(23, "9000000.999999999"),(24, "9000001.000000000"),(25, "9000001.000000001"),(26, "9000001.000000009"),(27, "9000001.099999999"),(28, "9000001.900000000"),(29, "9000001.900000001"),(30, "9000001.999999998"),(31, "9000001.999999999"),(32, "9999998.000000000"),(33, "9999998.000000001"),(34, "9999998.000000009"),(35, "9999998.099999999"),(36, "9999998.900000000"),(37, "9999998.900000001"),(38, "9999998.999999998"),(39, "9999998.999999999"),
      (40, "9999999.000000000"),(41, "9999999.000000001"),(42, "9999999.000000009"),(43, "9999999.099999999"),(44, "9999999.900000000"),(45, "9999999.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_9_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_9_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_18_3_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_18_3_nullable(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_18_3_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999"),(8, "8.000000000000000000"),(9, "8.000000000000000001"),(10, "8.000000000000000009"),(11, "8.099999999999999999"),(12, "8.900000000000000000"),(13, "8.900000000000000001"),(14, "8.999999999999999998"),(15, "8.999999999999999999"),(16, "9.000000000000000000"),(17, "9.000000000000000001"),(18, "9.000000000000000009"),(19, "9.099999999999999999"),
      (20, "9.900000000000000000"),(21, "9.900000000000000001"),(22, "9.999999999999999998"),(23, "9.999999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_18_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_18_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_18_3_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_18_3_not_nullable(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_18_3_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999"),(8, "8.000000000000000000"),(9, "8.000000000000000001"),(10, "8.000000000000000009"),(11, "8.099999999999999999"),(12, "8.900000000000000000"),(13, "8.900000000000000001"),(14, "8.999999999999999998"),(15, "8.999999999999999999"),(16, "9.000000000000000000"),(17, "9.000000000000000001"),(18, "9.000000000000000009"),(19, "9.099999999999999999"),
      (20, "9.900000000000000000"),(21, "9.900000000000000001"),(22, "9.999999999999999998"),(23, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_18_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_18_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_19_4_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_19_4_nullable(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_19_4_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_19_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_19_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_19_19_4_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_19_19_4_not_nullable(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_19_19_4_not_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_19_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_19_19_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_0_5_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_0_5_nullable(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_0_5_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_0_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_0_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_0_5_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_0_5_not_nullable(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_0_5_not_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_0_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_0_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_1_6_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_1_6_nullable(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_1_6_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_1_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_1_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_1_6_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_1_6_not_nullable(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_1_6_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_1_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_1_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_18_7_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_18_7_nullable(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_18_7_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999"),(8, "999999.000000000000000000"),(9, "999999.000000000000000001"),(10, "999999.000000000000000009"),(11, "999999.099999999999999999"),(12, "999999.900000000000000000"),(13, "999999.900000000000000001"),(14, "999999.999999999999999998"),(15, "999999.999999999999999999"),(16, "9000000.000000000000000000"),(17, "9000000.000000000000000001"),(18, "9000000.000000000000000009"),(19, "9000000.099999999999999999"),
      (20, "9000000.900000000000000000"),(21, "9000000.900000000000000001"),(22, "9000000.999999999999999998"),(23, "9000000.999999999999999999"),(24, "9000001.000000000000000000"),(25, "9000001.000000000000000001"),(26, "9000001.000000000000000009"),(27, "9000001.099999999999999999"),(28, "9000001.900000000000000000"),(29, "9000001.900000000000000001"),(30, "9000001.999999999999999998"),(31, "9000001.999999999999999999"),(32, "9999998.000000000000000000"),(33, "9999998.000000000000000001"),(34, "9999998.000000000000000009"),(35, "9999998.099999999999999999"),(36, "9999998.900000000000000000"),(37, "9999998.900000000000000001"),(38, "9999998.999999999999999998"),(39, "9999998.999999999999999999"),
      (40, "9999999.000000000000000000"),(41, "9999999.000000000000000001"),(42, "9999999.000000000000000009"),(43, "9999999.099999999999999999"),(44, "9999999.900000000000000000"),(45, "9999999.900000000000000001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_18_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_18_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_18_7_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_18_7_not_nullable(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_18_7_not_nullable values (0, "0.000000000000000000"),(1, "0.000000000000000001"),(2, "0.000000000000000009"),(3, "0.099999999999999999"),(4, "0.900000000000000000"),(5, "0.900000000000000001"),(6, "0.999999999999999998"),(7, "0.999999999999999999"),(8, "999999.000000000000000000"),(9, "999999.000000000000000001"),(10, "999999.000000000000000009"),(11, "999999.099999999999999999"),(12, "999999.900000000000000000"),(13, "999999.900000000000000001"),(14, "999999.999999999999999998"),(15, "999999.999999999999999999"),(16, "9000000.000000000000000000"),(17, "9000000.000000000000000001"),(18, "9000000.000000000000000009"),(19, "9000000.099999999999999999"),
      (20, "9000000.900000000000000000"),(21, "9000000.900000000000000001"),(22, "9000000.999999999999999998"),(23, "9000000.999999999999999999"),(24, "9000001.000000000000000000"),(25, "9000001.000000000000000001"),(26, "9000001.000000000000000009"),(27, "9000001.099999999999999999"),(28, "9000001.900000000000000000"),(29, "9000001.900000000000000001"),(30, "9000001.999999999999999998"),(31, "9000001.999999999999999999"),(32, "9999998.000000000000000000"),(33, "9999998.000000000000000001"),(34, "9999998.000000000000000009"),(35, "9999998.099999999999999999"),(36, "9999998.900000000000000000"),(37, "9999998.900000000000000001"),(38, "9999998.999999999999999998"),(39, "9999998.999999999999999999"),
      (40, "9999999.000000000000000000"),(41, "9999999.000000000000000001"),(42, "9999999.000000000000000009"),(43, "9999999.099999999999999999"),(44, "9999999.900000000000000000"),(45, "9999999.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_18_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_18_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_36_8_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_36_8_nullable(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_36_8_nullable values (0, "0.000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000001"),(6, "0.999999999999999999999999999999999998"),(7, "0.999999999999999999999999999999999999"),(8, "8.000000000000000000000000000000000000"),(9, "8.000000000000000000000000000000000001"),(10, "8.000000000000000000000000000000000009"),(11, "8.099999999999999999999999999999999999"),(12, "8.900000000000000000000000000000000000"),(13, "8.900000000000000000000000000000000001"),(14, "8.999999999999999999999999999999999998"),(15, "8.999999999999999999999999999999999999"),(16, "9.000000000000000000000000000000000000"),(17, "9.000000000000000000000000000000000001"),(18, "9.000000000000000000000000000000000009"),(19, "9.099999999999999999999999999999999999"),
      (20, "9.900000000000000000000000000000000000"),(21, "9.900000000000000000000000000000000001"),(22, "9.999999999999999999999999999999999998"),(23, "9.999999999999999999999999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_36_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_36_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_36_8_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_36_8_not_nullable(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_36_8_not_nullable values (0, "0.000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000001"),(6, "0.999999999999999999999999999999999998"),(7, "0.999999999999999999999999999999999999"),(8, "8.000000000000000000000000000000000000"),(9, "8.000000000000000000000000000000000001"),(10, "8.000000000000000000000000000000000009"),(11, "8.099999999999999999999999999999999999"),(12, "8.900000000000000000000000000000000000"),(13, "8.900000000000000000000000000000000001"),(14, "8.999999999999999999999999999999999998"),(15, "8.999999999999999999999999999999999999"),(16, "9.000000000000000000000000000000000000"),(17, "9.000000000000000000000000000000000001"),(18, "9.000000000000000000000000000000000009"),(19, "9.099999999999999999999999999999999999"),
      (20, "9.900000000000000000000000000000000000"),(21, "9.900000000000000000000000000000000001"),(22, "9.999999999999999999999999999999999998"),(23, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_36_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_36_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_37_9_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_37_9_nullable(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_37_9_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_37_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_37_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_37_37_9_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_37_37_9_not_nullable(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_37_37_9_not_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_37_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_37_37_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_0_10_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_0_10_nullable(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_0_10_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_0_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_0_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_0_10_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_0_10_not_nullable(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_0_10_not_nullable values (0, "0"),(1, "999999"),(2, "9000000"),(3, "9000001"),(4, "9999998"),(5, "9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_0_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_0_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_1_11_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_1_11_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_1_11_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_1_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_1_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_1_11_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_1_11_not_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_1_11_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9"),(4, "999999.0"),(5, "999999.1"),(6, "999999.8"),(7, "999999.9"),(8, "9000000.0"),(9, "9000000.1"),(10, "9000000.8"),(11, "9000000.9"),(12, "9000001.0"),(13, "9000001.1"),(14, "9000001.8"),(15, "9000001.9"),(16, "9999998.0"),(17, "9999998.1"),(18, "9999998.8"),(19, "9999998.9"),
      (20, "9999999.0"),(21, "9999999.1"),(22, "9999999.8"),(23, "9999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_1_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_1_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_19_12_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_19_12_nullable(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_19_12_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999"),(8, "999999.0000000000000000000"),(9, "999999.0000000000000000001"),(10, "999999.0000000000000000009"),(11, "999999.0999999999999999999"),(12, "999999.9000000000000000000"),(13, "999999.9000000000000000001"),(14, "999999.9999999999999999998"),(15, "999999.9999999999999999999"),(16, "9000000.0000000000000000000"),(17, "9000000.0000000000000000001"),(18, "9000000.0000000000000000009"),(19, "9000000.0999999999999999999"),
      (20, "9000000.9000000000000000000"),(21, "9000000.9000000000000000001"),(22, "9000000.9999999999999999998"),(23, "9000000.9999999999999999999"),(24, "9000001.0000000000000000000"),(25, "9000001.0000000000000000001"),(26, "9000001.0000000000000000009"),(27, "9000001.0999999999999999999"),(28, "9000001.9000000000000000000"),(29, "9000001.9000000000000000001"),(30, "9000001.9999999999999999998"),(31, "9000001.9999999999999999999"),(32, "9999998.0000000000000000000"),(33, "9999998.0000000000000000001"),(34, "9999998.0000000000000000009"),(35, "9999998.0999999999999999999"),(36, "9999998.9000000000000000000"),(37, "9999998.9000000000000000001"),(38, "9999998.9999999999999999998"),(39, "9999998.9999999999999999999"),
      (40, "9999999.0000000000000000000"),(41, "9999999.0000000000000000001"),(42, "9999999.0000000000000000009"),(43, "9999999.0999999999999999999"),(44, "9999999.9000000000000000000"),(45, "9999999.9000000000000000001")
      ,(46, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_19_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_19_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_19_12_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_19_12_not_nullable(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_19_12_not_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999"),(8, "999999.0000000000000000000"),(9, "999999.0000000000000000001"),(10, "999999.0000000000000000009"),(11, "999999.0999999999999999999"),(12, "999999.9000000000000000000"),(13, "999999.9000000000000000001"),(14, "999999.9999999999999999998"),(15, "999999.9999999999999999999"),(16, "9000000.0000000000000000000"),(17, "9000000.0000000000000000001"),(18, "9000000.0000000000000000009"),(19, "9000000.0999999999999999999"),
      (20, "9000000.9000000000000000000"),(21, "9000000.9000000000000000001"),(22, "9000000.9999999999999999998"),(23, "9000000.9999999999999999999"),(24, "9000001.0000000000000000000"),(25, "9000001.0000000000000000001"),(26, "9000001.0000000000000000009"),(27, "9000001.0999999999999999999"),(28, "9000001.9000000000000000000"),(29, "9000001.9000000000000000001"),(30, "9000001.9999999999999999998"),(31, "9000001.9999999999999999999"),(32, "9999998.0000000000000000000"),(33, "9999998.0000000000000000001"),(34, "9999998.0000000000000000009"),(35, "9999998.0999999999999999999"),(36, "9999998.9000000000000000000"),(37, "9999998.9000000000000000001"),(38, "9999998.9999999999999999998"),(39, "9999998.9999999999999999999"),
      (40, "9999999.0000000000000000000"),(41, "9999999.0000000000000000001"),(42, "9999999.0000000000000000009"),(43, "9999999.0999999999999999999"),(44, "9999999.9000000000000000000"),(45, "9999999.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_19_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_19_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_37_13_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_37_13_nullable(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_37_13_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999"),(8, "8.0000000000000000000000000000000000000"),(9, "8.0000000000000000000000000000000000001"),(10, "8.0000000000000000000000000000000000009"),(11, "8.0999999999999999999999999999999999999"),(12, "8.9000000000000000000000000000000000000"),(13, "8.9000000000000000000000000000000000001"),(14, "8.9999999999999999999999999999999999998"),(15, "8.9999999999999999999999999999999999999"),(16, "9.0000000000000000000000000000000000000"),(17, "9.0000000000000000000000000000000000001"),(18, "9.0000000000000000000000000000000000009"),(19, "9.0999999999999999999999999999999999999"),
      (20, "9.9000000000000000000000000000000000000"),(21, "9.9000000000000000000000000000000000001"),(22, "9.9999999999999999999999999999999999998"),(23, "9.9999999999999999999999999999999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_37_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_37_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_37_13_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_37_13_not_nullable(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_37_13_not_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999"),(8, "8.0000000000000000000000000000000000000"),(9, "8.0000000000000000000000000000000000001"),(10, "8.0000000000000000000000000000000000009"),(11, "8.0999999999999999999999999999999999999"),(12, "8.9000000000000000000000000000000000000"),(13, "8.9000000000000000000000000000000000001"),(14, "8.9999999999999999999999999999999999998"),(15, "8.9999999999999999999999999999999999999"),(16, "9.0000000000000000000000000000000000000"),(17, "9.0000000000000000000000000000000000001"),(18, "9.0000000000000000000000000000000000009"),(19, "9.0999999999999999999999999999999999999"),
      (20, "9.9000000000000000000000000000000000000"),(21, "9.9000000000000000000000000000000000001"),(22, "9.9999999999999999999999999999999999998"),(23, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_37_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_37_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_38_14_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_38_14_nullable(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_38_14_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001"),(6, "0.99999999999999999999999999999999999998"),(7, "0.99999999999999999999999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_38_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_38_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_8_1_from_decimal_38_38_14_not_nullable;"
    sql "create table test_cast_to_decimal_8_1_from_decimal_38_38_14_not_nullable(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_8_1_from_decimal_38_38_14_not_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001"),(6, "0.99999999999999999999999999999999999998"),(7, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_38_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 1)) from test_cast_to_decimal_8_1_from_decimal_38_38_14_not_nullable order by 1;'

}