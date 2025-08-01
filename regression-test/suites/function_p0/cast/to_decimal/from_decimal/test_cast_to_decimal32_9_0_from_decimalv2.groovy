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


suite("test_cast_to_decimal32_9_0_from_decimalv2") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_1_0_0_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_1_0_0_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_1_0_0_nullable values (0, "0"),(1, "8000000000"),(2, "9000000000")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_1_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_1_0_0_not_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_1_0_0_not_nullable values (0, "0"),(1, "8000000000"),(2, "9000000000");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_1_1_1_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_1_1_1_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_1_1_1_nullable values (0, "0.0"),(1, "0.9"),(2, "10000000.0"),(3, "80000000.0"),(4, "90000000.0")
      ,(5, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_1_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_1_1_1_not_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_1_1_1_not_nullable values (0, "0.0"),(1, "0.9"),(2, "10000000.0"),(3, "80000000.0"),(4, "90000000.0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_27_9_2_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_27_9_2_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_27_9_2_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "99999999.000000000"),(9, "99999999.000000001"),(10, "99999999.000000009"),(11, "99999999.099999999"),(12, "99999999.900000000"),(13, "99999999.900000001"),(14, "99999999.999999998"),(15, "99999999.999999999"),(16, "900000000.000000000"),(17, "900000000.000000001"),(18, "900000000.000000009"),(19, "900000000.099999999"),
      (20, "900000000.900000000"),(21, "900000000.900000001"),(22, "900000000.999999998"),(23, "900000000.999999999"),(24, "900000001.000000000"),(25, "900000001.000000001"),(26, "900000001.000000009"),(27, "900000001.099999999"),(28, "900000001.900000000"),(29, "900000001.900000001"),(30, "900000001.999999998"),(31, "900000001.999999999"),(32, "999999998.000000000"),(33, "999999998.000000001"),(34, "999999998.000000009"),(35, "999999998.099999999"),(36, "999999998.900000000"),(37, "999999998.900000001"),(38, "999999998.999999998"),(39, "999999998.999999999"),
      (40, "999999999.000000000"),(41, "999999999.000000001"),(42, "999999999.000000009"),(43, "999999999.099999999")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_27_9_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_27_9_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_27_9_2_not_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_27_9_2_not_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_27_9_2_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "99999999.000000000"),(9, "99999999.000000001"),(10, "99999999.000000009"),(11, "99999999.099999999"),(12, "99999999.900000000"),(13, "99999999.900000001"),(14, "99999999.999999998"),(15, "99999999.999999999"),(16, "900000000.000000000"),(17, "900000000.000000001"),(18, "900000000.000000009"),(19, "900000000.099999999"),
      (20, "900000000.900000000"),(21, "900000000.900000001"),(22, "900000000.999999998"),(23, "900000000.999999999"),(24, "900000001.000000000"),(25, "900000001.000000001"),(26, "900000001.000000009"),(27, "900000001.099999999"),(28, "900000001.900000000"),(29, "900000001.900000001"),(30, "900000001.999999998"),(31, "900000001.999999999"),(32, "999999998.000000000"),(33, "999999998.000000001"),(34, "999999998.000000009"),(35, "999999998.099999999"),(36, "999999998.900000000"),(37, "999999998.900000001"),(38, "999999998.999999998"),(39, "999999998.999999999"),
      (40, "999999999.000000000"),(41, "999999999.000000001"),(42, "999999999.000000009"),(43, "999999999.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_20_5_3_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_20_5_3_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_20_5_3_nullable values (0, "0.00000"),(1, "0.10000"),(2, "0.90000"),(3, "0.99999"),(4, "999.90000"),(5, "9000.00000"),(6, "9000.10000"),(7, "9999.80000"),(8, "999999990000.00000"),(9, "999999990000.10000"),(10, "999999990000.90000"),(11, "999999990000.99999"),(12, "999999990999.90000"),(13, "999999999000.00000"),(14, "999999999000.10000"),(15, "999999999999.80000"),(16, "9000000000000.00000"),(17, "9000000000000.10000"),(18, "9000000000000.90000"),(19, "9000000000000.99999"),
      (20, "9000000000999.90000"),(21, "9000000009000.00000"),(22, "9000000009000.10000"),(23, "9000000009999.80000"),(24, "9000000010000.00000"),(25, "9000000010000.10000"),(26, "9000000010000.90000"),(27, "9000000010000.99999"),(28, "9000000010999.90000"),(29, "9000000019000.00000"),(30, "9000000019000.10000"),(31, "9000000019999.80000"),(32, "9999999980000.00000"),(33, "9999999980000.10000"),(34, "9999999980000.90000"),(35, "9999999980000.99999"),(36, "9999999980999.90000"),(37, "9999999989000.00000"),(38, "9999999989000.10000"),(39, "9999999989999.80000"),
      (40, "9999999990000.00000"),(41, "9999999990000.10000"),(42, "9999999990000.90000"),(43, "9999999990000.99999"),(44, "9999999990999.90000")
      ,(45, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_20_5_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_20_5_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_9_0_from_decimalv2_20_5_3_not_nullable;"
    sql "create table test_cast_to_decimal_9_0_from_decimalv2_20_5_3_not_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_0_from_decimalv2_20_5_3_not_nullable values (0, "0.00000"),(1, "0.10000"),(2, "0.90000"),(3, "0.99999"),(4, "999.90000"),(5, "9000.00000"),(6, "9000.10000"),(7, "9999.80000"),(8, "999999990000.00000"),(9, "999999990000.10000"),(10, "999999990000.90000"),(11, "999999990000.99999"),(12, "999999990999.90000"),(13, "999999999000.00000"),(14, "999999999000.10000"),(15, "999999999999.80000"),(16, "9000000000000.00000"),(17, "9000000000000.10000"),(18, "9000000000000.90000"),(19, "9000000000000.99999"),
      (20, "9000000000999.90000"),(21, "9000000009000.00000"),(22, "9000000009000.10000"),(23, "9000000009999.80000"),(24, "9000000010000.00000"),(25, "9000000010000.10000"),(26, "9000000010000.90000"),(27, "9000000010000.99999"),(28, "9000000010999.90000"),(29, "9000000019000.00000"),(30, "9000000019000.10000"),(31, "9000000019999.80000"),(32, "9999999980000.00000"),(33, "9999999980000.10000"),(34, "9999999980000.90000"),(35, "9999999980000.99999"),(36, "9999999980999.90000"),(37, "9999999989000.00000"),(38, "9999999989000.10000"),(39, "9999999989999.80000"),
      (40, "9999999990000.00000"),(41, "9999999990000.10000"),(42, "9999999990000.90000"),(43, "9999999990000.99999"),(44, "9999999990999.90000");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_20_5_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal_9_0_from_decimalv2_20_5_3_not_nullable order by 1;'

}