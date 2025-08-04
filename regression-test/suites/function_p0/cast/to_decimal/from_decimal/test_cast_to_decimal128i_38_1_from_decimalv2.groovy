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


suite("test_cast_to_decimal128i_38_1_from_decimalv2") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_1_0_0_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_1_0_0_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_1_0_0_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_1_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_1_0_0_not_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_1_0_0_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_1_1_1_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_1_1_1_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_1_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_1_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_1_1_1_not_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_1_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_27_9_2_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_27_9_2_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_27_9_2_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "99999999999999999.000000000"),(9, "99999999999999999.000000001"),(10, "99999999999999999.000000009"),(11, "99999999999999999.099999999"),(12, "99999999999999999.900000000"),(13, "99999999999999999.900000001"),(14, "99999999999999999.999999998"),(15, "99999999999999999.999999999"),(16, "900000000000000000.000000000"),(17, "900000000000000000.000000001"),(18, "900000000000000000.000000009"),(19, "900000000000000000.099999999"),
      (20, "900000000000000000.900000000"),(21, "900000000000000000.900000001"),(22, "900000000000000000.999999998"),(23, "900000000000000000.999999999"),(24, "900000000000000001.000000000"),(25, "900000000000000001.000000001"),(26, "900000000000000001.000000009"),(27, "900000000000000001.099999999"),(28, "900000000000000001.900000000"),(29, "900000000000000001.900000001"),(30, "900000000000000001.999999998"),(31, "900000000000000001.999999999"),(32, "999999999999999998.000000000"),(33, "999999999999999998.000000001"),(34, "999999999999999998.000000009"),(35, "999999999999999998.099999999"),(36, "999999999999999998.900000000"),(37, "999999999999999998.900000001"),(38, "999999999999999998.999999998"),(39, "999999999999999998.999999999"),
      (40, "999999999999999999.000000000"),(41, "999999999999999999.000000001"),(42, "999999999999999999.000000009"),(43, "999999999999999999.099999999"),(44, "999999999999999999.900000000"),(45, "999999999999999999.900000001"),(46, "999999999999999999.999999998"),(47, "999999999999999999.999999999")
      ,(48, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_27_9_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_27_9_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_27_9_2_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_27_9_2_not_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_27_9_2_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "99999999999999999.000000000"),(9, "99999999999999999.000000001"),(10, "99999999999999999.000000009"),(11, "99999999999999999.099999999"),(12, "99999999999999999.900000000"),(13, "99999999999999999.900000001"),(14, "99999999999999999.999999998"),(15, "99999999999999999.999999999"),(16, "900000000000000000.000000000"),(17, "900000000000000000.000000001"),(18, "900000000000000000.000000009"),(19, "900000000000000000.099999999"),
      (20, "900000000000000000.900000000"),(21, "900000000000000000.900000001"),(22, "900000000000000000.999999998"),(23, "900000000000000000.999999999"),(24, "900000000000000001.000000000"),(25, "900000000000000001.000000001"),(26, "900000000000000001.000000009"),(27, "900000000000000001.099999999"),(28, "900000000000000001.900000000"),(29, "900000000000000001.900000001"),(30, "900000000000000001.999999998"),(31, "900000000000000001.999999999"),(32, "999999999999999998.000000000"),(33, "999999999999999998.000000001"),(34, "999999999999999998.000000009"),(35, "999999999999999998.099999999"),(36, "999999999999999998.900000000"),(37, "999999999999999998.900000001"),(38, "999999999999999998.999999998"),(39, "999999999999999998.999999999"),
      (40, "999999999999999999.000000000"),(41, "999999999999999999.000000001"),(42, "999999999999999999.000000009"),(43, "999999999999999999.099999999"),(44, "999999999999999999.900000000"),(45, "999999999999999999.900000001"),(46, "999999999999999999.999999998"),(47, "999999999999999999.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_20_5_3_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_20_5_3_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_20_5_3_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "99999999999999.00000"),(9, "99999999999999.00001"),(10, "99999999999999.00009"),(11, "99999999999999.09999"),(12, "99999999999999.90000"),(13, "99999999999999.90001"),(14, "99999999999999.99998"),(15, "99999999999999.99999"),(16, "900000000000000.00000"),(17, "900000000000000.00001"),(18, "900000000000000.00009"),(19, "900000000000000.09999"),
      (20, "900000000000000.90000"),(21, "900000000000000.90001"),(22, "900000000000000.99998"),(23, "900000000000000.99999"),(24, "900000000000001.00000"),(25, "900000000000001.00001"),(26, "900000000000001.00009"),(27, "900000000000001.09999"),(28, "900000000000001.90000"),(29, "900000000000001.90001"),(30, "900000000000001.99998"),(31, "900000000000001.99999"),(32, "999999999999998.00000"),(33, "999999999999998.00001"),(34, "999999999999998.00009"),(35, "999999999999998.09999"),(36, "999999999999998.90000"),(37, "999999999999998.90001"),(38, "999999999999998.99998"),(39, "999999999999998.99999"),
      (40, "999999999999999.00000"),(41, "999999999999999.00001"),(42, "999999999999999.00009"),(43, "999999999999999.09999"),(44, "999999999999999.90000"),(45, "999999999999999.90001"),(46, "999999999999999.99998"),(47, "999999999999999.99999")
      ,(48, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_20_5_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_20_5_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_38_1_from_decimalv2_20_5_3_not_nullable;"
    sql "create table test_cast_to_decimal_38_1_from_decimalv2_20_5_3_not_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_38_1_from_decimalv2_20_5_3_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "99999999999999.00000"),(9, "99999999999999.00001"),(10, "99999999999999.00009"),(11, "99999999999999.09999"),(12, "99999999999999.90000"),(13, "99999999999999.90001"),(14, "99999999999999.99998"),(15, "99999999999999.99999"),(16, "900000000000000.00000"),(17, "900000000000000.00001"),(18, "900000000000000.00009"),(19, "900000000000000.09999"),
      (20, "900000000000000.90000"),(21, "900000000000000.90001"),(22, "900000000000000.99998"),(23, "900000000000000.99999"),(24, "900000000000001.00000"),(25, "900000000000001.00001"),(26, "900000000000001.00009"),(27, "900000000000001.09999"),(28, "900000000000001.90000"),(29, "900000000000001.90001"),(30, "900000000000001.99998"),(31, "900000000000001.99999"),(32, "999999999999998.00000"),(33, "999999999999998.00001"),(34, "999999999999998.00009"),(35, "999999999999998.09999"),(36, "999999999999998.90000"),(37, "999999999999998.90001"),(38, "999999999999998.99998"),(39, "999999999999998.99999"),
      (40, "999999999999999.00000"),(41, "999999999999999.00001"),(42, "999999999999999.00009"),(43, "999999999999999.09999"),(44, "999999999999999.90000"),(45, "999999999999999.90001"),(46, "999999999999999.99998"),(47, "999999999999999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_20_5_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 1)) from test_cast_to_decimal_38_1_from_decimalv2_20_5_3_not_nullable order by 1;'

}