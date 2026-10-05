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


suite("test_cast_to_decimal64_18_17_from_decimalv2") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_1_0_0_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_1_0_0_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_1_0_0_nullable values (0, "0"),(1, "8"),(2, "9")
      ,(3, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_1_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_1_0_0_not_nullable(f1 int, f2 decimalv2(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_1_0_0_not_nullable values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_1_1_1_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_1_1_1_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_1_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_1_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_1_1_1_not_nullable(f1 int, f2 decimalv2(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_1_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_1_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_27_9_2_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_27_9_2_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_27_9_2_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_27_9_2_not_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_27_9_2_not_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_27_9_2_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.099999999"),(4, "0.900000000"),(5, "0.900000001"),(6, "0.999999998"),(7, "0.999999999"),(8, "8.000000000"),(9, "8.000000001"),(10, "8.000000009"),(11, "8.099999999"),(12, "8.900000000"),(13, "8.900000001"),(14, "8.999999998"),(15, "8.999999999"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.099999999"),
      (20, "9.900000000"),(21, "9.900000001"),(22, "9.999999998"),(23, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_27_9_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_20_5_3_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_20_5_3_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_20_5_3_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "8.00000"),(9, "8.00001"),(10, "8.00009"),(11, "8.09999"),(12, "8.90000"),(13, "8.90001"),(14, "8.99998"),(15, "8.99999"),(16, "9.00000"),(17, "9.00001"),(18, "9.00009"),(19, "9.09999"),
      (20, "9.90000"),(21, "9.90001"),(22, "9.99998"),(23, "9.99999")
      ,(24, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_5_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_5_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_18_17_from_decimalv2_20_5_3_not_nullable;"
    sql "create table test_cast_to_decimal_18_17_from_decimalv2_20_5_3_not_nullable(f1 int, f2 decimalv2(20, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_18_17_from_decimalv2_20_5_3_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.09999"),(4, "0.90000"),(5, "0.90001"),(6, "0.99998"),(7, "0.99999"),(8, "8.00000"),(9, "8.00001"),(10, "8.00009"),(11, "8.09999"),(12, "8.90000"),(13, "8.90001"),(14, "8.99998"),(15, "8.99999"),(16, "9.00000"),(17, "9.00001"),(18, "9.00009"),(19, "9.09999"),
      (20, "9.90000"),(21, "9.90001"),(22, "9.99998"),(23, "9.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_5_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal_18_17_from_decimalv2_20_5_3_not_nullable order by 1;'

}