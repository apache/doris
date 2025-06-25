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


suite("test_cast_to_decimal64_18_18_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_19_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_19_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_19_9 values (5, "0.000000000"),(6, "0.000000001"),(7, "0.000000009"),(8, "0.099999999"),(9, "0.900000000"),(10, "0.900000001"),(11, "0.999999998"),(12, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_19_18 values (13, "0.000000000000000000"),(14, "0.000000000000000001"),(15, "0.000000000000000009"),(16, "0.099999999999999999"),(17, "0.900000000000000000"),(18, "0.900000000000000001"),(19, "0.999999999999999998"),(20, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_19_19 values (21, "0.0000000000000000000"),(22, "0.0000000000000000001"),(23, "0.0000000000000000009"),(24, "0.0999999999999999999"),(25, "0.9000000000000000000"),(26, "0.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_37_0 values (27, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_37_1 values (28, "0.0"),(29, "0.1"),(30, "0.8"),(31, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_37_18 values (32, "0.000000000000000000"),(33, "0.000000000000000001"),(34, "0.000000000000000009"),(35, "0.099999999999999999"),(36, "0.900000000000000000"),(37, "0.900000000000000001"),(38, "0.999999999999999998"),(39, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_37_36 values (40, "0.000000000000000000000000000000000000"),(41, "0.000000000000000000000000000000000001"),(42, "0.000000000000000000000000000000000009"),(43, "0.099999999999999999999999999999999999"),(44, "0.900000000000000000000000000000000000"),(45, "0.900000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_37_37 values (46, "0.0000000000000000000000000000000000000"),(47, "0.0000000000000000000000000000000000001"),(48, "0.0000000000000000000000000000000000009"),(49, "0.0999999999999999999999999999999999999"),(50, "0.9000000000000000000000000000000000000"),(51, "0.9000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_38_0 values (52, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_38_1 values (53, "0.0"),(54, "0.1"),(55, "0.8"),(56, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_38_19 values (57, "0.0000000000000000000"),(58, "0.0000000000000000001"),(59, "0.0000000000000000009"),(60, "0.0999999999999999999"),(61, "0.9000000000000000000"),(62, "0.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_38_37 values (63, "0.0000000000000000000000000000000000000"),(64, "0.0000000000000000000000000000000000001"),(65, "0.0000000000000000000000000000000000009"),(66, "0.0999999999999999999999999999999999999"),(67, "0.9000000000000000000000000000000000000"),(68, "0.9000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal64_18_18_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_decimal128i_38_38 values (69, "0.00000000000000000000000000000000000000"),(70, "0.00000000000000000000000000000000000001"),(71, "0.00000000000000000000000000000000000009"),(72, "0.09999999999999999999999999999999999999"),(73, "0.90000000000000000000000000000000000000"),(74, "0.90000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_decimal128i_38_38 order by 1;'

}