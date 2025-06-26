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


suite("test_cast_to_decimal128i_37_37_from_decimal256") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_0(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_1(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_19;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_19(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_19 values (5, "0.0000000000000000000"),(6, "0.0000000000000000001"),(7, "0.0000000000000000009"),(8, "0.0999999999999999999"),(9, "0.9000000000000000000"),(10, "0.9000000000000000001"),(11, "0.9999999999999999998"),(12, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_38(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_38 values (13, "0.00000000000000000000000000000000000000"),(14, "0.00000000000000000000000000000000000001"),(15, "0.00000000000000000000000000000000000009"),(16, "0.09999999999999999999999999999999999999"),(17, "0.90000000000000000000000000000000000000"),(18, "0.90000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_39_39;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_39_39(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_39_39 values (19, "0.000000000000000000000000000000000000000"),(20, "0.000000000000000000000000000000000000001"),(21, "0.000000000000000000000000000000000000009"),(22, "0.099999999999999999999999999999999999999"),(23, "0.900000000000000000000000000000000000000"),(24, "0.900000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_39 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_39_39 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_0(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_0 values (25, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_1(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_1 values (26, "0.0"),(27, "0.1"),(28, "0.8"),(29, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_37;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_37(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_37 values (30, "0.0000000000000000000000000000000000000"),(31, "0.0000000000000000000000000000000000001"),(32, "0.0000000000000000000000000000000000009"),(33, "0.0999999999999999999999999999999999999"),(34, "0.9000000000000000000000000000000000000"),(35, "0.9000000000000000000000000000000000001"),(36, "0.9999999999999999999999999999999999998"),(37, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_74;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_74(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_74 values (38, "0.00000000000000000000000000000000000000000000000000000000000000000000000000"),(39, "0.00000000000000000000000000000000000000000000000000000000000000000000000001"),(40, "0.00000000000000000000000000000000000000000000000000000000000000000000000009"),(41, "0.09999999999999999999999999999999999999999999999999999999999999999999999999"),(42, "0.90000000000000000000000000000000000000000000000000000000000000000000000000"),(43, "0.90000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_74 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_74 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_75_75;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_75_75(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_75_75 values (44, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(45, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(46, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(47, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(48, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(49, "0.900000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_75 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_75_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_0;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_0(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_0 values (50, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_1;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_1(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_1 values (51, "0.0"),(52, "0.1"),(53, "0.8"),(54, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_38;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_38(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_38 values (55, "0.00000000000000000000000000000000000000"),(56, "0.00000000000000000000000000000000000001"),(57, "0.00000000000000000000000000000000000009"),(58, "0.09999999999999999999999999999999999999"),(59, "0.90000000000000000000000000000000000000"),(60, "0.90000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_38 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_75;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_75(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_75 values (61, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(62, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(63, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(64, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(65, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(66, "0.900000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_75 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_75 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_37_37_from_decimal256_76_76;"
    sql "create table test_cast_to_decimal128i_37_37_from_decimal256_76_76(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_37_37_from_decimal256_76_76 values (67, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000"),(68, "0.0000000000000000000000000000000000000000000000000000000000000000000000000001"),(69, "0.0000000000000000000000000000000000000000000000000000000000000000000000000009"),(70, "0.0999999999999999999999999999999999999999999999999999999999999999999999999999"),(71, "0.9000000000000000000000000000000000000000000000000000000000000000000000000000"),(72, "0.9000000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_76 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal128i_37_37_from_decimal256_76_76 order by 1;'

}