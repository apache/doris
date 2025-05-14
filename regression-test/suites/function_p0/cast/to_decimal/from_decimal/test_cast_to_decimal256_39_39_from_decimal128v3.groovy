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


suite("test_cast_to_decimal256_39_39_from_decimal128v3") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_19_0 values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_19_1 values (1, "0.0"),(2, "0.1"),(3, "0.8"),(4, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_19_9 values (5, "0.000000000"),(6, "0.000000001"),(7, "0.000000009"),(8, "0.099999999"),(9, "0.900000000"),(10, "0.900000001"),(11, "0.999999998"),(12, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_19_18 values (13, "0.000000000000000000"),(14, "0.000000000000000001"),(15, "0.000000000000000009"),(16, "0.099999999999999999"),(17, "0.900000000000000000"),(18, "0.900000000000000001"),(19, "0.999999999999999998"),(20, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_19_19 values (21, "0.0000000000000000000"),(22, "0.0000000000000000001"),(23, "0.0000000000000000009"),(24, "0.0999999999999999999"),(25, "0.9000000000000000000"),(26, "0.9000000000000000001"),(27, "0.9999999999999999998"),(28, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_37_0 values (29, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_37_1 values (30, "0.0"),(31, "0.1"),(32, "0.8"),(33, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_37_18 values (34, "0.000000000000000000"),(35, "0.000000000000000001"),(36, "0.000000000000000009"),(37, "0.099999999999999999"),(38, "0.900000000000000000"),(39, "0.900000000000000001"),(40, "0.999999999999999998"),(41, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_37_36 values (42, "0.000000000000000000000000000000000000"),(43, "0.000000000000000000000000000000000001"),(44, "0.000000000000000000000000000000000009"),(45, "0.099999999999999999999999999999999999"),(46, "0.900000000000000000000000000000000000"),(47, "0.900000000000000000000000000000000001"),(48, "0.999999999999999999999999999999999998"),(49, "0.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_37_37 values (50, "0.0000000000000000000000000000000000000"),(51, "0.0000000000000000000000000000000000001"),(52, "0.0000000000000000000000000000000000009"),(53, "0.0999999999999999999999999999999999999"),(54, "0.9000000000000000000000000000000000000"),(55, "0.9000000000000000000000000000000000001"),(56, "0.9999999999999999999999999999999999998"),(57, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_38_0 values (58, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_38_1 values (59, "0.0"),(60, "0.1"),(61, "0.8"),(62, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_38_19 values (63, "0.0000000000000000000"),(64, "0.0000000000000000001"),(65, "0.0000000000000000009"),(66, "0.0999999999999999999"),(67, "0.9000000000000000000"),(68, "0.9000000000000000001"),(69, "0.9999999999999999998"),(70, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_38_37 values (71, "0.0000000000000000000000000000000000000"),(72, "0.0000000000000000000000000000000000001"),(73, "0.0000000000000000000000000000000000009"),(74, "0.0999999999999999999999999999999999999"),(75, "0.9000000000000000000000000000000000000"),(76, "0.9000000000000000000000000000000000001"),(77, "0.9999999999999999999999999999999999998"),(78, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_39_39_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal256_39_39_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_39_39_from_decimal128v3_38_38 values (79, "0.00000000000000000000000000000000000000"),(80, "0.00000000000000000000000000000000000001"),(81, "0.00000000000000000000000000000000000009"),(82, "0.09999999999999999999999999999999999999"),(83, "0.90000000000000000000000000000000000000"),(84, "0.90000000000000000000000000000000000001"),(85, "0.99999999999999999999999999999999999998"),(86, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(39, 39)) from test_cast_to_decimal256_39_39_from_decimal128v3_38_38 order by 1;'

}