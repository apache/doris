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
    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_0_0_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_0_0_nullable(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_0_0_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_0_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_0_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_0_0_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_0_0_not_nullable(f1 int, f2 decimalv3(39, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_0_0_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_0_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_0_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_1_1_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_1_1_nullable(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_1_1_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_1_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_1_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_1_1_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_1_1_not_nullable(f1 int, f2 decimalv3(39, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_1_1_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_1_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_1_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_19_2_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_19_2_nullable(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_19_2_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_19_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_19_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_19_2_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_19_2_not_nullable(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_19_2_not_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.0999999999999999999"),(4, "0.9000000000000000000"),(5, "0.9000000000000000001"),(6, "0.9999999999999999998"),(7, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_19_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_19_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_38_3_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_38_3_nullable(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_38_3_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_38_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_38_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_38_3_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_38_3_not_nullable(f1 int, f2 decimalv3(39, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_38_3_not_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_38_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_38_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_39_4_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_39_4_nullable(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_39_4_nullable values (0, "0.000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_39_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_39_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_39_39_4_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_39_39_4_not_nullable(f1 int, f2 decimalv3(39, 39)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_39_39_4_not_nullable values (0, "0.000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_39_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_39_39_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_0_5_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_0_5_nullable(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_0_5_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_0_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_0_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_0_5_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_0_5_not_nullable(f1 int, f2 decimalv3(75, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_0_5_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_0_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_0_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_1_6_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_1_6_nullable(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_1_6_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_1_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_1_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_1_6_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_1_6_not_nullable(f1 int, f2 decimalv3(75, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_1_6_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_1_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_1_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_37_7_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_37_7_nullable(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_37_7_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999")
      ,(8, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_37_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_37_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_37_7_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_37_7_not_nullable(f1 int, f2 decimalv3(75, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_37_7_not_nullable values (0, "0.0000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000001"),(6, "0.9999999999999999999999999999999999998"),(7, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_37_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_37_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_74_8_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_74_8_nullable(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_74_8_nullable values (0, "0.00000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000000000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_74_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_74_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_74_8_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_74_8_not_nullable(f1 int, f2 decimalv3(75, 74)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_74_8_not_nullable values (0, "0.00000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_74_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_74_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_75_9_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_75_9_nullable(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_75_9_nullable values (0, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000000000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_75_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_75_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_75_75_9_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_75_75_9_not_nullable(f1 int, f2 decimalv3(75, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_75_75_9_not_nullable values (0, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_75_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_75_75_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_0_10_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_0_10_nullable(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_0_10_nullable values (0, "0")
      ,(1, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_0_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_0_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_0_10_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_0_10_not_nullable(f1 int, f2 decimalv3(76, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_0_10_not_nullable values (0, "0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_0_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_0_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_1_11_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_1_11_nullable(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_1_11_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9")
      ,(4, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_1_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_1_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_1_11_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_1_11_not_nullable(f1 int, f2 decimalv3(76, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_1_11_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.8"),(3, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_1_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_1_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_38_12_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_38_12_nullable(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_38_12_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_38_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_38_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_38_12_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_38_12_not_nullable(f1 int, f2 decimalv3(76, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_38_12_not_nullable values (0, "0.00000000000000000000000000000000000000"),(1, "0.00000000000000000000000000000000000001"),(2, "0.00000000000000000000000000000000000009"),(3, "0.09999999999999999999999999999999999999"),(4, "0.90000000000000000000000000000000000000"),(5, "0.90000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_38_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_38_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_75_13_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_75_13_nullable(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_75_13_nullable values (0, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000000000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_75_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_75_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_75_13_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_75_13_not_nullable(f1 int, f2 decimalv3(76, 75)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_75_13_not_nullable values (0, "0.000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.099999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.900000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.900000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_75_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_75_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_76_14_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_76_14_nullable(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_76_14_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000000000000000000000000000000000000000000001")
      ,(6, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_76_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_76_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal_37_37_from_decimal_76_76_14_not_nullable;"
    sql "create table test_cast_to_decimal_37_37_from_decimal_76_76_14_not_nullable(f1 int, f2 decimalv3(76, 76)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_37_37_from_decimal_76_76_14_not_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000"),(1, "0.0000000000000000000000000000000000000000000000000000000000000000000000000001"),(2, "0.0000000000000000000000000000000000000000000000000000000000000000000000000009"),(3, "0.0999999999999999999999999999999999999999999999999999999999999999999999999999"),(4, "0.9000000000000000000000000000000000000000000000000000000000000000000000000000"),(5, "0.9000000000000000000000000000000000000000000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_76_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(37, 37)) from test_cast_to_decimal_37_37_from_decimal_76_76_14_not_nullable order by 1;'

}