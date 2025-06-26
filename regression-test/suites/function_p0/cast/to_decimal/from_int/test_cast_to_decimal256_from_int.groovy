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


suite("test_cast_to_decimal256_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_from_int_0_nullable;"
    sql "create table test_cast_to_decimal256_from_int_0_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_0_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_0_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_0_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_0_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_0_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_1_nullable;"
    sql "create table test_cast_to_decimal256_from_int_1_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_1_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_1_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_1_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_1_not_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_1_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_2_nullable;"
    sql "create table test_cast_to_decimal256_from_int_2_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_2_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_2_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_2_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_2_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_2_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_2_not_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_2_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_2_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_3_nullable;"
    sql "create table test_cast_to_decimal256_from_int_3_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_3_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_3_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_3_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_3_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_3_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_3_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_3_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_3_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_4_nullable;"
    sql "create table test_cast_to_decimal256_from_int_4_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_4_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_4_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_4_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_4_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_4_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_4_not_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_4_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_4_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_5_nullable;"
    sql "create table test_cast_to_decimal256_from_int_5_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_5_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_5_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_5_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_5_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_5_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_5_not_nullable values (0, "-128"),(1, "-127"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "126"),(8, "127");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_5_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_5_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_6_nullable;"
    sql "create table test_cast_to_decimal256_from_int_6_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_6_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_6_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_6_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_6_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_6_not_nullable(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_6_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_6_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_6_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_7_nullable;"
    sql "create table test_cast_to_decimal256_from_int_7_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_7_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_7_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_7_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_7_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_7_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_7_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_7_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_7_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_8_nullable;"
    sql "create table test_cast_to_decimal256_from_int_8_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_8_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_8_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_8_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_8_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_8_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_8_not_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_8_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_8_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_9_nullable;"
    sql "create table test_cast_to_decimal256_from_int_9_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_9_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_9_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_9_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_9_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_9_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_9_not_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_9_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_9_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_10_nullable;"
    sql "create table test_cast_to_decimal256_from_int_10_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_10_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_10_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_10_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_10_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_10_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_10_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_10_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_10_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_11_nullable;"
    sql "create table test_cast_to_decimal256_from_int_11_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_11_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_11_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_11_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_11_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_11_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_11_not_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_11_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_11_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_12_nullable;"
    sql "create table test_cast_to_decimal256_from_int_12_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_12_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_12_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_12_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_12_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_12_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_12_not_nullable values (0, "-32768"),(1, "-32767"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "32766"),(8, "32767");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_12_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_12_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_13_nullable;"
    sql "create table test_cast_to_decimal256_from_int_13_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_13_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_13_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_13_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_13_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_13_not_nullable(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_13_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_13_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_13_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_14_nullable;"
    sql "create table test_cast_to_decimal256_from_int_14_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_14_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_14_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_14_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_14_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_14_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_14_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_14_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_14_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_15_nullable;"
    sql "create table test_cast_to_decimal256_from_int_15_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_15_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_15_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_15_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_15_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_15_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_15_not_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_15_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_15_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_16_nullable;"
    sql "create table test_cast_to_decimal256_from_int_16_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_16_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_16_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_16_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_16_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_16_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_16_not_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_16_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_16_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_17_nullable;"
    sql "create table test_cast_to_decimal256_from_int_17_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_17_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_17_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_17_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_17_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_17_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_17_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_17_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_17_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_18_nullable;"
    sql "create table test_cast_to_decimal256_from_int_18_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_18_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_18_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_18_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_18_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_18_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_18_not_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_18_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_18_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_19_nullable;"
    sql "create table test_cast_to_decimal256_from_int_19_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_19_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_19_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_19_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_19_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_19_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_19_not_nullable values (0, "-2147483648"),(1, "-2147483647"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "2147483646"),(8, "2147483647");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_19_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_19_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_20_nullable;"
    sql "create table test_cast_to_decimal256_from_int_20_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_20_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_20_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_20_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_20_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_20_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_20_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_20_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_20_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_21_nullable;"
    sql "create table test_cast_to_decimal256_from_int_21_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_21_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_21_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_21_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_21_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_21_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_21_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_21_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_21_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_22_nullable;"
    sql "create table test_cast_to_decimal256_from_int_22_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_22_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_22_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_22_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_22_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_22_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_22_not_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_22_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_22_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_23_nullable;"
    sql "create table test_cast_to_decimal256_from_int_23_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_23_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_23_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_23_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_23_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_23_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_23_not_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_23_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_23_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_24_nullable;"
    sql "create table test_cast_to_decimal256_from_int_24_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_24_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_24_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_24_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_24_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_24_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_24_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_24_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_24_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_25_nullable;"
    sql "create table test_cast_to_decimal256_from_int_25_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_25_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_25_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_25_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_25_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_25_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_25_not_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_25_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_25_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_26_nullable;"
    sql "create table test_cast_to_decimal256_from_int_26_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_26_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_26_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_26_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_26_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_26_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_26_not_nullable values (0, "-9223372036854775808"),(1, "-9223372036854775807"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "9223372036854775806"),(8, "9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_26_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_26_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_27_nullable;"
    sql "create table test_cast_to_decimal256_from_int_27_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_27_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_27_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_27_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_27_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_27_not_nullable(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_27_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_27_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_27_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_28_nullable;"
    sql "create table test_cast_to_decimal256_from_int_28_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_28_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_28_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_28_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_28_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_28_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_28_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_28_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_from_int_28_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_29_nullable;"
    sql "create table test_cast_to_decimal256_from_int_29_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_29_nullable values (0, "-99999999999999999999999999999999999999"),(1, "-90000000000000000000000000000000000001"),(2, "-90000000000000000000000000000000000000"),(3, "-9999999999999999999999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "9999999999999999999999999999999999999"),(10, "90000000000000000000000000000000000000"),(11, "90000000000000000000000000000000000001"),(12, "99999999999999999999999999999999999999")
      ,(13, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_29_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_29_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_29_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_29_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_29_not_nullable values (0, "-99999999999999999999999999999999999999"),(1, "-90000000000000000000000000000000000001"),(2, "-90000000000000000000000000000000000000"),(3, "-9999999999999999999999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "9999999999999999999999999999999999999"),(10, "90000000000000000000000000000000000000"),(11, "90000000000000000000000000000000000001"),(12, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_29_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_from_int_29_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_30_nullable;"
    sql "create table test_cast_to_decimal256_from_int_30_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_30_nullable values (0, "-9999999999999999999"),(1, "-9000000000000000001"),(2, "-9000000000000000000"),(3, "-999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "999999999999999999"),(10, "9000000000000000000"),(11, "9000000000000000001"),(12, "9999999999999999999")
      ,(13, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_30_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_30_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_30_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_30_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_30_not_nullable values (0, "-9999999999999999999"),(1, "-9000000000000000001"),(2, "-9000000000000000000"),(3, "-999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "999999999999999999"),(10, "9000000000000000000"),(11, "9000000000000000001"),(12, "9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_30_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_from_int_30_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_31_nullable;"
    sql "create table test_cast_to_decimal256_from_int_31_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_31_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_31_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_31_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_31_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_31_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_31_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_31_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_from_int_31_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_32_nullable;"
    sql "create table test_cast_to_decimal256_from_int_32_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_32_nullable values (0, "-170141183460469231731687303715884105728"),(1, "-170141183460469231731687303715884105727"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "170141183460469231731687303715884105726"),(8, "170141183460469231731687303715884105727")
      ,(9, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_32_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_32_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_32_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_32_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_32_not_nullable values (0, "-170141183460469231731687303715884105728"),(1, "-170141183460469231731687303715884105727"),(2, "-9"),(3, "-1"),(4, "0"),(5, "1"),(6, "9"),(7, "170141183460469231731687303715884105726"),(8, "170141183460469231731687303715884105727");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_32_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_from_int_32_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_33_nullable;"
    sql "create table test_cast_to_decimal256_from_int_33_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_33_nullable values (0, "-99999999999999999999999999999999999999"),(1, "-90000000000000000000000000000000000001"),(2, "-90000000000000000000000000000000000000"),(3, "-9999999999999999999999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "9999999999999999999999999999999999999"),(10, "90000000000000000000000000000000000000"),(11, "90000000000000000000000000000000000001"),(12, "99999999999999999999999999999999999999")
      ,(13, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_33_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_33_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_33_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_33_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_33_not_nullable values (0, "-99999999999999999999999999999999999999"),(1, "-90000000000000000000000000000000000001"),(2, "-90000000000000000000000000000000000000"),(3, "-9999999999999999999999999999999999999"),(4, "-9"),(5, "-1"),(6, "0"),(7, "1"),(8, "9"),(9, "9999999999999999999999999999999999999"),(10, "90000000000000000000000000000000000000"),(11, "90000000000000000000000000000000000001"),(12, "99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_33_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_from_int_33_not_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_34_nullable;"
    sql "create table test_cast_to_decimal256_from_int_34_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_34_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9")
      ,(7, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_34_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_34_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal256_from_int_34_not_nullable;"
    sql "create table test_cast_to_decimal256_from_int_34_not_nullable(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_from_int_34_not_nullable values (0, "-9"),(1, "-8"),(2, "-1"),(3, "0"),(4, "1"),(5, "8"),(6, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_34_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_from_int_34_not_nullable order by 1;'

}