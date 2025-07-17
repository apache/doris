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


suite("test_cast_to_decimal32_1_0_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_str_0_nullable;"
    sql "create table test_cast_to_decimal32_1_0_from_str_0_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_str_0_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, "0"),(3, "1"),(4, "8"),(5, "9"),(6, "0."),(7, "1."),(8, "8."),(9, "9."),(10, "-0"),(11, "-1"),(12, "-8"),(13, "-9"),(14, "-0."),(15, "-1."),(16, "-8."),(17, "-9."),(18, "0.49999"),(19, "1.49999"),
      (20, "8.49999"),(21, "9.49999"),(22, "0.5"),(23, "1.5"),(24, "8.5"),(25, "9.49999"),(26, "-0.49999"),(27, "-1.49999"),(28, "-8.49999"),(29, "-9.49999"),(30, "-0.5"),(31, "-1.5"),(32, "-8.5"),(33, "-9.49999")
      ,(34, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_str_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_str_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_str_0_not_nullable;"
    sql "create table test_cast_to_decimal32_1_0_from_str_0_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_str_0_not_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, "0"),(3, "1"),(4, "8"),(5, "9"),(6, "0."),(7, "1."),(8, "8."),(9, "9."),(10, "-0"),(11, "-1"),(12, "-8"),(13, "-9"),(14, "-0."),(15, "-1."),(16, "-8."),(17, "-9."),(18, "0.49999"),(19, "1.49999"),
      (20, "8.49999"),(21, "9.49999"),(22, "0.5"),(23, "1.5"),(24, "8.5"),(25, "9.49999"),(26, "-0.49999"),(27, "-1.49999"),(28, "-8.49999"),(29, "-9.49999"),(30, "-0.5"),(31, "-1.5"),(32, "-8.5"),(33, "-9.49999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_str_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_str_0_not_nullable order by 1;'

}