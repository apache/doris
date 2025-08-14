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


suite("test_cast_to_decimal32_1_1_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_1_from_str_1_nullable;"
    sql "create table test_cast_to_decimal32_1_1_from_str_1_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_str_1_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, ".04"),(3, ".14"),(4, ".84"),(5, ".94"),(6, ".05"),(7, ".15"),(8, ".85"),(9, ".94"),(10, "-.04"),(11, "-.14"),(12, "-.84"),(13, "-.94"),(14, "-.05"),(15, "-.15"),(16, "-.85"),(17, "-.94")
      ,(18, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_1_from_str_1_not_nullable;"
    sql "create table test_cast_to_decimal32_1_1_from_str_1_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_str_1_not_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, ".04"),(3, ".14"),(4, ".84"),(5, ".94"),(6, ".05"),(7, ".15"),(8, ".85"),(9, ".94"),(10, "-.04"),(11, "-.14"),(12, "-.84"),(13, "-.94"),(14, "-.05"),(15, "-.15"),(16, "-.85"),(17, "-.94");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_not_nullable order by 1;'

}