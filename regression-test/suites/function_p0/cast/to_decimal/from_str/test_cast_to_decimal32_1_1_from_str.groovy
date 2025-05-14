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
    sql "drop table if exists test_cast_to_decimal32_1_1_from_str_1_1_1;"
    sql "create table test_cast_to_decimal32_1_1_from_str_1_1_1(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_1_from_str_1_1_1 values (34, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(35, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(36, ".04"),(37, ".14"),(38, ".84"),(39, ".94"),(40, ".05"),(41, ".15"),(42, ".85"),(43, ".94"),
      (44, "-.04"),(45, "-.14"),(46, "-.84"),(47, "-.94"),(48, "-.05"),(49, "-.15"),(50, "-.85"),(51, "-.94");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 1)) from test_cast_to_decimal32_1_1_from_str_1_1_1 order by 1;'

}