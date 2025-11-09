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


suite("test_cast_to_decimal64_18_18_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_18_18_from_str_23_nullable;"
    sql "create table test_cast_to_decimal64_18_18_from_str_23_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_str_23_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, ".0000000000000000004"),(3, ".0000000000000000014"),(4, ".0000000000000000094"),(5, ".0999999999999999994"),(6, ".9000000000000000004"),(7, ".9000000000000000014"),(8, ".9999999999999999984"),(9, ".9999999999999999994"),(10, ".0000000000000000005"),(11, ".0000000000000000015"),(12, ".0000000000000000095"),(13, ".0999999999999999995"),(14, ".9000000000000000005"),(15, ".9000000000000000015"),(16, ".9999999999999999985"),(17, ".9999999999999999994"),(18, "-.0000000000000000004"),(19, "-.0000000000000000014"),
      (20, "-.0000000000000000094"),(21, "-.0999999999999999994"),(22, "-.9000000000000000004"),(23, "-.9000000000000000014"),(24, "-.9999999999999999984"),(25, "-.9999999999999999994"),(26, "-.0000000000000000005"),(27, "-.0000000000000000015"),(28, "-.0000000000000000095"),(29, "-.0999999999999999995"),(30, "-.9000000000000000005"),(31, "-.9000000000000000015"),(32, "-.9999999999999999985"),(33, "-.9999999999999999994")
      ,(34, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_18_from_str_23_not_nullable;"
    sql "create table test_cast_to_decimal64_18_18_from_str_23_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_str_23_not_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, ".0000000000000000004"),(3, ".0000000000000000014"),(4, ".0000000000000000094"),(5, ".0999999999999999994"),(6, ".9000000000000000004"),(7, ".9000000000000000014"),(8, ".9999999999999999984"),(9, ".9999999999999999994"),(10, ".0000000000000000005"),(11, ".0000000000000000015"),(12, ".0000000000000000095"),(13, ".0999999999999999995"),(14, ".9000000000000000005"),(15, ".9000000000000000015"),(16, ".9999999999999999985"),(17, ".9999999999999999994"),(18, "-.0000000000000000004"),(19, "-.0000000000000000014"),
      (20, "-.0000000000000000094"),(21, "-.0999999999999999994"),(22, "-.9000000000000000004"),(23, "-.9000000000000000014"),(24, "-.9999999999999999984"),(25, "-.9999999999999999994"),(26, "-.0000000000000000005"),(27, "-.0000000000000000015"),(28, "-.0000000000000000095"),(29, "-.0999999999999999995"),(30, "-.9000000000000000005"),(31, "-.9000000000000000015"),(32, "-.9999999999999999985"),(33, "-.9999999999999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_not_nullable order by 1;'

}