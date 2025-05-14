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
    sql "drop table if exists test_cast_to_decimal64_18_18_from_str_23_18_18;"
    sql "create table test_cast_to_decimal64_18_18_from_str_23_18_18(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_18_from_str_23_18_18 values (5134, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5135, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5136, ".0000000000000000004"),(5137, ".0000000000000000014"),(5138, ".0000000000000000094"),(5139, ".0999999999999999994"),(5140, ".9000000000000000004"),(5141, ".9000000000000000014"),(5142, ".9999999999999999984"),(5143, ".9999999999999999994"),
      (5144, ".0000000000000000005"),(5145, ".0000000000000000015"),(5146, ".0000000000000000095"),(5147, ".0999999999999999995"),(5148, ".9000000000000000005"),(5149, ".9000000000000000015"),(5150, ".9999999999999999985"),(5151, ".9999999999999999994"),(5152, "-.0000000000000000004"),(5153, "-.0000000000000000014"),
      (5154, "-.0000000000000000094"),(5155, "-.0999999999999999994"),(5156, "-.9000000000000000004"),(5157, "-.9000000000000000014"),(5158, "-.9999999999999999984"),(5159, "-.9999999999999999994"),(5160, "-.0000000000000000005"),(5161, "-.0000000000000000015"),(5162, "-.0000000000000000095"),(5163, "-.0999999999999999995"),
      (5164, "-.9000000000000000005"),(5165, "-.9000000000000000015"),(5166, "-.9999999999999999985"),(5167, "-.9999999999999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(18, 18)) from test_cast_to_decimal64_18_18_from_str_23_18_18 order by 1;'

}