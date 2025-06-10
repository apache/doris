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


suite("test_cast_str_to_decimal32_27") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_27_27_9_9;"
    sql "create table test_cast_str_to_decimal32_27_27_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_27_27_9_9 values (5254, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5255, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5256, ".0000000004"),(5257, ".0000000014"),(5258, ".0000000094"),(5259, ".0999999994"),(5260, ".9000000004"),(5261, ".9000000014"),(5262, ".9999999984"),(5263, ".9999999994"),
      (5264, ".0000000005"),(5265, ".0000000015"),(5266, ".0000000095"),(5267, ".0999999995"),(5268, ".9000000005"),(5269, ".9000000015"),(5270, ".9999999985"),(5271, ".9999999994"),(5272, "-.0000000004"),(5273, "-.0000000014"),
      (5274, "-.0000000094"),(5275, "-.0999999994"),(5276, "-.9000000004"),(5277, "-.9000000014"),(5278, "-.9999999984"),(5279, "-.9999999994"),(5280, "-.0000000005"),(5281, "-.0000000015"),(5282, "-.0000000095"),(5283, "-.0999999995"),
      (5284, "-.9000000005"),(5285, "-.9000000015"),(5286, "-.9999999985"),(5287, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_27_27_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_27_27_9_9 order by 1;'

}