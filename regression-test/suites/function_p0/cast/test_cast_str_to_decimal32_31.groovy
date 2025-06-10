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


suite("test_cast_str_to_decimal32_31") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_31_31_9_9;"
    sql "create table test_cast_str_to_decimal32_31_31_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_31_31_9_9 values (5390, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5391, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5392, ".0000000004"),(5393, ".0000000014"),(5394, ".0000000094"),(5395, ".0999999994"),(5396, ".9000000004"),(5397, ".9000000014"),(5398, ".9999999984"),(5399, ".9999999994"),
      (5400, ".0000000005"),(5401, ".0000000015"),(5402, ".0000000095"),(5403, ".0999999995"),(5404, ".9000000005"),(5405, ".9000000015"),(5406, ".9999999985"),(5407, ".9999999994"),(5408, "-.0000000004"),(5409, "-.0000000014"),
      (5410, "-.0000000094"),(5411, "-.0999999994"),(5412, "-.9000000004"),(5413, "-.9000000014"),(5414, "-.9999999984"),(5415, "-.9999999994"),(5416, "-.0000000005"),(5417, "-.0000000015"),(5418, "-.0000000095"),(5419, "-.0999999995"),
      (5420, "-.9000000005"),(5421, "-.9000000015"),(5422, "-.9999999985"),(5423, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_31_31_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_31_31_9_9 order by 1;'

}