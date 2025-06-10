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


suite("test_cast_str_to_decimal32_30") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_30_30_9_9;"
    sql "create table test_cast_str_to_decimal32_30_30_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_30_30_9_9 values (5356, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5357, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5358, ".0000000004"),(5359, ".0000000014"),(5360, ".0000000094"),(5361, ".0999999994"),(5362, ".9000000004"),(5363, ".9000000014"),(5364, ".9999999984"),(5365, ".9999999994"),
      (5366, ".0000000005"),(5367, ".0000000015"),(5368, ".0000000095"),(5369, ".0999999995"),(5370, ".9000000005"),(5371, ".9000000015"),(5372, ".9999999985"),(5373, ".9999999994"),(5374, "-.0000000004"),(5375, "-.0000000014"),
      (5376, "-.0000000094"),(5377, "-.0999999994"),(5378, "-.9000000004"),(5379, "-.9000000014"),(5380, "-.9999999984"),(5381, "-.9999999994"),(5382, "-.0000000005"),(5383, "-.0000000015"),(5384, "-.0000000095"),(5385, "-.0999999995"),
      (5386, "-.9000000005"),(5387, "-.9000000015"),(5388, "-.9999999985"),(5389, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_30_30_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_30_30_9_9 order by 1;'

}