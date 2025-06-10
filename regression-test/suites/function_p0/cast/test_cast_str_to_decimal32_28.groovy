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


suite("test_cast_str_to_decimal32_28") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_28_28_9_9;"
    sql "create table test_cast_str_to_decimal32_28_28_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_28_28_9_9 values (5288, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5289, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5290, ".0000000004"),(5291, ".0000000014"),(5292, ".0000000094"),(5293, ".0999999994"),(5294, ".9000000004"),(5295, ".9000000014"),(5296, ".9999999984"),(5297, ".9999999994"),
      (5298, ".0000000005"),(5299, ".0000000015"),(5300, ".0000000095"),(5301, ".0999999995"),(5302, ".9000000005"),(5303, ".9000000015"),(5304, ".9999999985"),(5305, ".9999999994"),(5306, "-.0000000004"),(5307, "-.0000000014"),
      (5308, "-.0000000094"),(5309, "-.0999999994"),(5310, "-.9000000004"),(5311, "-.9000000014"),(5312, "-.9999999984"),(5313, "-.9999999994"),(5314, "-.0000000005"),(5315, "-.0000000015"),(5316, "-.0000000095"),(5317, "-.0999999995"),
      (5318, "-.9000000005"),(5319, "-.9000000015"),(5320, "-.9999999985"),(5321, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_28_28_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_28_28_9_9 order by 1;'

}