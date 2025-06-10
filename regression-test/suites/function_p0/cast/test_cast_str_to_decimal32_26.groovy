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


suite("test_cast_str_to_decimal32_26") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_26_26_9_9;"
    sql "create table test_cast_str_to_decimal32_26_26_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_26_26_9_9 values (5220, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5221, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5222, ".0000000004"),(5223, ".0000000014"),(5224, ".0000000094"),(5225, ".0999999994"),(5226, ".9000000004"),(5227, ".9000000014"),(5228, ".9999999984"),(5229, ".9999999994"),
      (5230, ".0000000005"),(5231, ".0000000015"),(5232, ".0000000095"),(5233, ".0999999995"),(5234, ".9000000005"),(5235, ".9000000015"),(5236, ".9999999985"),(5237, ".9999999994"),(5238, "-.0000000004"),(5239, "-.0000000014"),
      (5240, "-.0000000094"),(5241, "-.0999999994"),(5242, "-.9000000004"),(5243, "-.9000000014"),(5244, "-.9999999984"),(5245, "-.9999999994"),(5246, "-.0000000005"),(5247, "-.0000000015"),(5248, "-.0000000095"),(5249, "-.0999999995"),
      (5250, "-.9000000005"),(5251, "-.9000000015"),(5252, "-.9999999985"),(5253, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_26_26_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_26_26_9_9 order by 1;'

}