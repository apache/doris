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


suite("test_cast_str_to_decimal32_25") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_25_25_9_9;"
    sql "create table test_cast_str_to_decimal32_25_25_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_25_25_9_9 values (5186, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5187, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5188, ".0000000004"),(5189, ".0000000014"),(5190, ".0000000094"),(5191, ".0999999994"),(5192, ".9000000004"),(5193, ".9000000014"),(5194, ".9999999984"),(5195, ".9999999994"),
      (5196, ".0000000005"),(5197, ".0000000015"),(5198, ".0000000095"),(5199, ".0999999995"),(5200, ".9000000005"),(5201, ".9000000015"),(5202, ".9999999985"),(5203, ".9999999994"),(5204, "-.0000000004"),(5205, "-.0000000014"),
      (5206, "-.0000000094"),(5207, "-.0999999994"),(5208, "-.9000000004"),(5209, "-.9000000014"),(5210, "-.9999999984"),(5211, "-.9999999994"),(5212, "-.0000000005"),(5213, "-.0000000015"),(5214, "-.0000000095"),(5215, "-.0999999995"),
      (5216, "-.9000000005"),(5217, "-.9000000015"),(5218, "-.9999999985"),(5219, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_25_25_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_25_25_9_9 order by 1;'

}