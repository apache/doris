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


suite("test_cast_str_to_decimal32_29") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_decimal32_29_29_9_9;"
    sql "create table test_cast_str_to_decimal32_29_29_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_decimal32_29_29_9_9 values (5322, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5323, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5324, ".0000000004"),(5325, ".0000000014"),(5326, ".0000000094"),(5327, ".0999999994"),(5328, ".9000000004"),(5329, ".9000000014"),(5330, ".9999999984"),(5331, ".9999999994"),
      (5332, ".0000000005"),(5333, ".0000000015"),(5334, ".0000000095"),(5335, ".0999999995"),(5336, ".9000000005"),(5337, ".9000000015"),(5338, ".9999999985"),(5339, ".9999999994"),(5340, "-.0000000004"),(5341, "-.0000000014"),
      (5342, "-.0000000094"),(5343, "-.0999999994"),(5344, "-.9000000004"),(5345, "-.9000000014"),(5346, "-.9999999984"),(5347, "-.9999999994"),(5348, "-.0000000005"),(5349, "-.0000000015"),(5350, "-.0000000095"),(5351, "-.0999999995"),
      (5352, "-.9000000005"),(5353, "-.9000000015"),(5354, "-.9999999985"),(5355, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_29_29_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_str_to_decimal32_29_29_9_9 order by 1;'

}