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


suite("test_cast_to_decimal32_9_9_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_9_from_str_32_9_9;"
    sql "create table test_cast_to_decimal32_9_9_from_str_32_9_9(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_9_from_str_32_9_9 values (5424, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5425, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(5426, ".0000000004"),(5427, ".0000000014"),(5428, ".0000000094"),(5429, ".0999999994"),(5430, ".9000000004"),(5431, ".9000000014"),(5432, ".9999999984"),(5433, ".9999999994"),
      (5434, ".0000000005"),(5435, ".0000000015"),(5436, ".0000000095"),(5437, ".0999999995"),(5438, ".9000000005"),(5439, ".9000000015"),(5440, ".9999999985"),(5441, ".9999999994"),(5442, "-.0000000004"),(5443, "-.0000000014"),
      (5444, "-.0000000094"),(5445, "-.0999999994"),(5446, "-.9000000004"),(5447, "-.9000000014"),(5448, "-.9999999984"),(5449, "-.9999999994"),(5450, "-.0000000005"),(5451, "-.0000000015"),(5452, "-.0000000095"),(5453, "-.0999999995"),
      (5454, "-.9000000005"),(5455, "-.9000000015"),(5456, "-.9999999985"),(5457, "-.9999999994");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_str_32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(9, 9)) from test_cast_to_decimal32_9_9_from_str_32_9_9 order by 1;'

}