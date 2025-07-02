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


suite("test_cast_to_decimal_9_3_from_str_invalid_input_0") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal_9_3_from_str_invalid_input_0_0;"
    sql "create table test_cast_to_decimal_9_3_from_str_invalid_input_0_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal_9_3_from_str_invalid_input_0_0 values (0, ""),(1, "."),(2, " "),(3, "	"),(4, "abc"),(5, "1 23"),(6, "1	23"),(7, "1.2.3"),(8, "a123.456"),(9, " a123.456"),(10, "	a123.456"),(11, "123.456a"),(12, "123.456a	"),(13, "123.456	a"),(14, "123.456
a"),(15, "12a3.456"),(16, "123a.456"),(17, "123.a456"),(18, "123.4a56"),(19, "+-123.456"),
      (20, "+- 123.456"),(21, "-+123.456"),(22, "++123.456"),(23, "--123.456"),(24, "+-.456"),(25, "-+.456"),(26, "++.456"),(27, "--.456"),(28, "0x123"),(29, "0x123.456"),(30, "e"),(31, "-e"),(32, "+e"),(33, "e+"),(34, "e-"),(35, "e1"),(36, "e+1"),(37, "e-1"),(38, ".e"),(39, "+.e"),
      (40, "-.e"),(41, ".e+"),(42, ".e-"),(43, ".e+"),(44, "1e"),(45, "1e+"),(46, "1e-"),(47, "1e1a"),(48, "1ea1"),(49, "1e1.1"),(50, "1e+1.1"),(51, "1e-1.1"),(52, "1e2e3");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_decimal_9_3_from_str_invalid_input_0_0_data_start_index = 0
    def test_cast_to_decimal_9_3_from_str_invalid_input_0_0_data_end_index = 53
    for (int data_index = test_cast_to_decimal_9_3_from_str_invalid_input_0_0_data_start_index; data_index < test_cast_to_decimal_9_3_from_str_invalid_input_0_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as decimalv3(9, 3)) from test_cast_to_decimal_9_3_from_str_invalid_input_0_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 3)) from test_cast_to_decimal_9_3_from_str_invalid_input_0_0 order by 1;'

}