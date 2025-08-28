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


suite("test_cast_to_tinyint_from_string_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_string_overflow_0;"
    sql "create table test_cast_to_tinyint_from_string_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_string_overflow_0 values (0, "128"),(1, "-129"),(2, "129"),(3, "-130"),(4, "130"),(5, "-131"),(6, "131"),(7, "-132"),(8, "132"),(9, "-133"),(10, "133"),(11, "-134"),(12, "134"),(13, "-135"),(14, "135"),(15, "-136"),(16, "136"),(17, "-137"),(18, "137"),(19, "-138"),
      (20, "255"),(21, "-255"),(22, "254"),(23, "-254"),(24, "253"),(25, "-253"),(26, "252"),(27, "-252"),(28, "251"),(29, "-251"),(30, "250"),(31, "-250"),(32, "249"),(33, "-249"),(34, "248"),(35, "-248"),(36, "247"),(37, "-247"),(38, "246"),(39, "-246"),
      (40, "32767"),(41, "-32768"),(42, "2147483647"),(43, "-2147483648"),(44, "9223372036854775807"),(45, "-9223372036854775808"),(46, "170141183460469231731687303715884105727"),(47, "-170141183460469231731687303715884105728"),(48, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(49, "-57896044618658097711785492504343953926634992332820282019728792003956564819968");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_string_overflow_0_data_start_index = 0
    def test_cast_to_tinyint_from_string_overflow_0_data_end_index = 50
    for (int data_index = test_cast_to_tinyint_from_string_overflow_0_data_start_index; data_index < test_cast_to_tinyint_from_string_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_string_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_string_overflow_0 order by 1;'

}