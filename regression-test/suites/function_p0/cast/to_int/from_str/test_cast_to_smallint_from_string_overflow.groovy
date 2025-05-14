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


suite("test_cast_to_smallint_from_string_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_smallint_from_string_overflow_0;"
    sql "create table test_cast_to_smallint_from_string_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_string_overflow_0 values (0, "32768"),(1, "-32769"),(2, "32769"),(3, "-32770"),(4, "32770"),(5, "-32771"),(6, "32771"),(7, "-32772"),(8, "32772"),(9, "-32773"),(10, "32773"),(11, "-32774"),(12, "32774"),(13, "-32775"),(14, "32775"),(15, "-32776"),(16, "32776"),(17, "-32777"),(18, "32777"),(19, "-32778"),
      (20, "65535"),(21, "-65535"),(22, "65534"),(23, "-65534"),(24, "65533"),(25, "-65533"),(26, "65532"),(27, "-65532"),(28, "65531"),(29, "-65531"),(30, "65530"),(31, "-65530"),(32, "65529"),(33, "-65529"),(34, "65528"),(35, "-65528"),(36, "65527"),(37, "-65527"),(38, "65526"),(39, "-65526"),
      (40, "2147483647"),(41, "-2147483648"),(42, "9223372036854775807"),(43, "-9223372036854775808"),(44, "170141183460469231731687303715884105727"),(45, "-170141183460469231731687303715884105728"),(46, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(47, "-57896044618658097711785492504343953926634992332820282019728792003956564819968");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_smallint_from_string_overflow_0_data_start_index = 0
    def test_cast_to_smallint_from_string_overflow_0_data_end_index = 48
    for (int data_index = test_cast_to_smallint_from_string_overflow_0_data_start_index; data_index < test_cast_to_smallint_from_string_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_overflow_0 order by 1;'

}