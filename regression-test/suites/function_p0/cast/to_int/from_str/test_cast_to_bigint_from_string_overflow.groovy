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


suite("test_cast_to_bigint_from_string_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_bigint_from_string_overflow_0;"
    sql "create table test_cast_to_bigint_from_string_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_string_overflow_0 values (0, "9223372036854775808"),(1, "-9223372036854775809"),(2, "9223372036854775809"),(3, "-9223372036854775810"),(4, "9223372036854775810"),(5, "-9223372036854775811"),(6, "9223372036854775811"),(7, "-9223372036854775812"),(8, "9223372036854775812"),(9, "-9223372036854775813"),(10, "9223372036854775813"),(11, "-9223372036854775814"),(12, "9223372036854775814"),(13, "-9223372036854775815"),(14, "9223372036854775815"),(15, "-9223372036854775816"),(16, "9223372036854775816"),(17, "-9223372036854775817"),(18, "9223372036854775817"),(19, "-9223372036854775818"),
      (20, "18446744073709551615"),(21, "-18446744073709551615"),(22, "18446744073709551614"),(23, "-18446744073709551614"),(24, "18446744073709551613"),(25, "-18446744073709551613"),(26, "18446744073709551612"),(27, "-18446744073709551612"),(28, "18446744073709551611"),(29, "-18446744073709551611"),(30, "18446744073709551610"),(31, "-18446744073709551610"),(32, "18446744073709551609"),(33, "-18446744073709551609"),(34, "18446744073709551608"),(35, "-18446744073709551608"),(36, "18446744073709551607"),(37, "-18446744073709551607"),(38, "18446744073709551606"),(39, "-18446744073709551606"),
      (40, "170141183460469231731687303715884105727"),(41, "-170141183460469231731687303715884105728"),(42, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(43, "-57896044618658097711785492504343953926634992332820282019728792003956564819968");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_bigint_from_string_overflow_0_data_start_index = 0
    def test_cast_to_bigint_from_string_overflow_0_data_end_index = 44
    for (int data_index = test_cast_to_bigint_from_string_overflow_0_data_start_index; data_index < test_cast_to_bigint_from_string_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_overflow_0 order by 1;'

}