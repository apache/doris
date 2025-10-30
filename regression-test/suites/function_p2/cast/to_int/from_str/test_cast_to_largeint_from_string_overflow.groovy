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


suite("test_cast_to_largeint_from_string_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_largeint_from_string_overflow_0;"
    sql "create table test_cast_to_largeint_from_string_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_largeint_from_string_overflow_0 values (0, "170141183460469231731687303715884105728"),(1, "-170141183460469231731687303715884105729"),(2, "170141183460469231731687303715884105729"),(3, "-170141183460469231731687303715884105730"),(4, "170141183460469231731687303715884105730"),(5, "-170141183460469231731687303715884105731"),(6, "170141183460469231731687303715884105731"),(7, "-170141183460469231731687303715884105732"),(8, "170141183460469231731687303715884105732"),(9, "-170141183460469231731687303715884105733"),(10, "170141183460469231731687303715884105733"),(11, "-170141183460469231731687303715884105734"),(12, "170141183460469231731687303715884105734"),(13, "-170141183460469231731687303715884105735"),(14, "170141183460469231731687303715884105735"),(15, "-170141183460469231731687303715884105736"),(16, "170141183460469231731687303715884105736"),(17, "-170141183460469231731687303715884105737"),(18, "170141183460469231731687303715884105737"),(19, "-170141183460469231731687303715884105738"),
      (20, "340282366920938463463374607431768211455"),(21, "-340282366920938463463374607431768211455"),(22, "340282366920938463463374607431768211454"),(23, "-340282366920938463463374607431768211454"),(24, "340282366920938463463374607431768211453"),(25, "-340282366920938463463374607431768211453"),(26, "340282366920938463463374607431768211452"),(27, "-340282366920938463463374607431768211452"),(28, "340282366920938463463374607431768211451"),(29, "-340282366920938463463374607431768211451"),(30, "340282366920938463463374607431768211450"),(31, "-340282366920938463463374607431768211450"),(32, "340282366920938463463374607431768211449"),(33, "-340282366920938463463374607431768211449"),(34, "340282366920938463463374607431768211448"),(35, "-340282366920938463463374607431768211448"),(36, "340282366920938463463374607431768211447"),(37, "-340282366920938463463374607431768211447"),(38, "340282366920938463463374607431768211446"),(39, "-340282366920938463463374607431768211446"),
      (40, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(41, "-57896044618658097711785492504343953926634992332820282019728792003956564819968");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_largeint_from_string_overflow_0_data_start_index = 0
    def test_cast_to_largeint_from_string_overflow_0_data_end_index = 42
    for (int data_index = test_cast_to_largeint_from_string_overflow_0_data_start_index; data_index < test_cast_to_largeint_from_string_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as largeint) from test_cast_to_largeint_from_string_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as largeint) from test_cast_to_largeint_from_string_overflow_0 order by 1;'

}