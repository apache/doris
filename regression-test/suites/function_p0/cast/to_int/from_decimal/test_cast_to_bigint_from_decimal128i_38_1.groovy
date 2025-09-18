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


suite("test_cast_to_bigint_from_decimal128i_38_1") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_bigint_from_decimal128i_38_1_0_nullable;"
    sql "create table test_cast_to_bigint_from_decimal128i_38_1_0_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_38_1_0_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "9223372036854775807.0"),(25, "9223372036854775807.1"),(26, "9223372036854775807.9"),(27, "9223372036854775807.9"),(28, "9223372036854775807.8"),(29, "9223372036854775807.0"),(30, "9223372036854775807.9"),(31, "9223372036854775807.8"),(32, "9223372036854775806.0"),(33, "9223372036854775806.1"),(34, "9223372036854775806.9"),(35, "9223372036854775806.9"),(36, "9223372036854775806.8"),(37, "9223372036854775806.0"),(38, "9223372036854775806.9"),(39, "9223372036854775806.8"),
      (40, "-9223372036854775808.0"),(41, "-9223372036854775808.1"),(42, "-9223372036854775808.9"),(43, "-9223372036854775808.9"),(44, "-9223372036854775808.8"),(45, "-9223372036854775808.0"),(46, "-9223372036854775808.9"),(47, "-9223372036854775808.8"),(48, "-9223372036854775807.0"),(49, "-9223372036854775807.1"),(50, "-9223372036854775807.9"),(51, "-9223372036854775807.9"),(52, "-9223372036854775807.8"),(53, "-9223372036854775807.0"),(54, "-9223372036854775807.9"),(55, "-9223372036854775807.8")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_38_1_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_38_1_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_decimal128i_38_1_0_not_nullable;"
    sql "create table test_cast_to_bigint_from_decimal128i_38_1_0_not_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_decimal128i_38_1_0_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "9223372036854775807.0"),(25, "9223372036854775807.1"),(26, "9223372036854775807.9"),(27, "9223372036854775807.9"),(28, "9223372036854775807.8"),(29, "9223372036854775807.0"),(30, "9223372036854775807.9"),(31, "9223372036854775807.8"),(32, "9223372036854775806.0"),(33, "9223372036854775806.1"),(34, "9223372036854775806.9"),(35, "9223372036854775806.9"),(36, "9223372036854775806.8"),(37, "9223372036854775806.0"),(38, "9223372036854775806.9"),(39, "9223372036854775806.8"),
      (40, "-9223372036854775808.0"),(41, "-9223372036854775808.1"),(42, "-9223372036854775808.9"),(43, "-9223372036854775808.9"),(44, "-9223372036854775808.8"),(45, "-9223372036854775808.0"),(46, "-9223372036854775808.9"),(47, "-9223372036854775808.8"),(48, "-9223372036854775807.0"),(49, "-9223372036854775807.1"),(50, "-9223372036854775807.9"),(51, "-9223372036854775807.9"),(52, "-9223372036854775807.8"),(53, "-9223372036854775807.0"),(54, "-9223372036854775807.9"),(55, "-9223372036854775807.8");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_38_1_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_decimal128i_38_1_0_not_nullable order by 1;'

}