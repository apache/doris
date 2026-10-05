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


suite("test_cast_to_bigint_from_string") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_bigint_from_string_0_nullable;"
    sql "create table test_cast_to_bigint_from_string_0_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_string_0_nullable values (0, "+0000"),(1, "+0001"),(2, "+0009"),(3, "+000123"),(4, "+0009223372036854775807"),(5, "-0001"),(6, "-0009"),(7, "-000123"),(8, "-0009223372036854775808"),(9, "+0009223372036854775806"),(10, "-0009223372036854775807"),(11, "+0"),(12, "+1"),(13, "+9"),(14, "+123"),(15, "+9223372036854775807"),(16, "-1"),(17, "-9"),(18, "-123"),(19, "-9223372036854775808"),
      (20, "+9223372036854775806"),(21, "-9223372036854775807"),(22, "0000"),(23, "0001"),(24, "0009"),(25, "000123"),(26, "0009223372036854775807"),(27, "-0001"),(28, "-0009"),(29, "-000123"),(30, "-0009223372036854775808"),(31, "0009223372036854775806"),(32, "-0009223372036854775807"),(33, "0"),(34, "1"),(35, "9"),(36, "123"),(37, "9223372036854775807"),(38, "-1"),(39, "-9"),
      (40, "-123"),(41, "-9223372036854775808"),(42, "9223372036854775806"),(43, "-9223372036854775807")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_string_0_not_nullable;"
    sql "create table test_cast_to_bigint_from_string_0_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_string_0_not_nullable values (0, "+0000"),(1, "+0001"),(2, "+0009"),(3, "+000123"),(4, "+0009223372036854775807"),(5, "-0001"),(6, "-0009"),(7, "-000123"),(8, "-0009223372036854775808"),(9, "+0009223372036854775806"),(10, "-0009223372036854775807"),(11, "+0"),(12, "+1"),(13, "+9"),(14, "+123"),(15, "+9223372036854775807"),(16, "-1"),(17, "-9"),(18, "-123"),(19, "-9223372036854775808"),
      (20, "+9223372036854775806"),(21, "-9223372036854775807"),(22, "0000"),(23, "0001"),(24, "0009"),(25, "000123"),(26, "0009223372036854775807"),(27, "-0001"),(28, "-0009"),(29, "-000123"),(30, "-0009223372036854775808"),(31, "0009223372036854775806"),(32, "-0009223372036854775807"),(33, "0"),(34, "1"),(35, "9"),(36, "123"),(37, "9223372036854775807"),(38, "-1"),(39, "-9"),
      (40, "-123"),(41, "-9223372036854775808"),(42, "9223372036854775806"),(43, "-9223372036854775807");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_string_0_not_nullable order by 1;'

}