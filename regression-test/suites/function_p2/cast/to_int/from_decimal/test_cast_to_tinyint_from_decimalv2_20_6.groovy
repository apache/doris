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


suite("test_cast_to_tinyint_from_decimalv2_20_6") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimalv2_20_6_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimalv2_20_6_0_nullable(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimalv2_20_6_0_nullable values (0, "0.000000"),(1, "0.000000"),(2, "0.000000"),(3, "0.001000"),(4, "0.001000"),(5, "0.000100"),(6, "0.000900"),(7, "0.000900"),(8, "1.000000"),(9, "1.000000"),(10, "1.000000"),(11, "1.001000"),(12, "1.001000"),(13, "1.000100"),(14, "1.000900"),(15, "1.000900"),(16, "9.000000"),(17, "9.000000"),(18, "9.000000"),(19, "9.001000"),
      (20, "9.001000"),(21, "9.000100"),(22, "9.000900"),(23, "9.000900"),(24, "127.000000"),(25, "127.000000"),(26, "127.000000"),(27, "127.001000"),(28, "127.001000"),(29, "127.000100"),(30, "127.000900"),(31, "127.000900"),(32, "126.000000"),(33, "126.000000"),(34, "126.000000"),(35, "126.001000"),(36, "126.001000"),(37, "126.000100"),(38, "126.000900"),(39, "126.000900"),
      (40, "-128.000000"),(41, "-128.000000"),(42, "-128.000000"),(43, "-128.001000"),(44, "-128.001000"),(45, "-128.000100"),(46, "-128.000900"),(47, "-128.000900"),(48, "-127.000000"),(49, "-127.000000"),(50, "-127.000000"),(51, "-127.001000"),(52, "-127.001000"),(53, "-127.000100"),(54, "-127.000900"),(55, "-127.000900")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimalv2_20_6_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimalv2_20_6_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimalv2_20_6_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimalv2_20_6_0_not_nullable(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimalv2_20_6_0_not_nullable values (0, "0.000000"),(1, "0.000000"),(2, "0.000000"),(3, "0.001000"),(4, "0.001000"),(5, "0.000100"),(6, "0.000900"),(7, "0.000900"),(8, "1.000000"),(9, "1.000000"),(10, "1.000000"),(11, "1.001000"),(12, "1.001000"),(13, "1.000100"),(14, "1.000900"),(15, "1.000900"),(16, "9.000000"),(17, "9.000000"),(18, "9.000000"),(19, "9.001000"),
      (20, "9.001000"),(21, "9.000100"),(22, "9.000900"),(23, "9.000900"),(24, "127.000000"),(25, "127.000000"),(26, "127.000000"),(27, "127.001000"),(28, "127.001000"),(29, "127.000100"),(30, "127.000900"),(31, "127.000900"),(32, "126.000000"),(33, "126.000000"),(34, "126.000000"),(35, "126.001000"),(36, "126.001000"),(37, "126.000100"),(38, "126.000900"),(39, "126.000900"),
      (40, "-128.000000"),(41, "-128.000000"),(42, "-128.000000"),(43, "-128.001000"),(44, "-128.001000"),(45, "-128.000100"),(46, "-128.000900"),(47, "-128.000900"),(48, "-127.000000"),(49, "-127.000000"),(50, "-127.000000"),(51, "-127.001000"),(52, "-127.001000"),(53, "-127.000100"),(54, "-127.000900"),(55, "-127.000900");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimalv2_20_6_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimalv2_20_6_0_not_nullable order by 1;'

}