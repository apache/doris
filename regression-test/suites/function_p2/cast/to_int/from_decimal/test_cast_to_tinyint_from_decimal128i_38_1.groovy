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


suite("test_cast_to_tinyint_from_decimal128i_38_1") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal128i_38_1_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal128i_38_1_0_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal128i_38_1_0_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "127.0"),(25, "127.1"),(26, "127.9"),(27, "127.9"),(28, "127.8"),(29, "127.0"),(30, "127.9"),(31, "127.8"),(32, "126.0"),(33, "126.1"),(34, "126.9"),(35, "126.9"),(36, "126.8"),(37, "126.0"),(38, "126.9"),(39, "126.8"),
      (40, "-128.0"),(41, "-128.1"),(42, "-128.9"),(43, "-128.9"),(44, "-128.8"),(45, "-128.0"),(46, "-128.9"),(47, "-128.8"),(48, "-127.0"),(49, "-127.1"),(50, "-127.9"),(51, "-127.9"),(52, "-127.8"),(53, "-127.0"),(54, "-127.9"),(55, "-127.8")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal128i_38_1_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal128i_38_1_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal128i_38_1_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal128i_38_1_0_not_nullable(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal128i_38_1_0_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "127.0"),(25, "127.1"),(26, "127.9"),(27, "127.9"),(28, "127.8"),(29, "127.0"),(30, "127.9"),(31, "127.8"),(32, "126.0"),(33, "126.1"),(34, "126.9"),(35, "126.9"),(36, "126.8"),(37, "126.0"),(38, "126.9"),(39, "126.8"),
      (40, "-128.0"),(41, "-128.1"),(42, "-128.9"),(43, "-128.9"),(44, "-128.8"),(45, "-128.0"),(46, "-128.9"),(47, "-128.8"),(48, "-127.0"),(49, "-127.1"),(50, "-127.9"),(51, "-127.9"),(52, "-127.8"),(53, "-127.0"),(54, "-127.9"),(55, "-127.8");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal128i_38_1_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal128i_38_1_0_not_nullable order by 1;'

}