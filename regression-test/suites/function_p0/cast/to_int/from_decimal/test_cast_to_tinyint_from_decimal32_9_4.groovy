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


suite("test_cast_to_tinyint_from_decimal32_9_4") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal32_9_4_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal32_9_4_0_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal32_9_4_0_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.9999"),(4, "0.9998"),(5, "0.0999"),(6, "0.9000"),(7, "0.9001"),(8, "1.0000"),(9, "1.0001"),(10, "1.0009"),(11, "1.9999"),(12, "1.9998"),(13, "1.0999"),(14, "1.9000"),(15, "1.9001"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.9999"),
      (20, "9.9998"),(21, "9.0999"),(22, "9.9000"),(23, "9.9001"),(24, "127.0000"),(25, "127.0001"),(26, "127.0009"),(27, "127.9999"),(28, "127.9998"),(29, "127.0999"),(30, "127.9000"),(31, "127.9001"),(32, "126.0000"),(33, "126.0001"),(34, "126.0009"),(35, "126.9999"),(36, "126.9998"),(37, "126.0999"),(38, "126.9000"),(39, "126.9001"),
      (40, "-128.0000"),(41, "-128.0001"),(42, "-128.0009"),(43, "-128.9999"),(44, "-128.9998"),(45, "-128.0999"),(46, "-128.9000"),(47, "-128.9001"),(48, "-127.0000"),(49, "-127.0001"),(50, "-127.0009"),(51, "-127.9999"),(52, "-127.9998"),(53, "-127.0999"),(54, "-127.9000"),(55, "-127.9001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_9_4_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_9_4_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal32_9_4_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal32_9_4_0_not_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal32_9_4_0_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.9999"),(4, "0.9998"),(5, "0.0999"),(6, "0.9000"),(7, "0.9001"),(8, "1.0000"),(9, "1.0001"),(10, "1.0009"),(11, "1.9999"),(12, "1.9998"),(13, "1.0999"),(14, "1.9000"),(15, "1.9001"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.9999"),
      (20, "9.9998"),(21, "9.0999"),(22, "9.9000"),(23, "9.9001"),(24, "127.0000"),(25, "127.0001"),(26, "127.0009"),(27, "127.9999"),(28, "127.9998"),(29, "127.0999"),(30, "127.9000"),(31, "127.9001"),(32, "126.0000"),(33, "126.0001"),(34, "126.0009"),(35, "126.9999"),(36, "126.9998"),(37, "126.0999"),(38, "126.9000"),(39, "126.9001"),
      (40, "-128.0000"),(41, "-128.0001"),(42, "-128.0009"),(43, "-128.9999"),(44, "-128.9998"),(45, "-128.0999"),(46, "-128.9000"),(47, "-128.9001"),(48, "-127.0000"),(49, "-127.0001"),(50, "-127.0009"),(51, "-127.9999"),(52, "-127.9998"),(53, "-127.0999"),(54, "-127.9000"),(55, "-127.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_9_4_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal32_9_4_0_not_nullable order by 1;'

}