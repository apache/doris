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


suite("test_cast_to_tinyint_from_decimal256_39_19") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_tinyint_from_decimal256_39_19_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal256_39_19_0_nullable(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal256_39_19_0_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.9999999999999999999"),(4, "0.9999999999999999998"),(5, "0.0999999999999999999"),(6, "0.9000000000000000000"),(7, "0.9000000000000000001"),(8, "1.0000000000000000000"),(9, "1.0000000000000000001"),(10, "1.0000000000000000009"),(11, "1.9999999999999999999"),(12, "1.9999999999999999998"),(13, "1.0999999999999999999"),(14, "1.9000000000000000000"),(15, "1.9000000000000000001"),(16, "9.0000000000000000000"),(17, "9.0000000000000000001"),(18, "9.0000000000000000009"),(19, "9.9999999999999999999"),
      (20, "9.9999999999999999998"),(21, "9.0999999999999999999"),(22, "9.9000000000000000000"),(23, "9.9000000000000000001"),(24, "127.0000000000000000000"),(25, "127.0000000000000000001"),(26, "127.0000000000000000009"),(27, "127.9999999999999999999"),(28, "127.9999999999999999998"),(29, "127.0999999999999999999"),(30, "127.9000000000000000000"),(31, "127.9000000000000000001"),(32, "126.0000000000000000000"),(33, "126.0000000000000000001"),(34, "126.0000000000000000009"),(35, "126.9999999999999999999"),(36, "126.9999999999999999998"),(37, "126.0999999999999999999"),(38, "126.9000000000000000000"),(39, "126.9000000000000000001"),
      (40, "-128.0000000000000000000"),(41, "-128.0000000000000000001"),(42, "-128.0000000000000000009"),(43, "-128.9999999999999999999"),(44, "-128.9999999999999999998"),(45, "-128.0999999999999999999"),(46, "-128.9000000000000000000"),(47, "-128.9000000000000000001"),(48, "-127.0000000000000000000"),(49, "-127.0000000000000000001"),(50, "-127.0000000000000000009"),(51, "-127.9999999999999999999"),(52, "-127.9999999999999999998"),(53, "-127.0999999999999999999"),(54, "-127.9000000000000000000"),(55, "-127.9000000000000000001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal256_39_19_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal256_39_19_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal256_39_19_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal256_39_19_0_not_nullable(f1 int, f2 decimalv3(39, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal256_39_19_0_not_nullable values (0, "0.0000000000000000000"),(1, "0.0000000000000000001"),(2, "0.0000000000000000009"),(3, "0.9999999999999999999"),(4, "0.9999999999999999998"),(5, "0.0999999999999999999"),(6, "0.9000000000000000000"),(7, "0.9000000000000000001"),(8, "1.0000000000000000000"),(9, "1.0000000000000000001"),(10, "1.0000000000000000009"),(11, "1.9999999999999999999"),(12, "1.9999999999999999998"),(13, "1.0999999999999999999"),(14, "1.9000000000000000000"),(15, "1.9000000000000000001"),(16, "9.0000000000000000000"),(17, "9.0000000000000000001"),(18, "9.0000000000000000009"),(19, "9.9999999999999999999"),
      (20, "9.9999999999999999998"),(21, "9.0999999999999999999"),(22, "9.9000000000000000000"),(23, "9.9000000000000000001"),(24, "127.0000000000000000000"),(25, "127.0000000000000000001"),(26, "127.0000000000000000009"),(27, "127.9999999999999999999"),(28, "127.9999999999999999998"),(29, "127.0999999999999999999"),(30, "127.9000000000000000000"),(31, "127.9000000000000000001"),(32, "126.0000000000000000000"),(33, "126.0000000000000000001"),(34, "126.0000000000000000009"),(35, "126.9999999999999999999"),(36, "126.9999999999999999998"),(37, "126.0999999999999999999"),(38, "126.9000000000000000000"),(39, "126.9000000000000000001"),
      (40, "-128.0000000000000000000"),(41, "-128.0000000000000000001"),(42, "-128.0000000000000000009"),(43, "-128.9999999999999999999"),(44, "-128.9999999999999999998"),(45, "-128.0999999999999999999"),(46, "-128.9000000000000000000"),(47, "-128.9000000000000000001"),(48, "-127.0000000000000000000"),(49, "-127.0000000000000000001"),(50, "-127.0000000000000000009"),(51, "-127.9999999999999999999"),(52, "-127.9999999999999999998"),(53, "-127.0999999999999999999"),(54, "-127.9000000000000000000"),(55, "-127.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal256_39_19_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal256_39_19_0_not_nullable order by 1;'

}