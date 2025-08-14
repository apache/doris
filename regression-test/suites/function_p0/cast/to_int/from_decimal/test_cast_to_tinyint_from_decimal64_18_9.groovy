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


suite("test_cast_to_tinyint_from_decimal64_18_9") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal64_18_9_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal64_18_9_0_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_18_9_0_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "127.000000000"),(25, "127.000000001"),(26, "127.000000009"),(27, "127.999999999"),(28, "127.999999998"),(29, "127.099999999"),(30, "127.900000000"),(31, "127.900000001"),(32, "126.000000000"),(33, "126.000000001"),(34, "126.000000009"),(35, "126.999999999"),(36, "126.999999998"),(37, "126.099999999"),(38, "126.900000000"),(39, "126.900000001"),
      (40, "-128.000000000"),(41, "-128.000000001"),(42, "-128.000000009"),(43, "-128.999999999"),(44, "-128.999999998"),(45, "-128.099999999"),(46, "-128.900000000"),(47, "-128.900000001"),(48, "-127.000000000"),(49, "-127.000000001"),(50, "-127.000000009"),(51, "-127.999999999"),(52, "-127.999999998"),(53, "-127.099999999"),(54, "-127.900000000"),(55, "-127.900000001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_18_9_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_18_9_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_18_9_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal64_18_9_0_not_nullable(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_18_9_0_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "127.000000000"),(25, "127.000000001"),(26, "127.000000009"),(27, "127.999999999"),(28, "127.999999998"),(29, "127.099999999"),(30, "127.900000000"),(31, "127.900000001"),(32, "126.000000000"),(33, "126.000000001"),(34, "126.000000009"),(35, "126.999999999"),(36, "126.999999998"),(37, "126.099999999"),(38, "126.900000000"),(39, "126.900000001"),
      (40, "-128.000000000"),(41, "-128.000000001"),(42, "-128.000000009"),(43, "-128.999999999"),(44, "-128.999999998"),(45, "-128.099999999"),(46, "-128.900000000"),(47, "-128.900000001"),(48, "-127.000000000"),(49, "-127.000000001"),(50, "-127.000000009"),(51, "-127.999999999"),(52, "-127.999999998"),(53, "-127.099999999"),(54, "-127.900000000"),(55, "-127.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_18_9_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_18_9_0_not_nullable order by 1;'

}