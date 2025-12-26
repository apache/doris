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


suite("test_cast_to_tinyint_from_decimal64_10_5") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_decimal64_10_5_0_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal64_10_5_0_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_10_5_0_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.99999"),(4, "0.99998"),(5, "0.09999"),(6, "0.90000"),(7, "0.90001"),(8, "1.00000"),(9, "1.00001"),(10, "1.00009"),(11, "1.99999"),(12, "1.99998"),(13, "1.09999"),(14, "1.90000"),(15, "1.90001"),(16, "9.00000"),(17, "9.00001"),(18, "9.00009"),(19, "9.99999"),
      (20, "9.99998"),(21, "9.09999"),(22, "9.90000"),(23, "9.90001"),(24, "127.00000"),(25, "127.00001"),(26, "127.00009"),(27, "127.99999"),(28, "127.99998"),(29, "127.09999"),(30, "127.90000"),(31, "127.90001"),(32, "126.00000"),(33, "126.00001"),(34, "126.00009"),(35, "126.99999"),(36, "126.99998"),(37, "126.09999"),(38, "126.90000"),(39, "126.90001"),
      (40, "-128.00000"),(41, "-128.00001"),(42, "-128.00009"),(43, "-128.99999"),(44, "-128.99998"),(45, "-128.09999"),(46, "-128.90000"),(47, "-128.90001"),(48, "-127.00000"),(49, "-127.00001"),(50, "-127.00009"),(51, "-127.99999"),(52, "-127.99998"),(53, "-127.09999"),(54, "-127.90000"),(55, "-127.90001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_10_5_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_10_5_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_tinyint_from_decimal64_10_5_0_not_nullable;"
    sql "create table test_cast_to_tinyint_from_decimal64_10_5_0_not_nullable(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_decimal64_10_5_0_not_nullable values (0, "0.00000"),(1, "0.00001"),(2, "0.00009"),(3, "0.99999"),(4, "0.99998"),(5, "0.09999"),(6, "0.90000"),(7, "0.90001"),(8, "1.00000"),(9, "1.00001"),(10, "1.00009"),(11, "1.99999"),(12, "1.99998"),(13, "1.09999"),(14, "1.90000"),(15, "1.90001"),(16, "9.00000"),(17, "9.00001"),(18, "9.00009"),(19, "9.99999"),
      (20, "9.99998"),(21, "9.09999"),(22, "9.90000"),(23, "9.90001"),(24, "127.00000"),(25, "127.00001"),(26, "127.00009"),(27, "127.99999"),(28, "127.99998"),(29, "127.09999"),(30, "127.90000"),(31, "127.90001"),(32, "126.00000"),(33, "126.00001"),(34, "126.00009"),(35, "126.99999"),(36, "126.99998"),(37, "126.09999"),(38, "126.90000"),(39, "126.90001"),
      (40, "-128.00000"),(41, "-128.00001"),(42, "-128.00009"),(43, "-128.99999"),(44, "-128.99998"),(45, "-128.09999"),(46, "-128.90000"),(47, "-128.90001"),(48, "-127.00000"),(49, "-127.00001"),(50, "-127.00009"),(51, "-127.99999"),(52, "-127.99998"),(53, "-127.09999"),(54, "-127.90000"),(55, "-127.90001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_10_5_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_decimal64_10_5_0_not_nullable order by 1;'

}