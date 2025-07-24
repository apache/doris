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


suite("test_cast_to_smallint_from_decimal32_9_4") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_smallint_from_decimal32_9_4_0_nullable;"
    sql "create table test_cast_to_smallint_from_decimal32_9_4_0_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimal32_9_4_0_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.9999"),(4, "0.9998"),(5, "0.0999"),(6, "0.9000"),(7, "0.9001"),(8, "1.0000"),(9, "1.0001"),(10, "1.0009"),(11, "1.9999"),(12, "1.9998"),(13, "1.0999"),(14, "1.9000"),(15, "1.9001"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.9999"),
      (20, "9.9998"),(21, "9.0999"),(22, "9.9000"),(23, "9.9001"),(24, "32767.0000"),(25, "32767.0001"),(26, "32767.0009"),(27, "32767.9999"),(28, "32767.9998"),(29, "32767.0999"),(30, "32767.9000"),(31, "32767.9001"),(32, "32766.0000"),(33, "32766.0001"),(34, "32766.0009"),(35, "32766.9999"),(36, "32766.9998"),(37, "32766.0999"),(38, "32766.9000"),(39, "32766.9001"),
      (40, "-32768.0000"),(41, "-32768.0001"),(42, "-32768.0009"),(43, "-32768.9999"),(44, "-32768.9998"),(45, "-32768.0999"),(46, "-32768.9000"),(47, "-32768.9001"),(48, "-32767.0000"),(49, "-32767.0001"),(50, "-32767.0009"),(51, "-32767.9999"),(52, "-32767.9998"),(53, "-32767.0999"),(54, "-32767.9000"),(55, "-32767.9001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_4_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_4_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_smallint_from_decimal32_9_4_0_not_nullable;"
    sql "create table test_cast_to_smallint_from_decimal32_9_4_0_not_nullable(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimal32_9_4_0_not_nullable values (0, "0.0000"),(1, "0.0001"),(2, "0.0009"),(3, "0.9999"),(4, "0.9998"),(5, "0.0999"),(6, "0.9000"),(7, "0.9001"),(8, "1.0000"),(9, "1.0001"),(10, "1.0009"),(11, "1.9999"),(12, "1.9998"),(13, "1.0999"),(14, "1.9000"),(15, "1.9001"),(16, "9.0000"),(17, "9.0001"),(18, "9.0009"),(19, "9.9999"),
      (20, "9.9998"),(21, "9.0999"),(22, "9.9000"),(23, "9.9001"),(24, "32767.0000"),(25, "32767.0001"),(26, "32767.0009"),(27, "32767.9999"),(28, "32767.9998"),(29, "32767.0999"),(30, "32767.9000"),(31, "32767.9001"),(32, "32766.0000"),(33, "32766.0001"),(34, "32766.0009"),(35, "32766.9999"),(36, "32766.9998"),(37, "32766.0999"),(38, "32766.9000"),(39, "32766.9001"),
      (40, "-32768.0000"),(41, "-32768.0001"),(42, "-32768.0009"),(43, "-32768.9999"),(44, "-32768.9998"),(45, "-32768.0999"),(46, "-32768.9000"),(47, "-32768.9001"),(48, "-32767.0000"),(49, "-32767.0001"),(50, "-32767.0009"),(51, "-32767.9999"),(52, "-32767.9998"),(53, "-32767.0999"),(54, "-32767.9000"),(55, "-32767.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_4_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_4_0_not_nullable order by 1;'

}