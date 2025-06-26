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


suite("test_cast_to_smallint_from_decimal32_9_1") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_smallint_from_decimal32_9_1_0_nullable;"
    sql "create table test_cast_to_smallint_from_decimal32_9_1_0_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimal32_9_1_0_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "32767.0"),(25, "32767.1"),(26, "32767.9"),(27, "32767.9"),(28, "32767.8"),(29, "32767.0"),(30, "32767.9"),(31, "32767.8"),(32, "32766.0"),(33, "32766.1"),(34, "32766.9"),(35, "32766.9"),(36, "32766.8"),(37, "32766.0"),(38, "32766.9"),(39, "32766.8"),
      (40, "-32768.0"),(41, "-32768.1"),(42, "-32768.9"),(43, "-32768.9"),(44, "-32768.8"),(45, "-32768.0"),(46, "-32768.9"),(47, "-32768.8"),(48, "-32767.0"),(49, "-32767.1"),(50, "-32767.9"),(51, "-32767.9"),(52, "-32767.8"),(53, "-32767.0"),(54, "-32767.9"),(55, "-32767.8")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_1_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_1_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_smallint_from_decimal32_9_1_0_not_nullable;"
    sql "create table test_cast_to_smallint_from_decimal32_9_1_0_not_nullable(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimal32_9_1_0_not_nullable values (0, "0.0"),(1, "0.1"),(2, "0.9"),(3, "0.9"),(4, "0.8"),(5, "0.0"),(6, "0.9"),(7, "0.8"),(8, "1.0"),(9, "1.1"),(10, "1.9"),(11, "1.9"),(12, "1.8"),(13, "1.0"),(14, "1.9"),(15, "1.8"),(16, "9.0"),(17, "9.1"),(18, "9.9"),(19, "9.9"),
      (20, "9.8"),(21, "9.0"),(22, "9.9"),(23, "9.8"),(24, "32767.0"),(25, "32767.1"),(26, "32767.9"),(27, "32767.9"),(28, "32767.8"),(29, "32767.0"),(30, "32767.9"),(31, "32767.8"),(32, "32766.0"),(33, "32766.1"),(34, "32766.9"),(35, "32766.9"),(36, "32766.8"),(37, "32766.0"),(38, "32766.9"),(39, "32766.8"),
      (40, "-32768.0"),(41, "-32768.1"),(42, "-32768.9"),(43, "-32768.9"),(44, "-32768.8"),(45, "-32768.0"),(46, "-32768.9"),(47, "-32768.8"),(48, "-32767.0"),(49, "-32767.1"),(50, "-32767.9"),(51, "-32767.9"),(52, "-32767.8"),(53, "-32767.0"),(54, "-32767.9"),(55, "-32767.8");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_1_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimal32_9_1_0_not_nullable order by 1;'

}