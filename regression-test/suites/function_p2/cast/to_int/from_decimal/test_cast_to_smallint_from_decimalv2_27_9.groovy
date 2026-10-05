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


suite("test_cast_to_smallint_from_decimalv2_27_9") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_smallint_from_decimalv2_27_9_0_nullable;"
    sql "create table test_cast_to_smallint_from_decimalv2_27_9_0_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimalv2_27_9_0_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "32767.000000000"),(25, "32767.000000001"),(26, "32767.000000009"),(27, "32767.999999999"),(28, "32767.999999998"),(29, "32767.099999999"),(30, "32767.900000000"),(31, "32767.900000001"),(32, "32766.000000000"),(33, "32766.000000001"),(34, "32766.000000009"),(35, "32766.999999999"),(36, "32766.999999998"),(37, "32766.099999999"),(38, "32766.900000000"),(39, "32766.900000001"),
      (40, "-32768.000000000"),(41, "-32768.000000001"),(42, "-32768.000000009"),(43, "-32768.999999999"),(44, "-32768.999999998"),(45, "-32768.099999999"),(46, "-32768.900000000"),(47, "-32768.900000001"),(48, "-32767.000000000"),(49, "-32767.000000001"),(50, "-32767.000000009"),(51, "-32767.999999999"),(52, "-32767.999999998"),(53, "-32767.099999999"),(54, "-32767.900000000"),(55, "-32767.900000001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimalv2_27_9_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimalv2_27_9_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_smallint_from_decimalv2_27_9_0_not_nullable;"
    sql "create table test_cast_to_smallint_from_decimalv2_27_9_0_not_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_decimalv2_27_9_0_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "32767.000000000"),(25, "32767.000000001"),(26, "32767.000000009"),(27, "32767.999999999"),(28, "32767.999999998"),(29, "32767.099999999"),(30, "32767.900000000"),(31, "32767.900000001"),(32, "32766.000000000"),(33, "32766.000000001"),(34, "32766.000000009"),(35, "32766.999999999"),(36, "32766.999999998"),(37, "32766.099999999"),(38, "32766.900000000"),(39, "32766.900000001"),
      (40, "-32768.000000000"),(41, "-32768.000000001"),(42, "-32768.000000009"),(43, "-32768.999999999"),(44, "-32768.999999998"),(45, "-32768.099999999"),(46, "-32768.900000000"),(47, "-32768.900000001"),(48, "-32767.000000000"),(49, "-32767.000000001"),(50, "-32767.000000009"),(51, "-32767.999999999"),(52, "-32767.999999998"),(53, "-32767.099999999"),(54, "-32767.900000000"),(55, "-32767.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimalv2_27_9_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_decimalv2_27_9_0_not_nullable order by 1;'

}