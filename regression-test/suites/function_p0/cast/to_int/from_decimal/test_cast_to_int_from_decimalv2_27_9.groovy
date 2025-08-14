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


suite("test_cast_to_int_from_decimalv2_27_9") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_decimalv2_27_9_0_nullable;"
    sql "create table test_cast_to_int_from_decimalv2_27_9_0_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_27_9_0_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "2147483647.000000000"),(25, "2147483647.000000001"),(26, "2147483647.000000009"),(27, "2147483647.999999999"),(28, "2147483647.999999998"),(29, "2147483647.099999999"),(30, "2147483647.900000000"),(31, "2147483647.900000001"),(32, "2147483646.000000000"),(33, "2147483646.000000001"),(34, "2147483646.000000009"),(35, "2147483646.999999999"),(36, "2147483646.999999998"),(37, "2147483646.099999999"),(38, "2147483646.900000000"),(39, "2147483646.900000001"),
      (40, "-2147483648.000000000"),(41, "-2147483648.000000001"),(42, "-2147483648.000000009"),(43, "-2147483648.999999999"),(44, "-2147483648.999999998"),(45, "-2147483648.099999999"),(46, "-2147483648.900000000"),(47, "-2147483648.900000001"),(48, "-2147483647.000000000"),(49, "-2147483647.000000001"),(50, "-2147483647.000000009"),(51, "-2147483647.999999999"),(52, "-2147483647.999999998"),(53, "-2147483647.099999999"),(54, "-2147483647.900000000"),(55, "-2147483647.900000001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_27_9_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_27_9_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_int_from_decimalv2_27_9_0_not_nullable;"
    sql "create table test_cast_to_int_from_decimalv2_27_9_0_not_nullable(f1 int, f2 decimalv2(27, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_27_9_0_not_nullable values (0, "0.000000000"),(1, "0.000000001"),(2, "0.000000009"),(3, "0.999999999"),(4, "0.999999998"),(5, "0.099999999"),(6, "0.900000000"),(7, "0.900000001"),(8, "1.000000000"),(9, "1.000000001"),(10, "1.000000009"),(11, "1.999999999"),(12, "1.999999998"),(13, "1.099999999"),(14, "1.900000000"),(15, "1.900000001"),(16, "9.000000000"),(17, "9.000000001"),(18, "9.000000009"),(19, "9.999999999"),
      (20, "9.999999998"),(21, "9.099999999"),(22, "9.900000000"),(23, "9.900000001"),(24, "2147483647.000000000"),(25, "2147483647.000000001"),(26, "2147483647.000000009"),(27, "2147483647.999999999"),(28, "2147483647.999999998"),(29, "2147483647.099999999"),(30, "2147483647.900000000"),(31, "2147483647.900000001"),(32, "2147483646.000000000"),(33, "2147483646.000000001"),(34, "2147483646.000000009"),(35, "2147483646.999999999"),(36, "2147483646.999999998"),(37, "2147483646.099999999"),(38, "2147483646.900000000"),(39, "2147483646.900000001"),
      (40, "-2147483648.000000000"),(41, "-2147483648.000000001"),(42, "-2147483648.000000009"),(43, "-2147483648.999999999"),(44, "-2147483648.999999998"),(45, "-2147483648.099999999"),(46, "-2147483648.900000000"),(47, "-2147483648.900000001"),(48, "-2147483647.000000000"),(49, "-2147483647.000000001"),(50, "-2147483647.000000009"),(51, "-2147483647.999999999"),(52, "-2147483647.999999998"),(53, "-2147483647.099999999"),(54, "-2147483647.900000000"),(55, "-2147483647.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_27_9_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_27_9_0_not_nullable order by 1;'

}