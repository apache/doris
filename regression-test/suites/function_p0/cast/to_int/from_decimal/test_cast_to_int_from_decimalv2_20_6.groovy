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


suite("test_cast_to_int_from_decimalv2_20_6") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_decimalv2_20_6_0_nullable;"
    sql "create table test_cast_to_int_from_decimalv2_20_6_0_nullable(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_20_6_0_nullable values (0, "0.000000"),(1, "0.000001"),(2, "0.000009"),(3, "0.999999"),(4, "0.999998"),(5, "0.099999"),(6, "0.900000"),(7, "0.900001"),(8, "1000.000000"),(9, "1000.000001"),(10, "1000.000009"),(11, "1000.999999"),(12, "1000.999998"),(13, "1000.099999"),(14, "1000.900000"),(15, "1000.900001"),(16, "9000.000000"),(17, "9000.000001"),(18, "9000.000009"),(19, "9000.999999"),
      (20, "9000.999998"),(21, "9000.099999"),(22, "9000.900000"),(23, "9000.900001"),(24, "2147483647000.000000"),(25, "2147483647000.000001"),(26, "2147483647000.000009"),(27, "2147483647000.999999"),(28, "2147483647000.999998"),(29, "2147483647000.099999"),(30, "2147483647000.900000"),(31, "2147483647000.900001"),(32, "2147483646000.000000"),(33, "2147483646000.000001"),(34, "2147483646000.000009"),(35, "2147483646000.999999"),(36, "2147483646000.999998"),(37, "2147483646000.099999"),(38, "2147483646000.900000"),(39, "2147483646000.900001"),
      (40, "-2147483648000.000000"),(41, "-2147483648000.000001"),(42, "-2147483648000.000009"),(43, "-2147483648000.999999"),(44, "-2147483648000.999998"),(45, "-2147483648000.099999"),(46, "-2147483648000.900000"),(47, "-2147483648000.900001"),(48, "-2147483647000.000000"),(49, "-2147483647000.000001"),(50, "-2147483647000.000009"),(51, "-2147483647000.999999"),(52, "-2147483647000.999998"),(53, "-2147483647000.099999"),(54, "-2147483647000.900000"),(55, "-2147483647000.900001")
      ,(56, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_20_6_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_20_6_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_int_from_decimalv2_20_6_0_not_nullable;"
    sql "create table test_cast_to_int_from_decimalv2_20_6_0_not_nullable(f1 int, f2 decimalv2(20, 6)) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_decimalv2_20_6_0_not_nullable values (0, "0.000000"),(1, "0.000001"),(2, "0.000009"),(3, "0.999999"),(4, "0.999998"),(5, "0.099999"),(6, "0.900000"),(7, "0.900001"),(8, "1000.000000"),(9, "1000.000001"),(10, "1000.000009"),(11, "1000.999999"),(12, "1000.999998"),(13, "1000.099999"),(14, "1000.900000"),(15, "1000.900001"),(16, "9000.000000"),(17, "9000.000001"),(18, "9000.000009"),(19, "9000.999999"),
      (20, "9000.999998"),(21, "9000.099999"),(22, "9000.900000"),(23, "9000.900001"),(24, "2147483647000.000000"),(25, "2147483647000.000001"),(26, "2147483647000.000009"),(27, "2147483647000.999999"),(28, "2147483647000.999998"),(29, "2147483647000.099999"),(30, "2147483647000.900000"),(31, "2147483647000.900001"),(32, "2147483646000.000000"),(33, "2147483646000.000001"),(34, "2147483646000.000009"),(35, "2147483646000.999999"),(36, "2147483646000.999998"),(37, "2147483646000.099999"),(38, "2147483646000.900000"),(39, "2147483646000.900001"),
      (40, "-2147483648000.000000"),(41, "-2147483648000.000001"),(42, "-2147483648000.000009"),(43, "-2147483648000.999999"),(44, "-2147483648000.999998"),(45, "-2147483648000.099999"),(46, "-2147483648000.900000"),(47, "-2147483648000.900001"),(48, "-2147483647000.000000"),(49, "-2147483647000.000001"),(50, "-2147483647000.000009"),(51, "-2147483647000.999999"),(52, "-2147483647000.999998"),(53, "-2147483647000.099999"),(54, "-2147483647000.900000"),(55, "-2147483647000.900001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_20_6_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_decimalv2_20_6_0_not_nullable order by 1;'

}