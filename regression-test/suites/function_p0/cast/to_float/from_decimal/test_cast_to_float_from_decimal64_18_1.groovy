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


suite("test_cast_to_float_from_decimal64_18_1") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_float_from_decimal64_18_1_0_nullable;"
    sql "create table test_cast_to_float_from_decimal64_18_1_0_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_decimal64_18_1_0_nullable values (0, "0.0"),(1, "0.0"),(2, "0.1"),(3, "-0.1"),(4, "0.8"),(5, "-0.8"),(6, "0.9"),(7, "-0.9"),(8, "1.0"),(9, "-1.0"),(10, "1.1"),(11, "-1.1"),(12, "1.8"),(13, "-1.8"),(14, "1.9"),(15, "-1.9"),(16, "9.0"),(17, "-9.0"),(18, "9.1"),(19, "-9.1"),
      (20, "9.8"),(21, "-9.8"),(22, "9.9"),(23, "-9.9"),(24, "9999999999999999.0"),(25, "-9999999999999999.0"),(26, "9999999999999999.1"),(27, "-9999999999999999.1"),(28, "9999999999999999.8"),(29, "-9999999999999999.8"),(30, "9999999999999999.9"),(31, "-9999999999999999.9"),(32, "90000000000000000.0"),(33, "-90000000000000000.0"),(34, "90000000000000000.1"),(35, "-90000000000000000.1"),(36, "90000000000000000.8"),(37, "-90000000000000000.8"),(38, "90000000000000000.9"),(39, "-90000000000000000.9"),
      (40, "90000000000000001.0"),(41, "-90000000000000001.0"),(42, "90000000000000001.1"),(43, "-90000000000000001.1"),(44, "90000000000000001.8"),(45, "-90000000000000001.8"),(46, "90000000000000001.9"),(47, "-90000000000000001.9"),(48, "99999999999999998.0"),(49, "-99999999999999998.0"),(50, "99999999999999998.1"),(51, "-99999999999999998.1"),(52, "99999999999999998.8"),(53, "-99999999999999998.8"),(54, "99999999999999998.9"),(55, "-99999999999999998.9"),(56, "99999999999999999.0"),(57, "-99999999999999999.0"),(58, "99999999999999999.1"),(59, "-99999999999999999.1"),
      (60, "99999999999999999.8"),(61, "-99999999999999999.8"),(62, "99999999999999999.9"),(63, "-99999999999999999.9")
      ,(64, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_decimal64_18_1_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_decimal64_18_1_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_float_from_decimal64_18_1_0_not_nullable;"
    sql "create table test_cast_to_float_from_decimal64_18_1_0_not_nullable(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_decimal64_18_1_0_not_nullable values (0, "0.0"),(1, "0.0"),(2, "0.1"),(3, "-0.1"),(4, "0.8"),(5, "-0.8"),(6, "0.9"),(7, "-0.9"),(8, "1.0"),(9, "-1.0"),(10, "1.1"),(11, "-1.1"),(12, "1.8"),(13, "-1.8"),(14, "1.9"),(15, "-1.9"),(16, "9.0"),(17, "-9.0"),(18, "9.1"),(19, "-9.1"),
      (20, "9.8"),(21, "-9.8"),(22, "9.9"),(23, "-9.9"),(24, "9999999999999999.0"),(25, "-9999999999999999.0"),(26, "9999999999999999.1"),(27, "-9999999999999999.1"),(28, "9999999999999999.8"),(29, "-9999999999999999.8"),(30, "9999999999999999.9"),(31, "-9999999999999999.9"),(32, "90000000000000000.0"),(33, "-90000000000000000.0"),(34, "90000000000000000.1"),(35, "-90000000000000000.1"),(36, "90000000000000000.8"),(37, "-90000000000000000.8"),(38, "90000000000000000.9"),(39, "-90000000000000000.9"),
      (40, "90000000000000001.0"),(41, "-90000000000000001.0"),(42, "90000000000000001.1"),(43, "-90000000000000001.1"),(44, "90000000000000001.8"),(45, "-90000000000000001.8"),(46, "90000000000000001.9"),(47, "-90000000000000001.9"),(48, "99999999999999998.0"),(49, "-99999999999999998.0"),(50, "99999999999999998.1"),(51, "-99999999999999998.1"),(52, "99999999999999998.8"),(53, "-99999999999999998.8"),(54, "99999999999999998.9"),(55, "-99999999999999998.9"),(56, "99999999999999999.0"),(57, "-99999999999999999.0"),(58, "99999999999999999.1"),(59, "-99999999999999999.1"),
      (60, "99999999999999999.8"),(61, "-99999999999999999.8"),(62, "99999999999999999.9"),(63, "-99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_decimal64_18_1_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_decimal64_18_1_0_not_nullable order by 1;'

}