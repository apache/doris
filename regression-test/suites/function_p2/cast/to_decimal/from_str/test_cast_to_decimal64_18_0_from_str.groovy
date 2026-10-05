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


suite("test_cast_to_decimal64_18_0_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_18_0_from_str_0_nullable;"
    sql "create table test_cast_to_decimal64_18_0_from_str_0_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_str_0_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, "0"),(3, "1"),(4, "9"),(5, "99999999999999999"),(6, "900000000000000000"),(7, "900000000000000001"),(8, "999999999999999998"),(9, "999999999999999999"),(10, "0."),(11, "1."),(12, "9."),(13, "99999999999999999."),(14, "900000000000000000."),(15, "900000000000000001."),(16, "999999999999999998."),(17, "999999999999999999."),(18, "-0"),(19, "-1"),
      (20, "-9"),(21, "-99999999999999999"),(22, "-900000000000000000"),(23, "-900000000000000001"),(24, "-999999999999999998"),(25, "-999999999999999999"),(26, "-0."),(27, "-1."),(28, "-9."),(29, "-99999999999999999."),(30, "-900000000000000000."),(31, "-900000000000000001."),(32, "-999999999999999998."),(33, "-999999999999999999."),(34, "0.49999"),(35, "1.49999"),(36, "9.49999"),(37, "99999999999999999.49999"),(38, "900000000000000000.49999"),(39, "900000000000000001.49999"),
      (40, "999999999999999998.49999"),(41, "999999999999999999.49999"),(42, "0.5"),(43, "1.5"),(44, "9.5"),(45, "99999999999999999.5"),(46, "900000000000000000.5"),(47, "900000000000000001.5"),(48, "999999999999999998.5"),(49, "999999999999999999.49999"),(50, "-0.49999"),(51, "-1.49999"),(52, "-9.49999"),(53, "-99999999999999999.49999"),(54, "-900000000000000000.49999"),(55, "-900000000000000001.49999"),(56, "-999999999999999998.49999"),(57, "-999999999999999999.49999"),(58, "-0.5"),(59, "-1.5"),
      (60, "-9.5"),(61, "-99999999999999999.5"),(62, "-900000000000000000.5"),(63, "-900000000000000001.5"),(64, "-999999999999999998.5"),(65, "-999999999999999999.49999")
      ,(66, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_str_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_str_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_str_0_not_nullable;"
    sql "create table test_cast_to_decimal64_18_0_from_str_0_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_str_0_not_nullable values (0, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(1, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(2, "0"),(3, "1"),(4, "9"),(5, "99999999999999999"),(6, "900000000000000000"),(7, "900000000000000001"),(8, "999999999999999998"),(9, "999999999999999999"),(10, "0."),(11, "1."),(12, "9."),(13, "99999999999999999."),(14, "900000000000000000."),(15, "900000000000000001."),(16, "999999999999999998."),(17, "999999999999999999."),(18, "-0"),(19, "-1"),
      (20, "-9"),(21, "-99999999999999999"),(22, "-900000000000000000"),(23, "-900000000000000001"),(24, "-999999999999999998"),(25, "-999999999999999999"),(26, "-0."),(27, "-1."),(28, "-9."),(29, "-99999999999999999."),(30, "-900000000000000000."),(31, "-900000000000000001."),(32, "-999999999999999998."),(33, "-999999999999999999."),(34, "0.49999"),(35, "1.49999"),(36, "9.49999"),(37, "99999999999999999.49999"),(38, "900000000000000000.49999"),(39, "900000000000000001.49999"),
      (40, "999999999999999998.49999"),(41, "999999999999999999.49999"),(42, "0.5"),(43, "1.5"),(44, "9.5"),(45, "99999999999999999.5"),(46, "900000000000000000.5"),(47, "900000000000000001.5"),(48, "999999999999999998.5"),(49, "999999999999999999.49999"),(50, "-0.49999"),(51, "-1.49999"),(52, "-9.49999"),(53, "-99999999999999999.49999"),(54, "-900000000000000000.49999"),(55, "-900000000000000001.49999"),(56, "-999999999999999998.49999"),(57, "-999999999999999999.49999"),(58, "-0.5"),(59, "-1.5"),
      (60, "-9.5"),(61, "-99999999999999999.5"),(62, "-900000000000000000.5"),(63, "-900000000000000001.5"),(64, "-999999999999999998.5"),(65, "-999999999999999999.49999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_str_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_str_0_not_nullable order by 1;'

}