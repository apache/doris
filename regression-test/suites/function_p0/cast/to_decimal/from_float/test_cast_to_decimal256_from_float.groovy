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


suite("test_cast_to_decimal256_from_float") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_to_decimal256_76_0_from_float32;"
    sql "create table test_cast_to_decimal256_76_0_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_float32 values (0, "0.04"),(1, "1.04"),(2, "9.04"),(3, "0.05"),(4, "1.05"),(5, "9.05"),(6, "-0.04"),(7, "-1.04"),(8, "-9.04"),(9, "-0.05"),
      (10, "-1.05"),(11, "-9.05");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_float32 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_float32;"
    sql "create table test_cast_to_decimal256_76_38_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_float32 values (12, "4e-39"),(13, "1.4e-38"),(14, "9.4e-38"),(15, "0.1"),(16, "0.9"),(17, "0.9"),(18, "1"),(19, "1"),(20, "1"),(21, "1"),
      (22, "1"),(23, "1.1"),(24, "5e-39"),(25, "1.5e-38"),(26, "9.5e-38"),(27, "0.1"),(28, "0.9"),(29, "0.9"),(30, "1"),(31, "1"),
      (32, "1"),(33, "1"),(34, "1"),(35, "1.1"),(36, "-4e-39"),(37, "-1.4e-38"),(38, "-9.4e-38"),(39, "-0.1"),(40, "-0.9"),(41, "-0.9"),
      (42, "-1"),(43, "-1"),(44, "-1"),(45, "-1"),(46, "-1"),(47, "-1.1"),(48, "-5e-39"),(49, "-1.5e-38"),(50, "-9.5e-38"),(51, "-0.1"),
      (52, "-0.9"),(53, "-0.9"),(54, "-1"),(55, "-1"),(56, "-1"),(57, "-1"),(58, "-1"),(59, "-1.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_float32 order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_float32;"
    sql "create table test_cast_to_decimal256_76_75_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_float32 values (60, "0"),(61, "0"),(62, "0"),(63, "0"),(64, "0"),(65, "0"),(66, "-0"),(67, "-0"),(68, "-0"),(69, "-0"),
      (70, "-0"),(71, "-0");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_float32 order by 1;'

}