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


suite("test_cast_to_int_from_str_with_fraction") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_str_with_fraction_0;"
    sql "create table test_cast_to_int_from_str_with_fraction_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_str_with_fraction_0 values (0, "+0000.4"),(1, "+0000.5"),(2, "+0001.4"),(3, "+0001.5"),(4, "+0009.4"),(5, "+0009.5"),(6, "+000123.4"),(7, "+000123.5"),(8, "+0002147483647.4"),(9, "+0002147483647.5"),(10, "-0001.4"),(11, "-0001.5"),(12, "-0009.4"),(13, "-0009.5"),(14, "-000123.4"),(15, "-000123.5"),(16, "-0002147483648.4"),(17, "-0002147483648.5"),(18, "+0002147483646.4"),(19, "+0002147483646.5"),
      (20, "-0002147483647.4"),(21, "-0002147483647.5"),(22, "+0.4"),(23, "+0.5"),(24, "+1.4"),(25, "+1.5"),(26, "+9.4"),(27, "+9.5"),(28, "+123.4"),(29, "+123.5"),(30, "+2147483647.4"),(31, "+2147483647.5"),(32, "-1.4"),(33, "-1.5"),(34, "-9.4"),(35, "-9.5"),(36, "-123.4"),(37, "-123.5"),(38, "-2147483648.4"),(39, "-2147483648.5"),
      (40, "+2147483646.4"),(41, "+2147483646.5"),(42, "-2147483647.4"),(43, "-2147483647.5"),(44, "0000.4"),(45, "0000.5"),(46, "0001.4"),(47, "0001.5"),(48, "0009.4"),(49, "0009.5"),(50, "000123.4"),(51, "000123.5"),(52, "0002147483647.4"),(53, "0002147483647.5"),(54, "-0001.4"),(55, "-0001.5"),(56, "-0009.4"),(57, "-0009.5"),(58, "-000123.4"),(59, "-000123.5"),
      (60, "-0002147483648.4"),(61, "-0002147483648.5"),(62, "0002147483646.4"),(63, "0002147483646.5"),(64, "-0002147483647.4"),(65, "-0002147483647.5"),(66, "0.4"),(67, "0.5"),(68, "1.4"),(69, "1.5"),(70, "9.4"),(71, "9.5"),(72, "123.4"),(73, "123.5"),(74, "2147483647.4"),(75, "2147483647.5"),(76, "-1.4"),(77, "-1.5"),(78, "-9.4"),(79, "-9.5"),
      (80, "-123.4"),(81, "-123.5"),(82, "-2147483648.4"),(83, "-2147483648.5"),(84, "2147483646.4"),(85, "2147483646.5"),(86, "-2147483647.4"),(87, "-2147483647.5");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_int_from_str_with_fraction_data_start_index = 0
    def test_cast_to_int_from_str_with_fraction_data_end_index = 88
    for (int data_index = test_cast_to_int_from_str_with_fraction_data_start_index; data_index < test_cast_to_int_from_str_with_fraction_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as int) from test_cast_to_int_from_str_with_fraction where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_str_with_fraction_0 order by 1;'

}