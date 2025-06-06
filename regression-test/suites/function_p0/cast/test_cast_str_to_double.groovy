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


suite("test_cast_str_to_double") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_str_to_double_0;"
    sql "create table test_cast_str_to_double_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_str_to_double_0 values (0, "0"),(1, "1"),(2, "9"),(3, "100000"),(4, "123456"),(5, "999999"),(6, "0."),(7, "1."),(8, "9."),(9, "100000."),(10, "123456."),(11, "999999."),(12, ".0"),(13, ".1"),(14, ".9"),(15, ".000001"),(16, ".000009"),(17, ".100000"),(18, ".900000"),(19, ".100001"),
      (20, ".900009"),(21, "0.123456"),(22, ".999999"),(23, "0.0"),(24, "1.0"),(25, "9.0"),(26, "100000.0"),(27, "999999.0"),(28, "123456.0"),(29, "0.000000"),(30, "0.000001"),(31, "0.000009"),(32, "0.100001"),(33, "0.900009"),(34, "1.000000"),(35, "9.000000"),(36, "1.00001"),(37, "9.00001"),(38, "100000.000000"),(39, "100001.000000"),
      (40, "999999.000000"),(41, "123456.000000"),(42, "123.456"),(43, "999.999"),(44, "nan"),(45, "NaN"),(46, "NAN"),(47, "inf"),(48, "Inf"),(49, "INF"),(50, "infinity"),(51, "Infinity"),(52, "INFINITY"),(53, "1.7976931348623157e+308"),(54, "2.2250738585072014e-308"),(55, "5e-324"),(56, "-0"),(57, "-1"),(58, "-9"),(59, "-100000"),
      (60, "-123456"),(61, "-999999"),(62, "-0."),(63, "-1."),(64, "-9."),(65, "-100000."),(66, "-123456."),(67, "-999999."),(68, "-.0"),(69, "-.1"),(70, "-.9"),(71, "-.000001"),(72, "-.000009"),(73, "-.100000"),(74, "-.900000"),(75, "-.100001"),(76, "-.900009"),(77, "-0.123456"),(78, "-.999999"),(79, "-0.0"),
      (80, "-1.0"),(81, "-9.0"),(82, "-100000.0"),(83, "-999999.0"),(84, "-123456.0"),(85, "-0.000000"),(86, "-0.000001"),(87, "-0.000009"),(88, "-0.100001"),(89, "-0.900009"),(90, "-1.000000"),(91, "-9.000000"),(92, "-1.00001"),(93, "-9.00001"),(94, "-100000.000000"),(95, "-100001.000000"),(96, "-999999.000000"),(97, "-123456.000000"),(98, "-123.456"),(99, "-999.999"),
      (100, "-nan"),(101, "-NaN"),(102, "-NAN"),(103, "-inf"),(104, "-Inf"),(105, "-INF"),(106, "-infinity"),(107, "-Infinity"),(108, "-INFINITY"),(109, "-1.7976931348623157e+308"),(110, "-2.2250738585072014e-308"),(111, "-5e-324");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as double) from test_cast_str_to_double_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as double) from test_cast_str_to_double_0 order by 1;'

}