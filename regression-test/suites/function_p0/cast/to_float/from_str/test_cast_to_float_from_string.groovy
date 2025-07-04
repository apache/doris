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


suite("test_cast_to_float_from_string") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_float_from_string_0_nullable;"
    sql "create table test_cast_to_float_from_string_0_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_string_0_nullable values (0, "0"),(1, "1"),(2, "9"),(3, "100000"),(4, "123456"),(5, "999999"),(6, "0."),(7, "1."),(8, "9."),(9, "100000."),(10, "123456."),(11, "999999."),(12, ".0"),(13, ".1"),(14, ".9"),(15, ".000001"),(16, ".000009"),(17, ".100000"),(18, ".900000"),(19, ".100001"),
      (20, ".900009"),(21, "0.123456"),(22, ".999999"),(23, "0.0"),(24, "1.0"),(25, "9.0"),(26, "100000.0"),(27, "999999.0"),(28, "123456.0"),(29, "0.000000"),(30, "0.000001"),(31, "0.000009"),(32, "0.100001"),(33, "0.900009"),(34, "1.000000"),(35, "9.000000"),(36, "1.00001"),(37, "9.00001"),(38, "100000.000000"),(39, "100001.000000"),
      (40, "999999.000000"),(41, "123456.000000"),(42, "123.456"),(43, "999.999"),(44, "nan"),(45, "NaN"),(46, "NAN"),(47, "inf"),(48, "Inf"),(49, "INF"),(50, "infinity"),(51, "Infinity"),(52, "INFINITY"),(53, "3.4028235e+38"),(54, "1.1754944e-38"),(55, "-0"),(56, "-1"),(57, "-9"),(58, "-100000"),(59, "-123456"),
      (60, "-999999"),(61, "-0."),(62, "-1."),(63, "-9."),(64, "-100000."),(65, "-123456."),(66, "-999999."),(67, "-.0"),(68, "-.1"),(69, "-.9"),(70, "-.000001"),(71, "-.000009"),(72, "-.100000"),(73, "-.900000"),(74, "-.100001"),(75, "-.900009"),(76, "-0.123456"),(77, "-.999999"),(78, "-0.0"),(79, "-1.0"),
      (80, "-9.0"),(81, "-100000.0"),(82, "-999999.0"),(83, "-123456.0"),(84, "-0.000000"),(85, "-0.000001"),(86, "-0.000009"),(87, "-0.100001"),(88, "-0.900009"),(89, "-1.000000"),(90, "-9.000000"),(91, "-1.00001"),(92, "-9.00001"),(93, "-100000.000000"),(94, "-100001.000000"),(95, "-999999.000000"),(96, "-123456.000000"),(97, "-123.456"),(98, "-999.999"),(99, "-nan"),
      (100, "-NaN"),(101, "-NAN"),(102, "-inf"),(103, "-Inf"),(104, "-INF"),(105, "-infinity"),(106, "-Infinity"),(107, "-INFINITY"),(108, "-3.4028235e+38"),(109, "-1.1754944e-38")
      ,(110, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_string_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_string_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_float_from_string_0_not_nullable;"
    sql "create table test_cast_to_float_from_string_0_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_string_0_not_nullable values (0, "0"),(1, "1"),(2, "9"),(3, "100000"),(4, "123456"),(5, "999999"),(6, "0."),(7, "1."),(8, "9."),(9, "100000."),(10, "123456."),(11, "999999."),(12, ".0"),(13, ".1"),(14, ".9"),(15, ".000001"),(16, ".000009"),(17, ".100000"),(18, ".900000"),(19, ".100001"),
      (20, ".900009"),(21, "0.123456"),(22, ".999999"),(23, "0.0"),(24, "1.0"),(25, "9.0"),(26, "100000.0"),(27, "999999.0"),(28, "123456.0"),(29, "0.000000"),(30, "0.000001"),(31, "0.000009"),(32, "0.100001"),(33, "0.900009"),(34, "1.000000"),(35, "9.000000"),(36, "1.00001"),(37, "9.00001"),(38, "100000.000000"),(39, "100001.000000"),
      (40, "999999.000000"),(41, "123456.000000"),(42, "123.456"),(43, "999.999"),(44, "nan"),(45, "NaN"),(46, "NAN"),(47, "inf"),(48, "Inf"),(49, "INF"),(50, "infinity"),(51, "Infinity"),(52, "INFINITY"),(53, "3.4028235e+38"),(54, "1.1754944e-38"),(55, "-0"),(56, "-1"),(57, "-9"),(58, "-100000"),(59, "-123456"),
      (60, "-999999"),(61, "-0."),(62, "-1."),(63, "-9."),(64, "-100000."),(65, "-123456."),(66, "-999999."),(67, "-.0"),(68, "-.1"),(69, "-.9"),(70, "-.000001"),(71, "-.000009"),(72, "-.100000"),(73, "-.900000"),(74, "-.100001"),(75, "-.900009"),(76, "-0.123456"),(77, "-.999999"),(78, "-0.0"),(79, "-1.0"),
      (80, "-9.0"),(81, "-100000.0"),(82, "-999999.0"),(83, "-123456.0"),(84, "-0.000000"),(85, "-0.000001"),(86, "-0.000009"),(87, "-0.100001"),(88, "-0.900009"),(89, "-1.000000"),(90, "-9.000000"),(91, "-1.00001"),(92, "-9.00001"),(93, "-100000.000000"),(94, "-100001.000000"),(95, "-999999.000000"),(96, "-123456.000000"),(97, "-123.456"),(98, "-999.999"),(99, "-nan"),
      (100, "-NaN"),(101, "-NAN"),(102, "-inf"),(103, "-Inf"),(104, "-INF"),(105, "-infinity"),(106, "-Infinity"),(107, "-INFINITY"),(108, "-3.4028235e+38"),(109, "-1.1754944e-38");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_string_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_string_0_not_nullable order by 1;'

}