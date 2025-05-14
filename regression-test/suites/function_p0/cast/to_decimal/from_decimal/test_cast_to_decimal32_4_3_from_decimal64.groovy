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


suite("test_cast_to_decimal32_4_3_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1"),
      (13, "9.8"),(14, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_5 values (15, "0.00000"),(16, "0.00001"),(17, "0.00009"),(18, "0.09999"),(19, "0.90000"),(20, "0.90001"),(21, "0.99998"),(22, "0.99999"),(23, "8.00000"),(24, "8.00001"),
      (25, "8.00009"),(26, "8.09999"),(27, "8.90000"),(28, "8.90001"),(29, "8.99998"),(30, "8.99999"),(31, "9.00000"),(32, "9.00001"),(33, "9.00009"),(34, "9.09999"),
      (35, "9.90000"),(36, "9.90001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_9 values (37, "0.000000000"),(38, "0.000000001"),(39, "0.000000009"),(40, "0.099999999"),(41, "0.900000000"),(42, "0.900000001"),(43, "0.999999998"),(44, "0.999999999"),(45, "8.000000000"),(46, "8.000000001"),
      (47, "8.000000009"),(48, "8.099999999"),(49, "8.900000000"),(50, "8.900000001"),(51, "8.999999998"),(52, "8.999999999"),(53, "9.000000000"),(54, "9.000000001"),(55, "9.000000009"),(56, "9.099999999"),
      (57, "9.900000000"),(58, "9.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_10_10 values (59, "0.0000000000"),(60, "0.0000000001"),(61, "0.0000000009"),(62, "0.0999999999"),(63, "0.9000000000"),(64, "0.9000000001"),(65, "0.9999999998"),(66, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_0 values (67, "0"),(68, "8"),(69, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_1 values (70, "0.0"),(71, "0.1"),(72, "0.8"),(73, "0.9"),(74, "8.0"),(75, "8.1"),(76, "8.8"),(77, "8.9"),(78, "9.0"),(79, "9.1"),
      (80, "9.8"),(81, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_8 values (82, "0.00000000"),(83, "0.00000001"),(84, "0.00000009"),(85, "0.09999999"),(86, "0.90000000"),(87, "0.90000001"),(88, "0.99999998"),(89, "0.99999999"),(90, "8.00000000"),(91, "8.00000001"),
      (92, "8.00000009"),(93, "8.09999999"),(94, "8.90000000"),(95, "8.90000001"),(96, "8.99999998"),(97, "8.99999999"),(98, "9.00000000"),(99, "9.00000001"),(100, "9.00000009"),(101, "9.09999999"),
      (102, "9.90000000"),(103, "9.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_16 values (104, "0.0000000000000000"),(105, "0.0000000000000001"),(106, "0.0000000000000009"),(107, "0.0999999999999999"),(108, "0.9000000000000000"),(109, "0.9000000000000001"),(110, "0.9999999999999998"),(111, "0.9999999999999999"),(112, "8.0000000000000000"),(113, "8.0000000000000001"),
      (114, "8.0000000000000009"),(115, "8.0999999999999999"),(116, "8.9000000000000000"),(117, "8.9000000000000001"),(118, "8.9999999999999998"),(119, "8.9999999999999999"),(120, "9.0000000000000000"),(121, "9.0000000000000001"),(122, "9.0000000000000009"),(123, "9.0999999999999999"),
      (124, "9.9000000000000000"),(125, "9.9000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_17_17 values (126, "0.00000000000000000"),(127, "0.00000000000000001"),(128, "0.00000000000000009"),(129, "0.09999999999999999"),(130, "0.90000000000000000"),(131, "0.90000000000000001"),(132, "0.99999999999999998"),(133, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_0 values (134, "0"),(135, "8"),(136, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_1 values (137, "0.0"),(138, "0.1"),(139, "0.8"),(140, "0.9"),(141, "8.0"),(142, "8.1"),(143, "8.8"),(144, "8.9"),(145, "9.0"),(146, "9.1"),
      (147, "9.8"),(148, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_9 values (149, "0.000000000"),(150, "0.000000001"),(151, "0.000000009"),(152, "0.099999999"),(153, "0.900000000"),(154, "0.900000001"),(155, "0.999999998"),(156, "0.999999999"),(157, "8.000000000"),(158, "8.000000001"),
      (159, "8.000000009"),(160, "8.099999999"),(161, "8.900000000"),(162, "8.900000001"),(163, "8.999999998"),(164, "8.999999999"),(165, "9.000000000"),(166, "9.000000001"),(167, "9.000000009"),(168, "9.099999999"),
      (169, "9.900000000"),(170, "9.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_17 values (171, "0.00000000000000000"),(172, "0.00000000000000001"),(173, "0.00000000000000009"),(174, "0.09999999999999999"),(175, "0.90000000000000000"),(176, "0.90000000000000001"),(177, "0.99999999999999998"),(178, "0.99999999999999999"),(179, "8.00000000000000000"),(180, "8.00000000000000001"),
      (181, "8.00000000000000009"),(182, "8.09999999999999999"),(183, "8.90000000000000000"),(184, "8.90000000000000001"),(185, "8.99999999999999998"),(186, "8.99999999999999999"),(187, "9.00000000000000000"),(188, "9.00000000000000001"),(189, "9.00000000000000009"),(190, "9.09999999999999999"),
      (191, "9.90000000000000000"),(192, "9.90000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal64_18_18 values (193, "0.000000000000000000"),(194, "0.000000000000000001"),(195, "0.000000000000000009"),(196, "0.099999999999999999"),(197, "0.900000000000000000"),(198, "0.900000000000000001"),(199, "0.999999999999999998"),(200, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal64_18_18 order by 1;'

}