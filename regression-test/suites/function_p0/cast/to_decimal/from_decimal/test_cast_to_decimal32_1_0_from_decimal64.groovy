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


suite("test_cast_to_decimal32_1_0_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_5 values (13, "0.00000"),(14, "0.00001"),(15, "0.00009"),(16, "0.09999"),(17, "0.90000"),(18, "0.90001"),(19, "0.99998"),(20, "0.99999"),(21, "8.00000"),(22, "8.00001"),
      (23, "8.00009"),(24, "8.09999"),(25, "8.90000"),(26, "8.90001"),(27, "8.99998"),(28, "8.99999"),(29, "9.00000"),(30, "9.00001"),(31, "9.00009"),(32, "9.09999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_9 values (33, "0.000000000"),(34, "0.000000001"),(35, "0.000000009"),(36, "0.099999999"),(37, "0.900000000"),(38, "0.900000001"),(39, "0.999999998"),(40, "0.999999999"),(41, "8.000000000"),(42, "8.000000001"),
      (43, "8.000000009"),(44, "8.099999999"),(45, "8.900000000"),(46, "8.900000001"),(47, "8.999999998"),(48, "8.999999999"),(49, "9.000000000"),(50, "9.000000001"),(51, "9.000000009"),(52, "9.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_10_10 values (53, "0.0000000000"),(54, "0.0000000001"),(55, "0.0000000009"),(56, "0.0999999999"),(57, "0.9000000000"),(58, "0.9000000001"),(59, "0.9999999998"),(60, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_0 values (61, "0"),(62, "8"),(63, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_1 values (64, "0.0"),(65, "0.1"),(66, "0.8"),(67, "0.9"),(68, "8.0"),(69, "8.1"),(70, "8.8"),(71, "8.9"),(72, "9.0"),(73, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_8 values (74, "0.00000000"),(75, "0.00000001"),(76, "0.00000009"),(77, "0.09999999"),(78, "0.90000000"),(79, "0.90000001"),(80, "0.99999998"),(81, "0.99999999"),(82, "8.00000000"),(83, "8.00000001"),
      (84, "8.00000009"),(85, "8.09999999"),(86, "8.90000000"),(87, "8.90000001"),(88, "8.99999998"),(89, "8.99999999"),(90, "9.00000000"),(91, "9.00000001"),(92, "9.00000009"),(93, "9.09999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_16 values (94, "0.0000000000000000"),(95, "0.0000000000000001"),(96, "0.0000000000000009"),(97, "0.0999999999999999"),(98, "0.9000000000000000"),(99, "0.9000000000000001"),(100, "0.9999999999999998"),(101, "0.9999999999999999"),(102, "8.0000000000000000"),(103, "8.0000000000000001"),
      (104, "8.0000000000000009"),(105, "8.0999999999999999"),(106, "8.9000000000000000"),(107, "8.9000000000000001"),(108, "8.9999999999999998"),(109, "8.9999999999999999"),(110, "9.0000000000000000"),(111, "9.0000000000000001"),(112, "9.0000000000000009"),(113, "9.0999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_17_17 values (114, "0.00000000000000000"),(115, "0.00000000000000001"),(116, "0.00000000000000009"),(117, "0.09999999999999999"),(118, "0.90000000000000000"),(119, "0.90000000000000001"),(120, "0.99999999999999998"),(121, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_0 values (122, "0"),(123, "8"),(124, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_1 values (125, "0.0"),(126, "0.1"),(127, "0.8"),(128, "0.9"),(129, "8.0"),(130, "8.1"),(131, "8.8"),(132, "8.9"),(133, "9.0"),(134, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_9 values (135, "0.000000000"),(136, "0.000000001"),(137, "0.000000009"),(138, "0.099999999"),(139, "0.900000000"),(140, "0.900000001"),(141, "0.999999998"),(142, "0.999999999"),(143, "8.000000000"),(144, "8.000000001"),
      (145, "8.000000009"),(146, "8.099999999"),(147, "8.900000000"),(148, "8.900000001"),(149, "8.999999998"),(150, "8.999999999"),(151, "9.000000000"),(152, "9.000000001"),(153, "9.000000009"),(154, "9.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_17 values (155, "0.00000000000000000"),(156, "0.00000000000000001"),(157, "0.00000000000000009"),(158, "0.09999999999999999"),(159, "0.90000000000000000"),(160, "0.90000000000000001"),(161, "0.99999999999999998"),(162, "0.99999999999999999"),(163, "8.00000000000000000"),(164, "8.00000000000000001"),
      (165, "8.00000000000000009"),(166, "8.09999999999999999"),(167, "8.90000000000000000"),(168, "8.90000000000000001"),(169, "8.99999999999999998"),(170, "8.99999999999999999"),(171, "9.00000000000000000"),(172, "9.00000000000000001"),(173, "9.00000000000000009"),(174, "9.09999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal64_18_18 values (175, "0.000000000000000000"),(176, "0.000000000000000001"),(177, "0.000000000000000009"),(178, "0.099999999999999999"),(179, "0.900000000000000000"),(180, "0.900000000000000001"),(181, "0.999999999999999998"),(182, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal64_18_18 order by 1;'

}