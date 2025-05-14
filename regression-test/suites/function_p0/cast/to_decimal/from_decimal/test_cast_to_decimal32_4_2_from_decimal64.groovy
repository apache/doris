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


suite("test_cast_to_decimal32_4_2_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_0 values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "9.0"),(11, "9.1"),(12, "9.8"),(13, "9.9"),(14, "90.0"),(15, "90.1"),
      (16, "90.8"),(17, "90.9"),(18, "91.0"),(19, "91.1"),(20, "91.8"),(21, "91.9"),(22, "98.0"),(23, "98.1"),(24, "98.8"),(25, "98.9"),
      (26, "99.0"),(27, "99.1"),(28, "99.8"),(29, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_5 values (30, "0.00000"),(31, "0.00001"),(32, "0.00009"),(33, "0.09999"),(34, "0.90000"),(35, "0.90001"),(36, "0.99998"),(37, "0.99999"),(38, "9.00000"),(39, "9.00001"),
      (40, "9.00009"),(41, "9.09999"),(42, "9.90000"),(43, "9.90001"),(44, "9.99998"),(45, "9.99999"),(46, "90.00000"),(47, "90.00001"),(48, "90.00009"),(49, "90.09999"),
      (50, "90.90000"),(51, "90.90001"),(52, "90.99998"),(53, "90.99999"),(54, "91.00000"),(55, "91.00001"),(56, "91.00009"),(57, "91.09999"),(58, "91.90000"),(59, "91.90001"),
      (60, "91.99998"),(61, "91.99999"),(62, "98.00000"),(63, "98.00001"),(64, "98.00009"),(65, "98.09999"),(66, "98.90000"),(67, "98.90001"),(68, "98.99998"),(69, "98.99999"),
      (70, "99.00000"),(71, "99.00001"),(72, "99.00009"),(73, "99.09999"),(74, "99.90000"),(75, "99.90001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_9 values (76, "0.000000000"),(77, "0.000000001"),(78, "0.000000009"),(79, "0.099999999"),(80, "0.900000000"),(81, "0.900000001"),(82, "0.999999998"),(83, "0.999999999"),(84, "8.000000000"),(85, "8.000000001"),
      (86, "8.000000009"),(87, "8.099999999"),(88, "8.900000000"),(89, "8.900000001"),(90, "8.999999998"),(91, "8.999999999"),(92, "9.000000000"),(93, "9.000000001"),(94, "9.000000009"),(95, "9.099999999"),
      (96, "9.900000000"),(97, "9.900000001"),(98, "9.999999998"),(99, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_10_10 values (100, "0.0000000000"),(101, "0.0000000001"),(102, "0.0000000009"),(103, "0.0999999999"),(104, "0.9000000000"),(105, "0.9000000001"),(106, "0.9999999998"),(107, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_0 values (108, "0"),(109, "9"),(110, "90"),(111, "91"),(112, "98"),(113, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_1 values (114, "0.0"),(115, "0.1"),(116, "0.8"),(117, "0.9"),(118, "9.0"),(119, "9.1"),(120, "9.8"),(121, "9.9"),(122, "90.0"),(123, "90.1"),
      (124, "90.8"),(125, "90.9"),(126, "91.0"),(127, "91.1"),(128, "91.8"),(129, "91.9"),(130, "98.0"),(131, "98.1"),(132, "98.8"),(133, "98.9"),
      (134, "99.0"),(135, "99.1"),(136, "99.8"),(137, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_8 values (138, "0.00000000"),(139, "0.00000001"),(140, "0.00000009"),(141, "0.09999999"),(142, "0.90000000"),(143, "0.90000001"),(144, "0.99999998"),(145, "0.99999999"),(146, "9.00000000"),(147, "9.00000001"),
      (148, "9.00000009"),(149, "9.09999999"),(150, "9.90000000"),(151, "9.90000001"),(152, "9.99999998"),(153, "9.99999999"),(154, "90.00000000"),(155, "90.00000001"),(156, "90.00000009"),(157, "90.09999999"),
      (158, "90.90000000"),(159, "90.90000001"),(160, "90.99999998"),(161, "90.99999999"),(162, "91.00000000"),(163, "91.00000001"),(164, "91.00000009"),(165, "91.09999999"),(166, "91.90000000"),(167, "91.90000001"),
      (168, "91.99999998"),(169, "91.99999999"),(170, "98.00000000"),(171, "98.00000001"),(172, "98.00000009"),(173, "98.09999999"),(174, "98.90000000"),(175, "98.90000001"),(176, "98.99999998"),(177, "98.99999999"),
      (178, "99.00000000"),(179, "99.00000001"),(180, "99.00000009"),(181, "99.09999999"),(182, "99.90000000"),(183, "99.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_16 values (184, "0.0000000000000000"),(185, "0.0000000000000001"),(186, "0.0000000000000009"),(187, "0.0999999999999999"),(188, "0.9000000000000000"),(189, "0.9000000000000001"),(190, "0.9999999999999998"),(191, "0.9999999999999999"),(192, "8.0000000000000000"),(193, "8.0000000000000001"),
      (194, "8.0000000000000009"),(195, "8.0999999999999999"),(196, "8.9000000000000000"),(197, "8.9000000000000001"),(198, "8.9999999999999998"),(199, "8.9999999999999999"),(200, "9.0000000000000000"),(201, "9.0000000000000001"),(202, "9.0000000000000009"),(203, "9.0999999999999999"),
      (204, "9.9000000000000000"),(205, "9.9000000000000001"),(206, "9.9999999999999998"),(207, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_17_17 values (208, "0.00000000000000000"),(209, "0.00000000000000001"),(210, "0.00000000000000009"),(211, "0.09999999999999999"),(212, "0.90000000000000000"),(213, "0.90000000000000001"),(214, "0.99999999999999998"),(215, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_0 values (216, "0"),(217, "9"),(218, "90"),(219, "91"),(220, "98"),(221, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_1 values (222, "0.0"),(223, "0.1"),(224, "0.8"),(225, "0.9"),(226, "9.0"),(227, "9.1"),(228, "9.8"),(229, "9.9"),(230, "90.0"),(231, "90.1"),
      (232, "90.8"),(233, "90.9"),(234, "91.0"),(235, "91.1"),(236, "91.8"),(237, "91.9"),(238, "98.0"),(239, "98.1"),(240, "98.8"),(241, "98.9"),
      (242, "99.0"),(243, "99.1"),(244, "99.8"),(245, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_9 values (246, "0.000000000"),(247, "0.000000001"),(248, "0.000000009"),(249, "0.099999999"),(250, "0.900000000"),(251, "0.900000001"),(252, "0.999999998"),(253, "0.999999999"),(254, "9.000000000"),(255, "9.000000001"),
      (256, "9.000000009"),(257, "9.099999999"),(258, "9.900000000"),(259, "9.900000001"),(260, "9.999999998"),(261, "9.999999999"),(262, "90.000000000"),(263, "90.000000001"),(264, "90.000000009"),(265, "90.099999999"),
      (266, "90.900000000"),(267, "90.900000001"),(268, "90.999999998"),(269, "90.999999999"),(270, "91.000000000"),(271, "91.000000001"),(272, "91.000000009"),(273, "91.099999999"),(274, "91.900000000"),(275, "91.900000001"),
      (276, "91.999999998"),(277, "91.999999999"),(278, "98.000000000"),(279, "98.000000001"),(280, "98.000000009"),(281, "98.099999999"),(282, "98.900000000"),(283, "98.900000001"),(284, "98.999999998"),(285, "98.999999999"),
      (286, "99.000000000"),(287, "99.000000001"),(288, "99.000000009"),(289, "99.099999999"),(290, "99.900000000"),(291, "99.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_17 values (292, "0.00000000000000000"),(293, "0.00000000000000001"),(294, "0.00000000000000009"),(295, "0.09999999999999999"),(296, "0.90000000000000000"),(297, "0.90000000000000001"),(298, "0.99999999999999998"),(299, "0.99999999999999999"),(300, "8.00000000000000000"),(301, "8.00000000000000001"),
      (302, "8.00000000000000009"),(303, "8.09999999999999999"),(304, "8.90000000000000000"),(305, "8.90000000000000001"),(306, "8.99999999999999998"),(307, "8.99999999999999999"),(308, "9.00000000000000000"),(309, "9.00000000000000001"),(310, "9.00000000000000009"),(311, "9.09999999999999999"),
      (312, "9.90000000000000000"),(313, "9.90000000000000001"),(314, "9.99999999999999998"),(315, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal64_18_18 values (316, "0.000000000000000000"),(317, "0.000000000000000001"),(318, "0.000000000000000009"),(319, "0.099999999999999999"),(320, "0.900000000000000000"),(321, "0.900000000000000001"),(322, "0.999999999999999998"),(323, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal64_18_18 order by 1;'

}