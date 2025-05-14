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


suite("test_cast_to_decimal32_9_1_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_0 values (0, "0"),(1, "9999999"),(2, "90000000"),(3, "90000001"),(4, "99999998"),(5, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "9999999.0"),(11, "9999999.1"),(12, "9999999.8"),(13, "9999999.9"),(14, "90000000.0"),(15, "90000000.1"),
      (16, "90000000.8"),(17, "90000000.9"),(18, "90000001.0"),(19, "90000001.1"),(20, "90000001.8"),(21, "90000001.9"),(22, "99999998.0"),(23, "99999998.1"),(24, "99999998.8"),(25, "99999998.9"),
      (26, "99999999.0"),(27, "99999999.1"),(28, "99999999.8"),(29, "99999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_5 values (30, "0.00000"),(31, "0.00001"),(32, "0.00009"),(33, "0.09999"),(34, "0.90000"),(35, "0.90001"),(36, "0.99998"),(37, "0.99999"),(38, "9999.00000"),(39, "9999.00001"),
      (40, "9999.00009"),(41, "9999.09999"),(42, "9999.90000"),(43, "9999.90001"),(44, "9999.99998"),(45, "9999.99999"),(46, "90000.00000"),(47, "90000.00001"),(48, "90000.00009"),(49, "90000.09999"),
      (50, "90000.90000"),(51, "90000.90001"),(52, "90000.99998"),(53, "90000.99999"),(54, "90001.00000"),(55, "90001.00001"),(56, "90001.00009"),(57, "90001.09999"),(58, "90001.90000"),(59, "90001.90001"),
      (60, "90001.99998"),(61, "90001.99999"),(62, "99998.00000"),(63, "99998.00001"),(64, "99998.00009"),(65, "99998.09999"),(66, "99998.90000"),(67, "99998.90001"),(68, "99998.99998"),(69, "99998.99999"),
      (70, "99999.00000"),(71, "99999.00001"),(72, "99999.00009"),(73, "99999.09999"),(74, "99999.90000"),(75, "99999.90001"),(76, "99999.99998"),(77, "99999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_9 values (78, "0.000000000"),(79, "0.000000001"),(80, "0.000000009"),(81, "0.099999999"),(82, "0.900000000"),(83, "0.900000001"),(84, "0.999999998"),(85, "0.999999999"),(86, "8.000000000"),(87, "8.000000001"),
      (88, "8.000000009"),(89, "8.099999999"),(90, "8.900000000"),(91, "8.900000001"),(92, "8.999999998"),(93, "8.999999999"),(94, "9.000000000"),(95, "9.000000001"),(96, "9.000000009"),(97, "9.099999999"),
      (98, "9.900000000"),(99, "9.900000001"),(100, "9.999999998"),(101, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_10_10 values (102, "0.0000000000"),(103, "0.0000000001"),(104, "0.0000000009"),(105, "0.0999999999"),(106, "0.9000000000"),(107, "0.9000000001"),(108, "0.9999999998"),(109, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_0 values (110, "0"),(111, "9999999"),(112, "90000000"),(113, "90000001"),(114, "99999998"),(115, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_1 values (116, "0.0"),(117, "0.1"),(118, "0.8"),(119, "0.9"),(120, "9999999.0"),(121, "9999999.1"),(122, "9999999.8"),(123, "9999999.9"),(124, "90000000.0"),(125, "90000000.1"),
      (126, "90000000.8"),(127, "90000000.9"),(128, "90000001.0"),(129, "90000001.1"),(130, "90000001.8"),(131, "90000001.9"),(132, "99999998.0"),(133, "99999998.1"),(134, "99999998.8"),(135, "99999998.9"),
      (136, "99999999.0"),(137, "99999999.1"),(138, "99999999.8"),(139, "99999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_8 values (140, "0.00000000"),(141, "0.00000001"),(142, "0.00000009"),(143, "0.09999999"),(144, "0.90000000"),(145, "0.90000001"),(146, "0.99999998"),(147, "0.99999999"),(148, "9999999.00000000"),(149, "9999999.00000001"),
      (150, "9999999.00000009"),(151, "9999999.09999999"),(152, "9999999.90000000"),(153, "9999999.90000001"),(154, "9999999.99999998"),(155, "9999999.99999999"),(156, "90000000.00000000"),(157, "90000000.00000001"),(158, "90000000.00000009"),(159, "90000000.09999999"),
      (160, "90000000.90000000"),(161, "90000000.90000001"),(162, "90000000.99999998"),(163, "90000000.99999999"),(164, "90000001.00000000"),(165, "90000001.00000001"),(166, "90000001.00000009"),(167, "90000001.09999999"),(168, "90000001.90000000"),(169, "90000001.90000001"),
      (170, "90000001.99999998"),(171, "90000001.99999999"),(172, "99999998.00000000"),(173, "99999998.00000001"),(174, "99999998.00000009"),(175, "99999998.09999999"),(176, "99999998.90000000"),(177, "99999998.90000001"),(178, "99999998.99999998"),(179, "99999998.99999999"),
      (180, "99999999.00000000"),(181, "99999999.00000001"),(182, "99999999.00000009"),(183, "99999999.09999999"),(184, "99999999.90000000"),(185, "99999999.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_16 values (186, "0.0000000000000000"),(187, "0.0000000000000001"),(188, "0.0000000000000009"),(189, "0.0999999999999999"),(190, "0.9000000000000000"),(191, "0.9000000000000001"),(192, "0.9999999999999998"),(193, "0.9999999999999999"),(194, "8.0000000000000000"),(195, "8.0000000000000001"),
      (196, "8.0000000000000009"),(197, "8.0999999999999999"),(198, "8.9000000000000000"),(199, "8.9000000000000001"),(200, "8.9999999999999998"),(201, "8.9999999999999999"),(202, "9.0000000000000000"),(203, "9.0000000000000001"),(204, "9.0000000000000009"),(205, "9.0999999999999999"),
      (206, "9.9000000000000000"),(207, "9.9000000000000001"),(208, "9.9999999999999998"),(209, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_17_17 values (210, "0.00000000000000000"),(211, "0.00000000000000001"),(212, "0.00000000000000009"),(213, "0.09999999999999999"),(214, "0.90000000000000000"),(215, "0.90000000000000001"),(216, "0.99999999999999998"),(217, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_0 values (218, "0"),(219, "9999999"),(220, "90000000"),(221, "90000001"),(222, "99999998"),(223, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_1 values (224, "0.0"),(225, "0.1"),(226, "0.8"),(227, "0.9"),(228, "9999999.0"),(229, "9999999.1"),(230, "9999999.8"),(231, "9999999.9"),(232, "90000000.0"),(233, "90000000.1"),
      (234, "90000000.8"),(235, "90000000.9"),(236, "90000001.0"),(237, "90000001.1"),(238, "90000001.8"),(239, "90000001.9"),(240, "99999998.0"),(241, "99999998.1"),(242, "99999998.8"),(243, "99999998.9"),
      (244, "99999999.0"),(245, "99999999.1"),(246, "99999999.8"),(247, "99999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_9 values (248, "0.000000000"),(249, "0.000000001"),(250, "0.000000009"),(251, "0.099999999"),(252, "0.900000000"),(253, "0.900000001"),(254, "0.999999998"),(255, "0.999999999"),(256, "9999999.000000000"),(257, "9999999.000000001"),
      (258, "9999999.000000009"),(259, "9999999.099999999"),(260, "9999999.900000000"),(261, "9999999.900000001"),(262, "9999999.999999998"),(263, "9999999.999999999"),(264, "90000000.000000000"),(265, "90000000.000000001"),(266, "90000000.000000009"),(267, "90000000.099999999"),
      (268, "90000000.900000000"),(269, "90000000.900000001"),(270, "90000000.999999998"),(271, "90000000.999999999"),(272, "90000001.000000000"),(273, "90000001.000000001"),(274, "90000001.000000009"),(275, "90000001.099999999"),(276, "90000001.900000000"),(277, "90000001.900000001"),
      (278, "90000001.999999998"),(279, "90000001.999999999"),(280, "99999998.000000000"),(281, "99999998.000000001"),(282, "99999998.000000009"),(283, "99999998.099999999"),(284, "99999998.900000000"),(285, "99999998.900000001"),(286, "99999998.999999998"),(287, "99999998.999999999"),
      (288, "99999999.000000000"),(289, "99999999.000000001"),(290, "99999999.000000009"),(291, "99999999.099999999"),(292, "99999999.900000000"),(293, "99999999.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_17 values (294, "0.00000000000000000"),(295, "0.00000000000000001"),(296, "0.00000000000000009"),(297, "0.09999999999999999"),(298, "0.90000000000000000"),(299, "0.90000000000000001"),(300, "0.99999999999999998"),(301, "0.99999999999999999"),(302, "8.00000000000000000"),(303, "8.00000000000000001"),
      (304, "8.00000000000000009"),(305, "8.09999999999999999"),(306, "8.90000000000000000"),(307, "8.90000000000000001"),(308, "8.99999999999999998"),(309, "8.99999999999999999"),(310, "9.00000000000000000"),(311, "9.00000000000000001"),(312, "9.00000000000000009"),(313, "9.09999999999999999"),
      (314, "9.90000000000000000"),(315, "9.90000000000000001"),(316, "9.99999999999999998"),(317, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal64_18_18 values (318, "0.000000000000000000"),(319, "0.000000000000000001"),(320, "0.000000000000000009"),(321, "0.099999999999999999"),(322, "0.900000000000000000"),(323, "0.900000000000000001"),(324, "0.999999999999999998"),(325, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal64_18_18 order by 1;'

}