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


suite("test_cast_to_decimal64_10_0_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_10_0 values (0, "0"),(1, "999999999"),(2, "9000000000"),(3, "9000000001"),(4, "9999999998"),(5, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_10_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "99999999.0"),(11, "99999999.1"),(12, "99999999.8"),(13, "99999999.9"),(14, "900000000.0"),(15, "900000000.1"),
      (16, "900000000.8"),(17, "900000000.9"),(18, "900000001.0"),(19, "900000001.1"),(20, "900000001.8"),(21, "900000001.9"),(22, "999999998.0"),(23, "999999998.1"),(24, "999999998.8"),(25, "999999998.9"),
      (26, "999999999.0"),(27, "999999999.1"),(28, "999999999.8"),(29, "999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_10_5 values (30, "0.00000"),(31, "0.00001"),(32, "0.00009"),(33, "0.09999"),(34, "0.90000"),(35, "0.90001"),(36, "0.99998"),(37, "0.99999"),(38, "9999.00000"),(39, "9999.00001"),
      (40, "9999.00009"),(41, "9999.09999"),(42, "9999.90000"),(43, "9999.90001"),(44, "9999.99998"),(45, "9999.99999"),(46, "90000.00000"),(47, "90000.00001"),(48, "90000.00009"),(49, "90000.09999"),
      (50, "90000.90000"),(51, "90000.90001"),(52, "90000.99998"),(53, "90000.99999"),(54, "90001.00000"),(55, "90001.00001"),(56, "90001.00009"),(57, "90001.09999"),(58, "90001.90000"),(59, "90001.90001"),
      (60, "90001.99998"),(61, "90001.99999"),(62, "99998.00000"),(63, "99998.00001"),(64, "99998.00009"),(65, "99998.09999"),(66, "99998.90000"),(67, "99998.90001"),(68, "99998.99998"),(69, "99998.99999"),
      (70, "99999.00000"),(71, "99999.00001"),(72, "99999.00009"),(73, "99999.09999"),(74, "99999.90000"),(75, "99999.90001"),(76, "99999.99998"),(77, "99999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_10_9 values (78, "0.000000000"),(79, "0.000000001"),(80, "0.000000009"),(81, "0.099999999"),(82, "0.900000000"),(83, "0.900000001"),(84, "0.999999998"),(85, "0.999999999"),(86, "8.000000000"),(87, "8.000000001"),
      (88, "8.000000009"),(89, "8.099999999"),(90, "8.900000000"),(91, "8.900000001"),(92, "8.999999998"),(93, "8.999999999"),(94, "9.000000000"),(95, "9.000000001"),(96, "9.000000009"),(97, "9.099999999"),
      (98, "9.900000000"),(99, "9.900000001"),(100, "9.999999998"),(101, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_10_10 values (102, "0.0000000000"),(103, "0.0000000001"),(104, "0.0000000009"),(105, "0.0999999999"),(106, "0.9000000000"),(107, "0.9000000001"),(108, "0.9999999998"),(109, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_0 values (110, "0"),(111, "999999999"),(112, "9000000000"),(113, "9000000001"),(114, "9999999998"),(115, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_1 values (116, "0.0"),(117, "0.1"),(118, "0.8"),(119, "0.9"),(120, "999999999.0"),(121, "999999999.1"),(122, "999999999.8"),(123, "999999999.9"),(124, "9000000000.0"),(125, "9000000000.1"),
      (126, "9000000000.8"),(127, "9000000000.9"),(128, "9000000001.0"),(129, "9000000001.1"),(130, "9000000001.8"),(131, "9000000001.9"),(132, "9999999998.0"),(133, "9999999998.1"),(134, "9999999998.8"),(135, "9999999998.9"),
      (136, "9999999999.0"),(137, "9999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_8 values (138, "0.00000000"),(139, "0.00000001"),(140, "0.00000009"),(141, "0.09999999"),(142, "0.90000000"),(143, "0.90000001"),(144, "0.99999998"),(145, "0.99999999"),(146, "99999999.00000000"),(147, "99999999.00000001"),
      (148, "99999999.00000009"),(149, "99999999.09999999"),(150, "99999999.90000000"),(151, "99999999.90000001"),(152, "99999999.99999998"),(153, "99999999.99999999"),(154, "900000000.00000000"),(155, "900000000.00000001"),(156, "900000000.00000009"),(157, "900000000.09999999"),
      (158, "900000000.90000000"),(159, "900000000.90000001"),(160, "900000000.99999998"),(161, "900000000.99999999"),(162, "900000001.00000000"),(163, "900000001.00000001"),(164, "900000001.00000009"),(165, "900000001.09999999"),(166, "900000001.90000000"),(167, "900000001.90000001"),
      (168, "900000001.99999998"),(169, "900000001.99999999"),(170, "999999998.00000000"),(171, "999999998.00000001"),(172, "999999998.00000009"),(173, "999999998.09999999"),(174, "999999998.90000000"),(175, "999999998.90000001"),(176, "999999998.99999998"),(177, "999999998.99999999"),
      (178, "999999999.00000000"),(179, "999999999.00000001"),(180, "999999999.00000009"),(181, "999999999.09999999"),(182, "999999999.90000000"),(183, "999999999.90000001"),(184, "999999999.99999998"),(185, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_16 values (186, "0.0000000000000000"),(187, "0.0000000000000001"),(188, "0.0000000000000009"),(189, "0.0999999999999999"),(190, "0.9000000000000000"),(191, "0.9000000000000001"),(192, "0.9999999999999998"),(193, "0.9999999999999999"),(194, "8.0000000000000000"),(195, "8.0000000000000001"),
      (196, "8.0000000000000009"),(197, "8.0999999999999999"),(198, "8.9000000000000000"),(199, "8.9000000000000001"),(200, "8.9999999999999998"),(201, "8.9999999999999999"),(202, "9.0000000000000000"),(203, "9.0000000000000001"),(204, "9.0000000000000009"),(205, "9.0999999999999999"),
      (206, "9.9000000000000000"),(207, "9.9000000000000001"),(208, "9.9999999999999998"),(209, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_17_17 values (210, "0.00000000000000000"),(211, "0.00000000000000001"),(212, "0.00000000000000009"),(213, "0.09999999999999999"),(214, "0.90000000000000000"),(215, "0.90000000000000001"),(216, "0.99999999999999998"),(217, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_0 values (218, "0"),(219, "999999999"),(220, "9000000000"),(221, "9000000001"),(222, "9999999998"),(223, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_1 values (224, "0.0"),(225, "0.1"),(226, "0.8"),(227, "0.9"),(228, "999999999.0"),(229, "999999999.1"),(230, "999999999.8"),(231, "999999999.9"),(232, "9000000000.0"),(233, "9000000000.1"),
      (234, "9000000000.8"),(235, "9000000000.9"),(236, "9000000001.0"),(237, "9000000001.1"),(238, "9000000001.8"),(239, "9000000001.9"),(240, "9999999998.0"),(241, "9999999998.1"),(242, "9999999998.8"),(243, "9999999998.9"),
      (244, "9999999999.0"),(245, "9999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_9 values (246, "0.000000000"),(247, "0.000000001"),(248, "0.000000009"),(249, "0.099999999"),(250, "0.900000000"),(251, "0.900000001"),(252, "0.999999998"),(253, "0.999999999"),(254, "99999999.000000000"),(255, "99999999.000000001"),
      (256, "99999999.000000009"),(257, "99999999.099999999"),(258, "99999999.900000000"),(259, "99999999.900000001"),(260, "99999999.999999998"),(261, "99999999.999999999"),(262, "900000000.000000000"),(263, "900000000.000000001"),(264, "900000000.000000009"),(265, "900000000.099999999"),
      (266, "900000000.900000000"),(267, "900000000.900000001"),(268, "900000000.999999998"),(269, "900000000.999999999"),(270, "900000001.000000000"),(271, "900000001.000000001"),(272, "900000001.000000009"),(273, "900000001.099999999"),(274, "900000001.900000000"),(275, "900000001.900000001"),
      (276, "900000001.999999998"),(277, "900000001.999999999"),(278, "999999998.000000000"),(279, "999999998.000000001"),(280, "999999998.000000009"),(281, "999999998.099999999"),(282, "999999998.900000000"),(283, "999999998.900000001"),(284, "999999998.999999998"),(285, "999999998.999999999"),
      (286, "999999999.000000000"),(287, "999999999.000000001"),(288, "999999999.000000009"),(289, "999999999.099999999"),(290, "999999999.900000000"),(291, "999999999.900000001"),(292, "999999999.999999998"),(293, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_17 values (294, "0.00000000000000000"),(295, "0.00000000000000001"),(296, "0.00000000000000009"),(297, "0.09999999999999999"),(298, "0.90000000000000000"),(299, "0.90000000000000001"),(300, "0.99999999999999998"),(301, "0.99999999999999999"),(302, "8.00000000000000000"),(303, "8.00000000000000001"),
      (304, "8.00000000000000009"),(305, "8.09999999999999999"),(306, "8.90000000000000000"),(307, "8.90000000000000001"),(308, "8.99999999999999998"),(309, "8.99999999999999999"),(310, "9.00000000000000000"),(311, "9.00000000000000001"),(312, "9.00000000000000009"),(313, "9.09999999999999999"),
      (314, "9.90000000000000000"),(315, "9.90000000000000001"),(316, "9.99999999999999998"),(317, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_10_0_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal64_10_0_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_10_0_from_decimal64_18_18 values (318, "0.000000000000000000"),(319, "0.000000000000000001"),(320, "0.000000000000000009"),(321, "0.099999999999999999"),(322, "0.900000000000000000"),(323, "0.900000000000000001"),(324, "0.999999999999999998"),(325, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(10, 0)) from test_cast_to_decimal64_10_0_from_decimal64_18_18 order by 1;'

}