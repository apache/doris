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


suite("test_cast_to_decimal128v3_37_1_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_10_0 values (0, "0"),(1, "999999999"),(2, "9000000000"),(3, "9000000001"),(4, "9999999998"),(5, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_10_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "99999999.0"),(11, "99999999.1"),(12, "99999999.8"),(13, "99999999.9"),(14, "900000000.0"),(15, "900000000.1"),
      (16, "900000000.8"),(17, "900000000.9"),(18, "900000001.0"),(19, "900000001.1"),(20, "900000001.8"),(21, "900000001.9"),(22, "999999998.0"),(23, "999999998.1"),(24, "999999998.8"),(25, "999999998.9"),
      (26, "999999999.0"),(27, "999999999.1"),(28, "999999999.8"),(29, "999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_10_5 values (30, "0.00000"),(31, "0.00001"),(32, "0.00009"),(33, "0.09999"),(34, "0.90000"),(35, "0.90001"),(36, "0.99998"),(37, "0.99999"),(38, "9999.00000"),(39, "9999.00001"),
      (40, "9999.00009"),(41, "9999.09999"),(42, "9999.90000"),(43, "9999.90001"),(44, "9999.99998"),(45, "9999.99999"),(46, "90000.00000"),(47, "90000.00001"),(48, "90000.00009"),(49, "90000.09999"),
      (50, "90000.90000"),(51, "90000.90001"),(52, "90000.99998"),(53, "90000.99999"),(54, "90001.00000"),(55, "90001.00001"),(56, "90001.00009"),(57, "90001.09999"),(58, "90001.90000"),(59, "90001.90001"),
      (60, "90001.99998"),(61, "90001.99999"),(62, "99998.00000"),(63, "99998.00001"),(64, "99998.00009"),(65, "99998.09999"),(66, "99998.90000"),(67, "99998.90001"),(68, "99998.99998"),(69, "99998.99999"),
      (70, "99999.00000"),(71, "99999.00001"),(72, "99999.00009"),(73, "99999.09999"),(74, "99999.90000"),(75, "99999.90001"),(76, "99999.99998"),(77, "99999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_10_9 values (78, "0.000000000"),(79, "0.000000001"),(80, "0.000000009"),(81, "0.099999999"),(82, "0.900000000"),(83, "0.900000001"),(84, "0.999999998"),(85, "0.999999999"),(86, "8.000000000"),(87, "8.000000001"),
      (88, "8.000000009"),(89, "8.099999999"),(90, "8.900000000"),(91, "8.900000001"),(92, "8.999999998"),(93, "8.999999999"),(94, "9.000000000"),(95, "9.000000001"),(96, "9.000000009"),(97, "9.099999999"),
      (98, "9.900000000"),(99, "9.900000001"),(100, "9.999999998"),(101, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_10_10 values (102, "0.0000000000"),(103, "0.0000000001"),(104, "0.0000000009"),(105, "0.0999999999"),(106, "0.9000000000"),(107, "0.9000000001"),(108, "0.9999999998"),(109, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_17_0 values (110, "0"),(111, "9999999999999999"),(112, "90000000000000000"),(113, "90000000000000001"),(114, "99999999999999998"),(115, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_17_1 values (116, "0.0"),(117, "0.1"),(118, "0.8"),(119, "0.9"),(120, "999999999999999.0"),(121, "999999999999999.1"),(122, "999999999999999.8"),(123, "999999999999999.9"),(124, "9000000000000000.0"),(125, "9000000000000000.1"),
      (126, "9000000000000000.8"),(127, "9000000000000000.9"),(128, "9000000000000001.0"),(129, "9000000000000001.1"),(130, "9000000000000001.8"),(131, "9000000000000001.9"),(132, "9999999999999998.0"),(133, "9999999999999998.1"),(134, "9999999999999998.8"),(135, "9999999999999998.9"),
      (136, "9999999999999999.0"),(137, "9999999999999999.1"),(138, "9999999999999999.8"),(139, "9999999999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_17_8 values (140, "0.00000000"),(141, "0.00000001"),(142, "0.00000009"),(143, "0.09999999"),(144, "0.90000000"),(145, "0.90000001"),(146, "0.99999998"),(147, "0.99999999"),(148, "99999999.00000000"),(149, "99999999.00000001"),
      (150, "99999999.00000009"),(151, "99999999.09999999"),(152, "99999999.90000000"),(153, "99999999.90000001"),(154, "99999999.99999998"),(155, "99999999.99999999"),(156, "900000000.00000000"),(157, "900000000.00000001"),(158, "900000000.00000009"),(159, "900000000.09999999"),
      (160, "900000000.90000000"),(161, "900000000.90000001"),(162, "900000000.99999998"),(163, "900000000.99999999"),(164, "900000001.00000000"),(165, "900000001.00000001"),(166, "900000001.00000009"),(167, "900000001.09999999"),(168, "900000001.90000000"),(169, "900000001.90000001"),
      (170, "900000001.99999998"),(171, "900000001.99999999"),(172, "999999998.00000000"),(173, "999999998.00000001"),(174, "999999998.00000009"),(175, "999999998.09999999"),(176, "999999998.90000000"),(177, "999999998.90000001"),(178, "999999998.99999998"),(179, "999999998.99999999"),
      (180, "999999999.00000000"),(181, "999999999.00000001"),(182, "999999999.00000009"),(183, "999999999.09999999"),(184, "999999999.90000000"),(185, "999999999.90000001"),(186, "999999999.99999998"),(187, "999999999.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_17_16 values (188, "0.0000000000000000"),(189, "0.0000000000000001"),(190, "0.0000000000000009"),(191, "0.0999999999999999"),(192, "0.9000000000000000"),(193, "0.9000000000000001"),(194, "0.9999999999999998"),(195, "0.9999999999999999"),(196, "8.0000000000000000"),(197, "8.0000000000000001"),
      (198, "8.0000000000000009"),(199, "8.0999999999999999"),(200, "8.9000000000000000"),(201, "8.9000000000000001"),(202, "8.9999999999999998"),(203, "8.9999999999999999"),(204, "9.0000000000000000"),(205, "9.0000000000000001"),(206, "9.0000000000000009"),(207, "9.0999999999999999"),
      (208, "9.9000000000000000"),(209, "9.9000000000000001"),(210, "9.9999999999999998"),(211, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_17_17 values (212, "0.00000000000000000"),(213, "0.00000000000000001"),(214, "0.00000000000000009"),(215, "0.09999999999999999"),(216, "0.90000000000000000"),(217, "0.90000000000000001"),(218, "0.99999999999999998"),(219, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_18_0 values (220, "0"),(221, "99999999999999999"),(222, "900000000000000000"),(223, "900000000000000001"),(224, "999999999999999998"),(225, "999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_18_1 values (226, "0.0"),(227, "0.1"),(228, "0.8"),(229, "0.9"),(230, "9999999999999999.0"),(231, "9999999999999999.1"),(232, "9999999999999999.8"),(233, "9999999999999999.9"),(234, "90000000000000000.0"),(235, "90000000000000000.1"),
      (236, "90000000000000000.8"),(237, "90000000000000000.9"),(238, "90000000000000001.0"),(239, "90000000000000001.1"),(240, "90000000000000001.8"),(241, "90000000000000001.9"),(242, "99999999999999998.0"),(243, "99999999999999998.1"),(244, "99999999999999998.8"),(245, "99999999999999998.9"),
      (246, "99999999999999999.0"),(247, "99999999999999999.1"),(248, "99999999999999999.8"),(249, "99999999999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_18_9 values (250, "0.000000000"),(251, "0.000000001"),(252, "0.000000009"),(253, "0.099999999"),(254, "0.900000000"),(255, "0.900000001"),(256, "0.999999998"),(257, "0.999999999"),(258, "99999999.000000000"),(259, "99999999.000000001"),
      (260, "99999999.000000009"),(261, "99999999.099999999"),(262, "99999999.900000000"),(263, "99999999.900000001"),(264, "99999999.999999998"),(265, "99999999.999999999"),(266, "900000000.000000000"),(267, "900000000.000000001"),(268, "900000000.000000009"),(269, "900000000.099999999"),
      (270, "900000000.900000000"),(271, "900000000.900000001"),(272, "900000000.999999998"),(273, "900000000.999999999"),(274, "900000001.000000000"),(275, "900000001.000000001"),(276, "900000001.000000009"),(277, "900000001.099999999"),(278, "900000001.900000000"),(279, "900000001.900000001"),
      (280, "900000001.999999998"),(281, "900000001.999999999"),(282, "999999998.000000000"),(283, "999999998.000000001"),(284, "999999998.000000009"),(285, "999999998.099999999"),(286, "999999998.900000000"),(287, "999999998.900000001"),(288, "999999998.999999998"),(289, "999999998.999999999"),
      (290, "999999999.000000000"),(291, "999999999.000000001"),(292, "999999999.000000009"),(293, "999999999.099999999"),(294, "999999999.900000000"),(295, "999999999.900000001"),(296, "999999999.999999998"),(297, "999999999.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_18_17 values (298, "0.00000000000000000"),(299, "0.00000000000000001"),(300, "0.00000000000000009"),(301, "0.09999999999999999"),(302, "0.90000000000000000"),(303, "0.90000000000000001"),(304, "0.99999999999999998"),(305, "0.99999999999999999"),(306, "8.00000000000000000"),(307, "8.00000000000000001"),
      (308, "8.00000000000000009"),(309, "8.09999999999999999"),(310, "8.90000000000000000"),(311, "8.90000000000000001"),(312, "8.99999999999999998"),(313, "8.99999999999999999"),(314, "9.00000000000000000"),(315, "9.00000000000000001"),(316, "9.00000000000000009"),(317, "9.09999999999999999"),
      (318, "9.90000000000000000"),(319, "9.90000000000000001"),(320, "9.99999999999999998"),(321, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_37_1_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal128v3_37_1_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_37_1_from_decimal64_18_18 values (322, "0.000000000000000000"),(323, "0.000000000000000001"),(324, "0.000000000000000009"),(325, "0.099999999999999999"),(326, "0.900000000000000000"),(327, "0.900000000000000001"),(328, "0.999999999999999998"),(329, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(37, 1)) from test_cast_to_decimal128v3_37_1_from_decimal64_18_18 order by 1;'

}