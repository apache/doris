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


suite("test_cast_to_decimal32_9_0_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_0 values (0, "0"),(1, "99999999"),(2, "900000000"),(3, "900000001"),(4, "999999998"),(5, "999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "99999999.0"),(11, "99999999.1"),(12, "99999999.8"),(13, "99999999.9"),(14, "900000000.0"),(15, "900000000.1"),
      (16, "900000000.8"),(17, "900000000.9"),(18, "900000001.0"),(19, "900000001.1"),(20, "900000001.8"),(21, "900000001.9"),(22, "999999998.0"),(23, "999999998.1"),(24, "999999998.8"),(25, "999999998.9"),
      (26, "999999999.0"),(27, "999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_5 values (28, "0.00000"),(29, "0.00001"),(30, "0.00009"),(31, "0.09999"),(32, "0.90000"),(33, "0.90001"),(34, "0.99998"),(35, "0.99999"),(36, "9999.00000"),(37, "9999.00001"),
      (38, "9999.00009"),(39, "9999.09999"),(40, "9999.90000"),(41, "9999.90001"),(42, "9999.99998"),(43, "9999.99999"),(44, "90000.00000"),(45, "90000.00001"),(46, "90000.00009"),(47, "90000.09999"),
      (48, "90000.90000"),(49, "90000.90001"),(50, "90000.99998"),(51, "90000.99999"),(52, "90001.00000"),(53, "90001.00001"),(54, "90001.00009"),(55, "90001.09999"),(56, "90001.90000"),(57, "90001.90001"),
      (58, "90001.99998"),(59, "90001.99999"),(60, "99998.00000"),(61, "99998.00001"),(62, "99998.00009"),(63, "99998.09999"),(64, "99998.90000"),(65, "99998.90001"),(66, "99998.99998"),(67, "99998.99999"),
      (68, "99999.00000"),(69, "99999.00001"),(70, "99999.00009"),(71, "99999.09999"),(72, "99999.90000"),(73, "99999.90001"),(74, "99999.99998"),(75, "99999.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_9 values (76, "0.000000000"),(77, "0.000000001"),(78, "0.000000009"),(79, "0.099999999"),(80, "0.900000000"),(81, "0.900000001"),(82, "0.999999998"),(83, "0.999999999"),(84, "8.000000000"),(85, "8.000000001"),
      (86, "8.000000009"),(87, "8.099999999"),(88, "8.900000000"),(89, "8.900000001"),(90, "8.999999998"),(91, "8.999999999"),(92, "9.000000000"),(93, "9.000000001"),(94, "9.000000009"),(95, "9.099999999"),
      (96, "9.900000000"),(97, "9.900000001"),(98, "9.999999998"),(99, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_10_10 values (100, "0.0000000000"),(101, "0.0000000001"),(102, "0.0000000009"),(103, "0.0999999999"),(104, "0.9000000000"),(105, "0.9000000001"),(106, "0.9999999998"),(107, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_0 values (108, "0"),(109, "99999999"),(110, "900000000"),(111, "900000001"),(112, "999999998"),(113, "999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_1 values (114, "0.0"),(115, "0.1"),(116, "0.8"),(117, "0.9"),(118, "99999999.0"),(119, "99999999.1"),(120, "99999999.8"),(121, "99999999.9"),(122, "900000000.0"),(123, "900000000.1"),
      (124, "900000000.8"),(125, "900000000.9"),(126, "900000001.0"),(127, "900000001.1"),(128, "900000001.8"),(129, "900000001.9"),(130, "999999998.0"),(131, "999999998.1"),(132, "999999998.8"),(133, "999999998.9"),
      (134, "999999999.0"),(135, "999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_8 values (136, "0.00000000"),(137, "0.00000001"),(138, "0.00000009"),(139, "0.09999999"),(140, "0.90000000"),(141, "0.90000001"),(142, "0.99999998"),(143, "0.99999999"),(144, "99999999.00000000"),(145, "99999999.00000001"),
      (146, "99999999.00000009"),(147, "99999999.09999999"),(148, "99999999.90000000"),(149, "99999999.90000001"),(150, "99999999.99999998"),(151, "99999999.99999999"),(152, "900000000.00000000"),(153, "900000000.00000001"),(154, "900000000.00000009"),(155, "900000000.09999999"),
      (156, "900000000.90000000"),(157, "900000000.90000001"),(158, "900000000.99999998"),(159, "900000000.99999999"),(160, "900000001.00000000"),(161, "900000001.00000001"),(162, "900000001.00000009"),(163, "900000001.09999999"),(164, "900000001.90000000"),(165, "900000001.90000001"),
      (166, "900000001.99999998"),(167, "900000001.99999999"),(168, "999999998.00000000"),(169, "999999998.00000001"),(170, "999999998.00000009"),(171, "999999998.09999999"),(172, "999999998.90000000"),(173, "999999998.90000001"),(174, "999999998.99999998"),(175, "999999998.99999999"),
      (176, "999999999.00000000"),(177, "999999999.00000001"),(178, "999999999.00000009"),(179, "999999999.09999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_16 values (180, "0.0000000000000000"),(181, "0.0000000000000001"),(182, "0.0000000000000009"),(183, "0.0999999999999999"),(184, "0.9000000000000000"),(185, "0.9000000000000001"),(186, "0.9999999999999998"),(187, "0.9999999999999999"),(188, "8.0000000000000000"),(189, "8.0000000000000001"),
      (190, "8.0000000000000009"),(191, "8.0999999999999999"),(192, "8.9000000000000000"),(193, "8.9000000000000001"),(194, "8.9999999999999998"),(195, "8.9999999999999999"),(196, "9.0000000000000000"),(197, "9.0000000000000001"),(198, "9.0000000000000009"),(199, "9.0999999999999999"),
      (200, "9.9000000000000000"),(201, "9.9000000000000001"),(202, "9.9999999999999998"),(203, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_17_17 values (204, "0.00000000000000000"),(205, "0.00000000000000001"),(206, "0.00000000000000009"),(207, "0.09999999999999999"),(208, "0.90000000000000000"),(209, "0.90000000000000001"),(210, "0.99999999999999998"),(211, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_0 values (212, "0"),(213, "99999999"),(214, "900000000"),(215, "900000001"),(216, "999999998"),(217, "999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_1 values (218, "0.0"),(219, "0.1"),(220, "0.8"),(221, "0.9"),(222, "99999999.0"),(223, "99999999.1"),(224, "99999999.8"),(225, "99999999.9"),(226, "900000000.0"),(227, "900000000.1"),
      (228, "900000000.8"),(229, "900000000.9"),(230, "900000001.0"),(231, "900000001.1"),(232, "900000001.8"),(233, "900000001.9"),(234, "999999998.0"),(235, "999999998.1"),(236, "999999998.8"),(237, "999999998.9"),
      (238, "999999999.0"),(239, "999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_9 values (240, "0.000000000"),(241, "0.000000001"),(242, "0.000000009"),(243, "0.099999999"),(244, "0.900000000"),(245, "0.900000001"),(246, "0.999999998"),(247, "0.999999999"),(248, "99999999.000000000"),(249, "99999999.000000001"),
      (250, "99999999.000000009"),(251, "99999999.099999999"),(252, "99999999.900000000"),(253, "99999999.900000001"),(254, "99999999.999999998"),(255, "99999999.999999999"),(256, "900000000.000000000"),(257, "900000000.000000001"),(258, "900000000.000000009"),(259, "900000000.099999999"),
      (260, "900000000.900000000"),(261, "900000000.900000001"),(262, "900000000.999999998"),(263, "900000000.999999999"),(264, "900000001.000000000"),(265, "900000001.000000001"),(266, "900000001.000000009"),(267, "900000001.099999999"),(268, "900000001.900000000"),(269, "900000001.900000001"),
      (270, "900000001.999999998"),(271, "900000001.999999999"),(272, "999999998.000000000"),(273, "999999998.000000001"),(274, "999999998.000000009"),(275, "999999998.099999999"),(276, "999999998.900000000"),(277, "999999998.900000001"),(278, "999999998.999999998"),(279, "999999998.999999999"),
      (280, "999999999.000000000"),(281, "999999999.000000001"),(282, "999999999.000000009"),(283, "999999999.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_17 values (284, "0.00000000000000000"),(285, "0.00000000000000001"),(286, "0.00000000000000009"),(287, "0.09999999999999999"),(288, "0.90000000000000000"),(289, "0.90000000000000001"),(290, "0.99999999999999998"),(291, "0.99999999999999999"),(292, "8.00000000000000000"),(293, "8.00000000000000001"),
      (294, "8.00000000000000009"),(295, "8.09999999999999999"),(296, "8.90000000000000000"),(297, "8.90000000000000001"),(298, "8.99999999999999998"),(299, "8.99999999999999999"),(300, "9.00000000000000000"),(301, "9.00000000000000001"),(302, "9.00000000000000009"),(303, "9.09999999999999999"),
      (304, "9.90000000000000000"),(305, "9.90000000000000001"),(306, "9.99999999999999998"),(307, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal32_9_0_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_decimal64_18_18 values (308, "0.000000000000000000"),(309, "0.000000000000000001"),(310, "0.000000000000000009"),(311, "0.099999999999999999"),(312, "0.900000000000000000"),(313, "0.900000000000000001"),(314, "0.999999999999999998"),(315, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_decimal64_18_18 order by 1;'

}