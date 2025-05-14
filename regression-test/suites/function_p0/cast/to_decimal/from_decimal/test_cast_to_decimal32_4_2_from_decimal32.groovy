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


suite("test_cast_to_decimal32_4_2_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_0 values (7, "0"),(8, "9"),(9, "90"),(10, "91"),(11, "98"),(12, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_1 values (13, "0.0"),(14, "0.1"),(15, "0.8"),(16, "0.9"),(17, "9.0"),(18, "9.1"),(19, "9.8"),(20, "9.9"),(21, "90.0"),(22, "90.1"),
      (23, "90.8"),(24, "90.9"),(25, "91.0"),(26, "91.1"),(27, "91.8"),(28, "91.9"),(29, "98.0"),(30, "98.1"),(31, "98.8"),(32, "98.9"),
      (33, "99.0"),(34, "99.1"),(35, "99.8"),(36, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_2 values (37, "0.00"),(38, "0.01"),(39, "0.09"),(40, "0.90"),(41, "0.91"),(42, "0.98"),(43, "0.99"),(44, "9.00"),(45, "9.01"),(46, "9.09"),
      (47, "9.90"),(48, "9.91"),(49, "9.98"),(50, "9.99"),(51, "90.00"),(52, "90.01"),(53, "90.09"),(54, "90.90"),(55, "90.91"),(56, "90.98"),
      (57, "90.99"),(58, "91.00"),(59, "91.01"),(60, "91.09"),(61, "91.90"),(62, "91.91"),(63, "91.98"),(64, "91.99"),(65, "98.00"),(66, "98.01"),
      (67, "98.09"),(68, "98.90"),(69, "98.91"),(70, "98.98"),(71, "98.99"),(72, "99.00"),(73, "99.01"),(74, "99.09"),(75, "99.90"),(76, "99.91"),
      (77, "99.98"),(78, "99.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_3 values (79, "0.000"),(80, "0.001"),(81, "0.009"),(82, "0.099"),(83, "0.900"),(84, "0.901"),(85, "0.998"),(86, "0.999"),(87, "8.000"),(88, "8.001"),
      (89, "8.009"),(90, "8.099"),(91, "8.900"),(92, "8.901"),(93, "8.998"),(94, "8.999"),(95, "9.000"),(96, "9.001"),(97, "9.009"),(98, "9.099"),
      (99, "9.900"),(100, "9.901"),(101, "9.998"),(102, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_4_4 values (103, "0.0000"),(104, "0.0001"),(105, "0.0009"),(106, "0.0999"),(107, "0.9000"),(108, "0.9001"),(109, "0.9998"),(110, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_0 values (111, "0"),(112, "9"),(113, "90"),(114, "91"),(115, "98"),(116, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_1 values (117, "0.0"),(118, "0.1"),(119, "0.8"),(120, "0.9"),(121, "9.0"),(122, "9.1"),(123, "9.8"),(124, "9.9"),(125, "90.0"),(126, "90.1"),
      (127, "90.8"),(128, "90.9"),(129, "91.0"),(130, "91.1"),(131, "91.8"),(132, "91.9"),(133, "98.0"),(134, "98.1"),(135, "98.8"),(136, "98.9"),
      (137, "99.0"),(138, "99.1"),(139, "99.8"),(140, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_4 values (141, "0.0000"),(142, "0.0001"),(143, "0.0009"),(144, "0.0999"),(145, "0.9000"),(146, "0.9001"),(147, "0.9998"),(148, "0.9999"),(149, "9.0000"),(150, "9.0001"),
      (151, "9.0009"),(152, "9.0999"),(153, "9.9000"),(154, "9.9001"),(155, "9.9998"),(156, "9.9999"),(157, "90.0000"),(158, "90.0001"),(159, "90.0009"),(160, "90.0999"),
      (161, "90.9000"),(162, "90.9001"),(163, "90.9998"),(164, "90.9999"),(165, "91.0000"),(166, "91.0001"),(167, "91.0009"),(168, "91.0999"),(169, "91.9000"),(170, "91.9001"),
      (171, "91.9998"),(172, "91.9999"),(173, "98.0000"),(174, "98.0001"),(175, "98.0009"),(176, "98.0999"),(177, "98.9000"),(178, "98.9001"),(179, "98.9998"),(180, "98.9999"),
      (181, "99.0000"),(182, "99.0001"),(183, "99.0009"),(184, "99.0999"),(185, "99.9000"),(186, "99.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_7 values (187, "0.0000000"),(188, "0.0000001"),(189, "0.0000009"),(190, "0.0999999"),(191, "0.9000000"),(192, "0.9000001"),(193, "0.9999998"),(194, "0.9999999"),(195, "8.0000000"),(196, "8.0000001"),
      (197, "8.0000009"),(198, "8.0999999"),(199, "8.9000000"),(200, "8.9000001"),(201, "8.9999998"),(202, "8.9999999"),(203, "9.0000000"),(204, "9.0000001"),(205, "9.0000009"),(206, "9.0999999"),
      (207, "9.9000000"),(208, "9.9000001"),(209, "9.9999998"),(210, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_8_8 values (211, "0.00000000"),(212, "0.00000001"),(213, "0.00000009"),(214, "0.09999999"),(215, "0.90000000"),(216, "0.90000001"),(217, "0.99999998"),(218, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_0 values (219, "0"),(220, "9"),(221, "90"),(222, "91"),(223, "98"),(224, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_1 values (225, "0.0"),(226, "0.1"),(227, "0.8"),(228, "0.9"),(229, "9.0"),(230, "9.1"),(231, "9.8"),(232, "9.9"),(233, "90.0"),(234, "90.1"),
      (235, "90.8"),(236, "90.9"),(237, "91.0"),(238, "91.1"),(239, "91.8"),(240, "91.9"),(241, "98.0"),(242, "98.1"),(243, "98.8"),(244, "98.9"),
      (245, "99.0"),(246, "99.1"),(247, "99.8"),(248, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_4 values (249, "0.0000"),(250, "0.0001"),(251, "0.0009"),(252, "0.0999"),(253, "0.9000"),(254, "0.9001"),(255, "0.9998"),(256, "0.9999"),(257, "9.0000"),(258, "9.0001"),
      (259, "9.0009"),(260, "9.0999"),(261, "9.9000"),(262, "9.9001"),(263, "9.9998"),(264, "9.9999"),(265, "90.0000"),(266, "90.0001"),(267, "90.0009"),(268, "90.0999"),
      (269, "90.9000"),(270, "90.9001"),(271, "90.9998"),(272, "90.9999"),(273, "91.0000"),(274, "91.0001"),(275, "91.0009"),(276, "91.0999"),(277, "91.9000"),(278, "91.9001"),
      (279, "91.9998"),(280, "91.9999"),(281, "98.0000"),(282, "98.0001"),(283, "98.0009"),(284, "98.0999"),(285, "98.9000"),(286, "98.9001"),(287, "98.9998"),(288, "98.9999"),
      (289, "99.0000"),(290, "99.0001"),(291, "99.0009"),(292, "99.0999"),(293, "99.9000"),(294, "99.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_8 values (295, "0.00000000"),(296, "0.00000001"),(297, "0.00000009"),(298, "0.09999999"),(299, "0.90000000"),(300, "0.90000001"),(301, "0.99999998"),(302, "0.99999999"),(303, "8.00000000"),(304, "8.00000001"),
      (305, "8.00000009"),(306, "8.09999999"),(307, "8.90000000"),(308, "8.90000001"),(309, "8.99999998"),(310, "8.99999999"),(311, "9.00000000"),(312, "9.00000001"),(313, "9.00000009"),(314, "9.09999999"),
      (315, "9.90000000"),(316, "9.90000001"),(317, "9.99999998"),(318, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal32_9_9 values (319, "0.000000000"),(320, "0.000000001"),(321, "0.000000009"),(322, "0.099999999"),(323, "0.900000000"),(324, "0.900000001"),(325, "0.999999998"),(326, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal32_9_9 order by 1;'

}