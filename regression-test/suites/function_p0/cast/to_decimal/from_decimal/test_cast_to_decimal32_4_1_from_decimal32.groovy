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


suite("test_cast_to_decimal32_4_1_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_0 values (7, "0"),(8, "99"),(9, "900"),(10, "901"),(11, "998"),(12, "999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_1 values (13, "0.0"),(14, "0.1"),(15, "0.8"),(16, "0.9"),(17, "99.0"),(18, "99.1"),(19, "99.8"),(20, "99.9"),(21, "900.0"),(22, "900.1"),
      (23, "900.8"),(24, "900.9"),(25, "901.0"),(26, "901.1"),(27, "901.8"),(28, "901.9"),(29, "998.0"),(30, "998.1"),(31, "998.8"),(32, "998.9"),
      (33, "999.0"),(34, "999.1"),(35, "999.8"),(36, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_2 values (37, "0.00"),(38, "0.01"),(39, "0.09"),(40, "0.90"),(41, "0.91"),(42, "0.98"),(43, "0.99"),(44, "9.00"),(45, "9.01"),(46, "9.09"),
      (47, "9.90"),(48, "9.91"),(49, "9.98"),(50, "9.99"),(51, "90.00"),(52, "90.01"),(53, "90.09"),(54, "90.90"),(55, "90.91"),(56, "90.98"),
      (57, "90.99"),(58, "91.00"),(59, "91.01"),(60, "91.09"),(61, "91.90"),(62, "91.91"),(63, "91.98"),(64, "91.99"),(65, "98.00"),(66, "98.01"),
      (67, "98.09"),(68, "98.90"),(69, "98.91"),(70, "98.98"),(71, "98.99"),(72, "99.00"),(73, "99.01"),(74, "99.09"),(75, "99.90"),(76, "99.91"),
      (77, "99.98"),(78, "99.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_3 values (79, "0.000"),(80, "0.001"),(81, "0.009"),(82, "0.099"),(83, "0.900"),(84, "0.901"),(85, "0.998"),(86, "0.999"),(87, "8.000"),(88, "8.001"),
      (89, "8.009"),(90, "8.099"),(91, "8.900"),(92, "8.901"),(93, "8.998"),(94, "8.999"),(95, "9.000"),(96, "9.001"),(97, "9.009"),(98, "9.099"),
      (99, "9.900"),(100, "9.901"),(101, "9.998"),(102, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_4_4 values (103, "0.0000"),(104, "0.0001"),(105, "0.0009"),(106, "0.0999"),(107, "0.9000"),(108, "0.9001"),(109, "0.9998"),(110, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_0 values (111, "0"),(112, "99"),(113, "900"),(114, "901"),(115, "998"),(116, "999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_1 values (117, "0.0"),(118, "0.1"),(119, "0.8"),(120, "0.9"),(121, "99.0"),(122, "99.1"),(123, "99.8"),(124, "99.9"),(125, "900.0"),(126, "900.1"),
      (127, "900.8"),(128, "900.9"),(129, "901.0"),(130, "901.1"),(131, "901.8"),(132, "901.9"),(133, "998.0"),(134, "998.1"),(135, "998.8"),(136, "998.9"),
      (137, "999.0"),(138, "999.1"),(139, "999.8"),(140, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_4 values (141, "0.0000"),(142, "0.0001"),(143, "0.0009"),(144, "0.0999"),(145, "0.9000"),(146, "0.9001"),(147, "0.9998"),(148, "0.9999"),(149, "99.0000"),(150, "99.0001"),
      (151, "99.0009"),(152, "99.0999"),(153, "99.9000"),(154, "99.9001"),(155, "99.9998"),(156, "99.9999"),(157, "900.0000"),(158, "900.0001"),(159, "900.0009"),(160, "900.0999"),
      (161, "900.9000"),(162, "900.9001"),(163, "900.9998"),(164, "900.9999"),(165, "901.0000"),(166, "901.0001"),(167, "901.0009"),(168, "901.0999"),(169, "901.9000"),(170, "901.9001"),
      (171, "901.9998"),(172, "901.9999"),(173, "998.0000"),(174, "998.0001"),(175, "998.0009"),(176, "998.0999"),(177, "998.9000"),(178, "998.9001"),(179, "998.9998"),(180, "998.9999"),
      (181, "999.0000"),(182, "999.0001"),(183, "999.0009"),(184, "999.0999"),(185, "999.9000"),(186, "999.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_7 values (187, "0.0000000"),(188, "0.0000001"),(189, "0.0000009"),(190, "0.0999999"),(191, "0.9000000"),(192, "0.9000001"),(193, "0.9999998"),(194, "0.9999999"),(195, "8.0000000"),(196, "8.0000001"),
      (197, "8.0000009"),(198, "8.0999999"),(199, "8.9000000"),(200, "8.9000001"),(201, "8.9999998"),(202, "8.9999999"),(203, "9.0000000"),(204, "9.0000001"),(205, "9.0000009"),(206, "9.0999999"),
      (207, "9.9000000"),(208, "9.9000001"),(209, "9.9999998"),(210, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_8_8 values (211, "0.00000000"),(212, "0.00000001"),(213, "0.00000009"),(214, "0.09999999"),(215, "0.90000000"),(216, "0.90000001"),(217, "0.99999998"),(218, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_0 values (219, "0"),(220, "99"),(221, "900"),(222, "901"),(223, "998"),(224, "999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_1 values (225, "0.0"),(226, "0.1"),(227, "0.8"),(228, "0.9"),(229, "99.0"),(230, "99.1"),(231, "99.8"),(232, "99.9"),(233, "900.0"),(234, "900.1"),
      (235, "900.8"),(236, "900.9"),(237, "901.0"),(238, "901.1"),(239, "901.8"),(240, "901.9"),(241, "998.0"),(242, "998.1"),(243, "998.8"),(244, "998.9"),
      (245, "999.0"),(246, "999.1"),(247, "999.8"),(248, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_4 values (249, "0.0000"),(250, "0.0001"),(251, "0.0009"),(252, "0.0999"),(253, "0.9000"),(254, "0.9001"),(255, "0.9998"),(256, "0.9999"),(257, "99.0000"),(258, "99.0001"),
      (259, "99.0009"),(260, "99.0999"),(261, "99.9000"),(262, "99.9001"),(263, "99.9998"),(264, "99.9999"),(265, "900.0000"),(266, "900.0001"),(267, "900.0009"),(268, "900.0999"),
      (269, "900.9000"),(270, "900.9001"),(271, "900.9998"),(272, "900.9999"),(273, "901.0000"),(274, "901.0001"),(275, "901.0009"),(276, "901.0999"),(277, "901.9000"),(278, "901.9001"),
      (279, "901.9998"),(280, "901.9999"),(281, "998.0000"),(282, "998.0001"),(283, "998.0009"),(284, "998.0999"),(285, "998.9000"),(286, "998.9001"),(287, "998.9998"),(288, "998.9999"),
      (289, "999.0000"),(290, "999.0001"),(291, "999.0009"),(292, "999.0999"),(293, "999.9000"),(294, "999.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_8 values (295, "0.00000000"),(296, "0.00000001"),(297, "0.00000009"),(298, "0.09999999"),(299, "0.90000000"),(300, "0.90000001"),(301, "0.99999998"),(302, "0.99999999"),(303, "8.00000000"),(304, "8.00000001"),
      (305, "8.00000009"),(306, "8.09999999"),(307, "8.90000000"),(308, "8.90000001"),(309, "8.99999998"),(310, "8.99999999"),(311, "9.00000000"),(312, "9.00000001"),(313, "9.00000009"),(314, "9.09999999"),
      (315, "9.90000000"),(316, "9.90000001"),(317, "9.99999998"),(318, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_1_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_4_1_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_1_from_decimal32_9_9 values (319, "0.000000000"),(320, "0.000000001"),(321, "0.000000009"),(322, "0.099999999"),(323, "0.900000000"),(324, "0.900000001"),(325, "0.999999998"),(326, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 1)) from test_cast_to_decimal32_4_1_from_decimal32_9_9 order by 1;'

}