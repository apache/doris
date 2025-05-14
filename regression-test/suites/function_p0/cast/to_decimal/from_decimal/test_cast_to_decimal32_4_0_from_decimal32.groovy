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


suite("test_cast_to_decimal32_4_0_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_4_0 values (7, "0"),(8, "999"),(9, "9000"),(10, "9001"),(11, "9998"),(12, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_4_1 values (13, "0.0"),(14, "0.1"),(15, "0.8"),(16, "0.9"),(17, "99.0"),(18, "99.1"),(19, "99.8"),(20, "99.9"),(21, "900.0"),(22, "900.1"),
      (23, "900.8"),(24, "900.9"),(25, "901.0"),(26, "901.1"),(27, "901.8"),(28, "901.9"),(29, "998.0"),(30, "998.1"),(31, "998.8"),(32, "998.9"),
      (33, "999.0"),(34, "999.1"),(35, "999.8"),(36, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_4_2 values (37, "0.00"),(38, "0.01"),(39, "0.09"),(40, "0.90"),(41, "0.91"),(42, "0.98"),(43, "0.99"),(44, "9.00"),(45, "9.01"),(46, "9.09"),
      (47, "9.90"),(48, "9.91"),(49, "9.98"),(50, "9.99"),(51, "90.00"),(52, "90.01"),(53, "90.09"),(54, "90.90"),(55, "90.91"),(56, "90.98"),
      (57, "90.99"),(58, "91.00"),(59, "91.01"),(60, "91.09"),(61, "91.90"),(62, "91.91"),(63, "91.98"),(64, "91.99"),(65, "98.00"),(66, "98.01"),
      (67, "98.09"),(68, "98.90"),(69, "98.91"),(70, "98.98"),(71, "98.99"),(72, "99.00"),(73, "99.01"),(74, "99.09"),(75, "99.90"),(76, "99.91"),
      (77, "99.98"),(78, "99.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_4_3 values (79, "0.000"),(80, "0.001"),(81, "0.009"),(82, "0.099"),(83, "0.900"),(84, "0.901"),(85, "0.998"),(86, "0.999"),(87, "8.000"),(88, "8.001"),
      (89, "8.009"),(90, "8.099"),(91, "8.900"),(92, "8.901"),(93, "8.998"),(94, "8.999"),(95, "9.000"),(96, "9.001"),(97, "9.009"),(98, "9.099"),
      (99, "9.900"),(100, "9.901"),(101, "9.998"),(102, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_4_4 values (103, "0.0000"),(104, "0.0001"),(105, "0.0009"),(106, "0.0999"),(107, "0.9000"),(108, "0.9001"),(109, "0.9998"),(110, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_0 values (111, "0"),(112, "999"),(113, "9000"),(114, "9001"),(115, "9998"),(116, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_1 values (117, "0.0"),(118, "0.1"),(119, "0.8"),(120, "0.9"),(121, "999.0"),(122, "999.1"),(123, "999.8"),(124, "999.9"),(125, "9000.0"),(126, "9000.1"),
      (127, "9000.8"),(128, "9000.9"),(129, "9001.0"),(130, "9001.1"),(131, "9001.8"),(132, "9001.9"),(133, "9998.0"),(134, "9998.1"),(135, "9998.8"),(136, "9998.9"),
      (137, "9999.0"),(138, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_4 values (139, "0.0000"),(140, "0.0001"),(141, "0.0009"),(142, "0.0999"),(143, "0.9000"),(144, "0.9001"),(145, "0.9998"),(146, "0.9999"),(147, "999.0000"),(148, "999.0001"),
      (149, "999.0009"),(150, "999.0999"),(151, "999.9000"),(152, "999.9001"),(153, "999.9998"),(154, "999.9999"),(155, "9000.0000"),(156, "9000.0001"),(157, "9000.0009"),(158, "9000.0999"),
      (159, "9000.9000"),(160, "9000.9001"),(161, "9000.9998"),(162, "9000.9999"),(163, "9001.0000"),(164, "9001.0001"),(165, "9001.0009"),(166, "9001.0999"),(167, "9001.9000"),(168, "9001.9001"),
      (169, "9001.9998"),(170, "9001.9999"),(171, "9998.0000"),(172, "9998.0001"),(173, "9998.0009"),(174, "9998.0999"),(175, "9998.9000"),(176, "9998.9001"),(177, "9998.9998"),(178, "9998.9999"),
      (179, "9999.0000"),(180, "9999.0001"),(181, "9999.0009"),(182, "9999.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_7 values (183, "0.0000000"),(184, "0.0000001"),(185, "0.0000009"),(186, "0.0999999"),(187, "0.9000000"),(188, "0.9000001"),(189, "0.9999998"),(190, "0.9999999"),(191, "8.0000000"),(192, "8.0000001"),
      (193, "8.0000009"),(194, "8.0999999"),(195, "8.9000000"),(196, "8.9000001"),(197, "8.9999998"),(198, "8.9999999"),(199, "9.0000000"),(200, "9.0000001"),(201, "9.0000009"),(202, "9.0999999"),
      (203, "9.9000000"),(204, "9.9000001"),(205, "9.9999998"),(206, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_8_8 values (207, "0.00000000"),(208, "0.00000001"),(209, "0.00000009"),(210, "0.09999999"),(211, "0.90000000"),(212, "0.90000001"),(213, "0.99999998"),(214, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_0 values (215, "0"),(216, "999"),(217, "9000"),(218, "9001"),(219, "9998"),(220, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_1 values (221, "0.0"),(222, "0.1"),(223, "0.8"),(224, "0.9"),(225, "999.0"),(226, "999.1"),(227, "999.8"),(228, "999.9"),(229, "9000.0"),(230, "9000.1"),
      (231, "9000.8"),(232, "9000.9"),(233, "9001.0"),(234, "9001.1"),(235, "9001.8"),(236, "9001.9"),(237, "9998.0"),(238, "9998.1"),(239, "9998.8"),(240, "9998.9"),
      (241, "9999.0"),(242, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_4 values (243, "0.0000"),(244, "0.0001"),(245, "0.0009"),(246, "0.0999"),(247, "0.9000"),(248, "0.9001"),(249, "0.9998"),(250, "0.9999"),(251, "999.0000"),(252, "999.0001"),
      (253, "999.0009"),(254, "999.0999"),(255, "999.9000"),(256, "999.9001"),(257, "999.9998"),(258, "999.9999"),(259, "9000.0000"),(260, "9000.0001"),(261, "9000.0009"),(262, "9000.0999"),
      (263, "9000.9000"),(264, "9000.9001"),(265, "9000.9998"),(266, "9000.9999"),(267, "9001.0000"),(268, "9001.0001"),(269, "9001.0009"),(270, "9001.0999"),(271, "9001.9000"),(272, "9001.9001"),
      (273, "9001.9998"),(274, "9001.9999"),(275, "9998.0000"),(276, "9998.0001"),(277, "9998.0009"),(278, "9998.0999"),(279, "9998.9000"),(280, "9998.9001"),(281, "9998.9998"),(282, "9998.9999"),
      (283, "9999.0000"),(284, "9999.0001"),(285, "9999.0009"),(286, "9999.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_8 values (287, "0.00000000"),(288, "0.00000001"),(289, "0.00000009"),(290, "0.09999999"),(291, "0.90000000"),(292, "0.90000001"),(293, "0.99999998"),(294, "0.99999999"),(295, "8.00000000"),(296, "8.00000001"),
      (297, "8.00000009"),(298, "8.09999999"),(299, "8.90000000"),(300, "8.90000001"),(301, "8.99999998"),(302, "8.99999999"),(303, "9.00000000"),(304, "9.00000001"),(305, "9.00000009"),(306, "9.09999999"),
      (307, "9.90000000"),(308, "9.90000001"),(309, "9.99999998"),(310, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal32_9_9 values (311, "0.000000000"),(312, "0.000000001"),(313, "0.000000009"),(314, "0.099999999"),(315, "0.900000000"),(316, "0.900000001"),(317, "0.999999998"),(318, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal32_9_9 order by 1;'

}