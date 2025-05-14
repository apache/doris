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


suite("test_cast_to_decimal32_9_1_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_4_0 values (7, "0"),(8, "999"),(9, "9000"),(10, "9001"),(11, "9998"),(12, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_4_1 values (13, "0.0"),(14, "0.1"),(15, "0.8"),(16, "0.9"),(17, "99.0"),(18, "99.1"),(19, "99.8"),(20, "99.9"),(21, "900.0"),(22, "900.1"),
      (23, "900.8"),(24, "900.9"),(25, "901.0"),(26, "901.1"),(27, "901.8"),(28, "901.9"),(29, "998.0"),(30, "998.1"),(31, "998.8"),(32, "998.9"),
      (33, "999.0"),(34, "999.1"),(35, "999.8"),(36, "999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_4_2 values (37, "0.00"),(38, "0.01"),(39, "0.09"),(40, "0.90"),(41, "0.91"),(42, "0.98"),(43, "0.99"),(44, "9.00"),(45, "9.01"),(46, "9.09"),
      (47, "9.90"),(48, "9.91"),(49, "9.98"),(50, "9.99"),(51, "90.00"),(52, "90.01"),(53, "90.09"),(54, "90.90"),(55, "90.91"),(56, "90.98"),
      (57, "90.99"),(58, "91.00"),(59, "91.01"),(60, "91.09"),(61, "91.90"),(62, "91.91"),(63, "91.98"),(64, "91.99"),(65, "98.00"),(66, "98.01"),
      (67, "98.09"),(68, "98.90"),(69, "98.91"),(70, "98.98"),(71, "98.99"),(72, "99.00"),(73, "99.01"),(74, "99.09"),(75, "99.90"),(76, "99.91"),
      (77, "99.98"),(78, "99.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_4_3 values (79, "0.000"),(80, "0.001"),(81, "0.009"),(82, "0.099"),(83, "0.900"),(84, "0.901"),(85, "0.998"),(86, "0.999"),(87, "8.000"),(88, "8.001"),
      (89, "8.009"),(90, "8.099"),(91, "8.900"),(92, "8.901"),(93, "8.998"),(94, "8.999"),(95, "9.000"),(96, "9.001"),(97, "9.009"),(98, "9.099"),
      (99, "9.900"),(100, "9.901"),(101, "9.998"),(102, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_4_4 values (103, "0.0000"),(104, "0.0001"),(105, "0.0009"),(106, "0.0999"),(107, "0.9000"),(108, "0.9001"),(109, "0.9998"),(110, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_8_0 values (111, "0"),(112, "9999999"),(113, "90000000"),(114, "90000001"),(115, "99999998"),(116, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_8_1 values (117, "0.0"),(118, "0.1"),(119, "0.8"),(120, "0.9"),(121, "999999.0"),(122, "999999.1"),(123, "999999.8"),(124, "999999.9"),(125, "9000000.0"),(126, "9000000.1"),
      (127, "9000000.8"),(128, "9000000.9"),(129, "9000001.0"),(130, "9000001.1"),(131, "9000001.8"),(132, "9000001.9"),(133, "9999998.0"),(134, "9999998.1"),(135, "9999998.8"),(136, "9999998.9"),
      (137, "9999999.0"),(138, "9999999.1"),(139, "9999999.8"),(140, "9999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_8_4 values (141, "0.0000"),(142, "0.0001"),(143, "0.0009"),(144, "0.0999"),(145, "0.9000"),(146, "0.9001"),(147, "0.9998"),(148, "0.9999"),(149, "999.0000"),(150, "999.0001"),
      (151, "999.0009"),(152, "999.0999"),(153, "999.9000"),(154, "999.9001"),(155, "999.9998"),(156, "999.9999"),(157, "9000.0000"),(158, "9000.0001"),(159, "9000.0009"),(160, "9000.0999"),
      (161, "9000.9000"),(162, "9000.9001"),(163, "9000.9998"),(164, "9000.9999"),(165, "9001.0000"),(166, "9001.0001"),(167, "9001.0009"),(168, "9001.0999"),(169, "9001.9000"),(170, "9001.9001"),
      (171, "9001.9998"),(172, "9001.9999"),(173, "9998.0000"),(174, "9998.0001"),(175, "9998.0009"),(176, "9998.0999"),(177, "9998.9000"),(178, "9998.9001"),(179, "9998.9998"),(180, "9998.9999"),
      (181, "9999.0000"),(182, "9999.0001"),(183, "9999.0009"),(184, "9999.0999"),(185, "9999.9000"),(186, "9999.9001"),(187, "9999.9998"),(188, "9999.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_8_7 values (189, "0.0000000"),(190, "0.0000001"),(191, "0.0000009"),(192, "0.0999999"),(193, "0.9000000"),(194, "0.9000001"),(195, "0.9999998"),(196, "0.9999999"),(197, "8.0000000"),(198, "8.0000001"),
      (199, "8.0000009"),(200, "8.0999999"),(201, "8.9000000"),(202, "8.9000001"),(203, "8.9999998"),(204, "8.9999999"),(205, "9.0000000"),(206, "9.0000001"),(207, "9.0000009"),(208, "9.0999999"),
      (209, "9.9000000"),(210, "9.9000001"),(211, "9.9999998"),(212, "9.9999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_8_8 values (213, "0.00000000"),(214, "0.00000001"),(215, "0.00000009"),(216, "0.09999999"),(217, "0.90000000"),(218, "0.90000001"),(219, "0.99999998"),(220, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_0 values (221, "0"),(222, "9999999"),(223, "90000000"),(224, "90000001"),(225, "99999998"),(226, "99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_1 values (227, "0.0"),(228, "0.1"),(229, "0.8"),(230, "0.9"),(231, "9999999.0"),(232, "9999999.1"),(233, "9999999.8"),(234, "9999999.9"),(235, "90000000.0"),(236, "90000000.1"),
      (237, "90000000.8"),(238, "90000000.9"),(239, "90000001.0"),(240, "90000001.1"),(241, "90000001.8"),(242, "90000001.9"),(243, "99999998.0"),(244, "99999998.1"),(245, "99999998.8"),(246, "99999998.9"),
      (247, "99999999.0"),(248, "99999999.1"),(249, "99999999.8"),(250, "99999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_4 values (251, "0.0000"),(252, "0.0001"),(253, "0.0009"),(254, "0.0999"),(255, "0.9000"),(256, "0.9001"),(257, "0.9998"),(258, "0.9999"),(259, "9999.0000"),(260, "9999.0001"),
      (261, "9999.0009"),(262, "9999.0999"),(263, "9999.9000"),(264, "9999.9001"),(265, "9999.9998"),(266, "9999.9999"),(267, "90000.0000"),(268, "90000.0001"),(269, "90000.0009"),(270, "90000.0999"),
      (271, "90000.9000"),(272, "90000.9001"),(273, "90000.9998"),(274, "90000.9999"),(275, "90001.0000"),(276, "90001.0001"),(277, "90001.0009"),(278, "90001.0999"),(279, "90001.9000"),(280, "90001.9001"),
      (281, "90001.9998"),(282, "90001.9999"),(283, "99998.0000"),(284, "99998.0001"),(285, "99998.0009"),(286, "99998.0999"),(287, "99998.9000"),(288, "99998.9001"),(289, "99998.9998"),(290, "99998.9999"),
      (291, "99999.0000"),(292, "99999.0001"),(293, "99999.0009"),(294, "99999.0999"),(295, "99999.9000"),(296, "99999.9001"),(297, "99999.9998"),(298, "99999.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_8 values (299, "0.00000000"),(300, "0.00000001"),(301, "0.00000009"),(302, "0.09999999"),(303, "0.90000000"),(304, "0.90000001"),(305, "0.99999998"),(306, "0.99999999"),(307, "8.00000000"),(308, "8.00000001"),
      (309, "8.00000009"),(310, "8.09999999"),(311, "8.90000000"),(312, "8.90000001"),(313, "8.99999998"),(314, "8.99999999"),(315, "9.00000000"),(316, "9.00000001"),(317, "9.00000009"),(318, "9.09999999"),
      (319, "9.90000000"),(320, "9.90000001"),(321, "9.99999998"),(322, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_1_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_9_1_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_1_from_decimal32_9_9 values (323, "0.000000000"),(324, "0.000000001"),(325, "0.000000009"),(326, "0.099999999"),(327, "0.900000000"),(328, "0.900000001"),(329, "0.999999998"),(330, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(9, 1)) from test_cast_to_decimal32_9_1_from_decimal32_9_9 order by 1;'

}