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


suite("test_cast_to_decimal32_8_4_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_19_0 values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_19_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "999.0"),(11, "999.1"),(12, "999.8"),(13, "999.9"),(14, "9000.0"),(15, "9000.1"),
      (16, "9000.8"),(17, "9000.9"),(18, "9001.0"),(19, "9001.1"),(20, "9001.8"),(21, "9001.9"),(22, "9998.0"),(23, "9998.1"),(24, "9998.8"),(25, "9998.9"),
      (26, "9999.0"),(27, "9999.1"),(28, "9999.8"),(29, "9999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_19_9 values (30, "0.000000000"),(31, "0.000000001"),(32, "0.000000009"),(33, "0.099999999"),(34, "0.900000000"),(35, "0.900000001"),(36, "0.999999998"),(37, "0.999999999"),(38, "999.000000000"),(39, "999.000000001"),
      (40, "999.000000009"),(41, "999.099999999"),(42, "999.900000000"),(43, "999.900000001"),(44, "999.999999998"),(45, "999.999999999"),(46, "9000.000000000"),(47, "9000.000000001"),(48, "9000.000000009"),(49, "9000.099999999"),
      (50, "9000.900000000"),(51, "9000.900000001"),(52, "9000.999999998"),(53, "9000.999999999"),(54, "9001.000000000"),(55, "9001.000000001"),(56, "9001.000000009"),(57, "9001.099999999"),(58, "9001.900000000"),(59, "9001.900000001"),
      (60, "9001.999999998"),(61, "9001.999999999"),(62, "9998.000000000"),(63, "9998.000000001"),(64, "9998.000000009"),(65, "9998.099999999"),(66, "9998.900000000"),(67, "9998.900000001"),(68, "9998.999999998"),(69, "9998.999999999"),
      (70, "9999.000000000"),(71, "9999.000000001"),(72, "9999.000000009"),(73, "9999.099999999"),(74, "9999.900000000"),(75, "9999.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_19_18 values (76, "0.000000000000000000"),(77, "0.000000000000000001"),(78, "0.000000000000000009"),(79, "0.099999999999999999"),(80, "0.900000000000000000"),(81, "0.900000000000000001"),(82, "0.999999999999999998"),(83, "0.999999999999999999"),(84, "8.000000000000000000"),(85, "8.000000000000000001"),
      (86, "8.000000000000000009"),(87, "8.099999999999999999"),(88, "8.900000000000000000"),(89, "8.900000000000000001"),(90, "8.999999999999999998"),(91, "8.999999999999999999"),(92, "9.000000000000000000"),(93, "9.000000000000000001"),(94, "9.000000000000000009"),(95, "9.099999999999999999"),
      (96, "9.900000000000000000"),(97, "9.900000000000000001"),(98, "9.999999999999999998"),(99, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_19_19 values (100, "0.0000000000000000000"),(101, "0.0000000000000000001"),(102, "0.0000000000000000009"),(103, "0.0999999999999999999"),(104, "0.9000000000000000000"),(105, "0.9000000000000000001"),(106, "0.9999999999999999998"),(107, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_37_0 values (108, "0"),(109, "999"),(110, "9000"),(111, "9001"),(112, "9998"),(113, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_37_1 values (114, "0.0"),(115, "0.1"),(116, "0.8"),(117, "0.9"),(118, "999.0"),(119, "999.1"),(120, "999.8"),(121, "999.9"),(122, "9000.0"),(123, "9000.1"),
      (124, "9000.8"),(125, "9000.9"),(126, "9001.0"),(127, "9001.1"),(128, "9001.8"),(129, "9001.9"),(130, "9998.0"),(131, "9998.1"),(132, "9998.8"),(133, "9998.9"),
      (134, "9999.0"),(135, "9999.1"),(136, "9999.8"),(137, "9999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_37_18 values (138, "0.000000000000000000"),(139, "0.000000000000000001"),(140, "0.000000000000000009"),(141, "0.099999999999999999"),(142, "0.900000000000000000"),(143, "0.900000000000000001"),(144, "0.999999999999999998"),(145, "0.999999999999999999"),(146, "999.000000000000000000"),(147, "999.000000000000000001"),
      (148, "999.000000000000000009"),(149, "999.099999999999999999"),(150, "999.900000000000000000"),(151, "999.900000000000000001"),(152, "999.999999999999999998"),(153, "999.999999999999999999"),(154, "9000.000000000000000000"),(155, "9000.000000000000000001"),(156, "9000.000000000000000009"),(157, "9000.099999999999999999"),
      (158, "9000.900000000000000000"),(159, "9000.900000000000000001"),(160, "9000.999999999999999998"),(161, "9000.999999999999999999"),(162, "9001.000000000000000000"),(163, "9001.000000000000000001"),(164, "9001.000000000000000009"),(165, "9001.099999999999999999"),(166, "9001.900000000000000000"),(167, "9001.900000000000000001"),
      (168, "9001.999999999999999998"),(169, "9001.999999999999999999"),(170, "9998.000000000000000000"),(171, "9998.000000000000000001"),(172, "9998.000000000000000009"),(173, "9998.099999999999999999"),(174, "9998.900000000000000000"),(175, "9998.900000000000000001"),(176, "9998.999999999999999998"),(177, "9998.999999999999999999"),
      (178, "9999.000000000000000000"),(179, "9999.000000000000000001"),(180, "9999.000000000000000009"),(181, "9999.099999999999999999"),(182, "9999.900000000000000000"),(183, "9999.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_37_36 values (184, "0.000000000000000000000000000000000000"),(185, "0.000000000000000000000000000000000001"),(186, "0.000000000000000000000000000000000009"),(187, "0.099999999999999999999999999999999999"),(188, "0.900000000000000000000000000000000000"),(189, "0.900000000000000000000000000000000001"),(190, "0.999999999999999999999999999999999998"),(191, "0.999999999999999999999999999999999999"),(192, "8.000000000000000000000000000000000000"),(193, "8.000000000000000000000000000000000001"),
      (194, "8.000000000000000000000000000000000009"),(195, "8.099999999999999999999999999999999999"),(196, "8.900000000000000000000000000000000000"),(197, "8.900000000000000000000000000000000001"),(198, "8.999999999999999999999999999999999998"),(199, "8.999999999999999999999999999999999999"),(200, "9.000000000000000000000000000000000000"),(201, "9.000000000000000000000000000000000001"),(202, "9.000000000000000000000000000000000009"),(203, "9.099999999999999999999999999999999999"),
      (204, "9.900000000000000000000000000000000000"),(205, "9.900000000000000000000000000000000001"),(206, "9.999999999999999999999999999999999998"),(207, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_37_37 values (208, "0.0000000000000000000000000000000000000"),(209, "0.0000000000000000000000000000000000001"),(210, "0.0000000000000000000000000000000000009"),(211, "0.0999999999999999999999999999999999999"),(212, "0.9000000000000000000000000000000000000"),(213, "0.9000000000000000000000000000000000001"),(214, "0.9999999999999999999999999999999999998"),(215, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_38_0 values (216, "0"),(217, "999"),(218, "9000"),(219, "9001"),(220, "9998"),(221, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_38_1 values (222, "0.0"),(223, "0.1"),(224, "0.8"),(225, "0.9"),(226, "999.0"),(227, "999.1"),(228, "999.8"),(229, "999.9"),(230, "9000.0"),(231, "9000.1"),
      (232, "9000.8"),(233, "9000.9"),(234, "9001.0"),(235, "9001.1"),(236, "9001.8"),(237, "9001.9"),(238, "9998.0"),(239, "9998.1"),(240, "9998.8"),(241, "9998.9"),
      (242, "9999.0"),(243, "9999.1"),(244, "9999.8"),(245, "9999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_38_19 values (246, "0.0000000000000000000"),(247, "0.0000000000000000001"),(248, "0.0000000000000000009"),(249, "0.0999999999999999999"),(250, "0.9000000000000000000"),(251, "0.9000000000000000001"),(252, "0.9999999999999999998"),(253, "0.9999999999999999999"),(254, "999.0000000000000000000"),(255, "999.0000000000000000001"),
      (256, "999.0000000000000000009"),(257, "999.0999999999999999999"),(258, "999.9000000000000000000"),(259, "999.9000000000000000001"),(260, "999.9999999999999999998"),(261, "999.9999999999999999999"),(262, "9000.0000000000000000000"),(263, "9000.0000000000000000001"),(264, "9000.0000000000000000009"),(265, "9000.0999999999999999999"),
      (266, "9000.9000000000000000000"),(267, "9000.9000000000000000001"),(268, "9000.9999999999999999998"),(269, "9000.9999999999999999999"),(270, "9001.0000000000000000000"),(271, "9001.0000000000000000001"),(272, "9001.0000000000000000009"),(273, "9001.0999999999999999999"),(274, "9001.9000000000000000000"),(275, "9001.9000000000000000001"),
      (276, "9001.9999999999999999998"),(277, "9001.9999999999999999999"),(278, "9998.0000000000000000000"),(279, "9998.0000000000000000001"),(280, "9998.0000000000000000009"),(281, "9998.0999999999999999999"),(282, "9998.9000000000000000000"),(283, "9998.9000000000000000001"),(284, "9998.9999999999999999998"),(285, "9998.9999999999999999999"),
      (286, "9999.0000000000000000000"),(287, "9999.0000000000000000001"),(288, "9999.0000000000000000009"),(289, "9999.0999999999999999999"),(290, "9999.9000000000000000000"),(291, "9999.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_38_37 values (292, "0.0000000000000000000000000000000000000"),(293, "0.0000000000000000000000000000000000001"),(294, "0.0000000000000000000000000000000000009"),(295, "0.0999999999999999999999999999999999999"),(296, "0.9000000000000000000000000000000000000"),(297, "0.9000000000000000000000000000000000001"),(298, "0.9999999999999999999999999999999999998"),(299, "0.9999999999999999999999999999999999999"),(300, "8.0000000000000000000000000000000000000"),(301, "8.0000000000000000000000000000000000001"),
      (302, "8.0000000000000000000000000000000000009"),(303, "8.0999999999999999999999999999999999999"),(304, "8.9000000000000000000000000000000000000"),(305, "8.9000000000000000000000000000000000001"),(306, "8.9999999999999999999999999999999999998"),(307, "8.9999999999999999999999999999999999999"),(308, "9.0000000000000000000000000000000000000"),(309, "9.0000000000000000000000000000000000001"),(310, "9.0000000000000000000000000000000000009"),(311, "9.0999999999999999999999999999999999999"),
      (312, "9.9000000000000000000000000000000000000"),(313, "9.9000000000000000000000000000000000001"),(314, "9.9999999999999999999999999999999999998"),(315, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_4_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal32_8_4_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_4_from_decimal128i_38_38 values (316, "0.00000000000000000000000000000000000000"),(317, "0.00000000000000000000000000000000000001"),(318, "0.00000000000000000000000000000000000009"),(319, "0.09999999999999999999999999999999999999"),(320, "0.90000000000000000000000000000000000000"),(321, "0.90000000000000000000000000000000000001"),(322, "0.99999999999999999999999999999999999998"),(323, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 4)) from test_cast_to_decimal32_8_4_from_decimal128i_38_38 order by 1;'

}