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


suite("test_cast_to_decimal32_4_2_from_decimal128v3") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_19_0 values (0, "0"),(1, "9"),(2, "90"),(3, "91"),(4, "98"),(5, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_19_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "9.0"),(11, "9.1"),(12, "9.8"),(13, "9.9"),(14, "90.0"),(15, "90.1"),
      (16, "90.8"),(17, "90.9"),(18, "91.0"),(19, "91.1"),(20, "91.8"),(21, "91.9"),(22, "98.0"),(23, "98.1"),(24, "98.8"),(25, "98.9"),
      (26, "99.0"),(27, "99.1"),(28, "99.8"),(29, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_19_9 values (30, "0.000000000"),(31, "0.000000001"),(32, "0.000000009"),(33, "0.099999999"),(34, "0.900000000"),(35, "0.900000001"),(36, "0.999999998"),(37, "0.999999999"),(38, "9.000000000"),(39, "9.000000001"),
      (40, "9.000000009"),(41, "9.099999999"),(42, "9.900000000"),(43, "9.900000001"),(44, "9.999999998"),(45, "9.999999999"),(46, "90.000000000"),(47, "90.000000001"),(48, "90.000000009"),(49, "90.099999999"),
      (50, "90.900000000"),(51, "90.900000001"),(52, "90.999999998"),(53, "90.999999999"),(54, "91.000000000"),(55, "91.000000001"),(56, "91.000000009"),(57, "91.099999999"),(58, "91.900000000"),(59, "91.900000001"),
      (60, "91.999999998"),(61, "91.999999999"),(62, "98.000000000"),(63, "98.000000001"),(64, "98.000000009"),(65, "98.099999999"),(66, "98.900000000"),(67, "98.900000001"),(68, "98.999999998"),(69, "98.999999999"),
      (70, "99.000000000"),(71, "99.000000001"),(72, "99.000000009"),(73, "99.099999999"),(74, "99.900000000"),(75, "99.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_19_18 values (76, "0.000000000000000000"),(77, "0.000000000000000001"),(78, "0.000000000000000009"),(79, "0.099999999999999999"),(80, "0.900000000000000000"),(81, "0.900000000000000001"),(82, "0.999999999999999998"),(83, "0.999999999999999999"),(84, "8.000000000000000000"),(85, "8.000000000000000001"),
      (86, "8.000000000000000009"),(87, "8.099999999999999999"),(88, "8.900000000000000000"),(89, "8.900000000000000001"),(90, "8.999999999999999998"),(91, "8.999999999999999999"),(92, "9.000000000000000000"),(93, "9.000000000000000001"),(94, "9.000000000000000009"),(95, "9.099999999999999999"),
      (96, "9.900000000000000000"),(97, "9.900000000000000001"),(98, "9.999999999999999998"),(99, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_19_19 values (100, "0.0000000000000000000"),(101, "0.0000000000000000001"),(102, "0.0000000000000000009"),(103, "0.0999999999999999999"),(104, "0.9000000000000000000"),(105, "0.9000000000000000001"),(106, "0.9999999999999999998"),(107, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_37_0 values (108, "0"),(109, "9"),(110, "90"),(111, "91"),(112, "98"),(113, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_37_1 values (114, "0.0"),(115, "0.1"),(116, "0.8"),(117, "0.9"),(118, "9.0"),(119, "9.1"),(120, "9.8"),(121, "9.9"),(122, "90.0"),(123, "90.1"),
      (124, "90.8"),(125, "90.9"),(126, "91.0"),(127, "91.1"),(128, "91.8"),(129, "91.9"),(130, "98.0"),(131, "98.1"),(132, "98.8"),(133, "98.9"),
      (134, "99.0"),(135, "99.1"),(136, "99.8"),(137, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_37_18 values (138, "0.000000000000000000"),(139, "0.000000000000000001"),(140, "0.000000000000000009"),(141, "0.099999999999999999"),(142, "0.900000000000000000"),(143, "0.900000000000000001"),(144, "0.999999999999999998"),(145, "0.999999999999999999"),(146, "9.000000000000000000"),(147, "9.000000000000000001"),
      (148, "9.000000000000000009"),(149, "9.099999999999999999"),(150, "9.900000000000000000"),(151, "9.900000000000000001"),(152, "9.999999999999999998"),(153, "9.999999999999999999"),(154, "90.000000000000000000"),(155, "90.000000000000000001"),(156, "90.000000000000000009"),(157, "90.099999999999999999"),
      (158, "90.900000000000000000"),(159, "90.900000000000000001"),(160, "90.999999999999999998"),(161, "90.999999999999999999"),(162, "91.000000000000000000"),(163, "91.000000000000000001"),(164, "91.000000000000000009"),(165, "91.099999999999999999"),(166, "91.900000000000000000"),(167, "91.900000000000000001"),
      (168, "91.999999999999999998"),(169, "91.999999999999999999"),(170, "98.000000000000000000"),(171, "98.000000000000000001"),(172, "98.000000000000000009"),(173, "98.099999999999999999"),(174, "98.900000000000000000"),(175, "98.900000000000000001"),(176, "98.999999999999999998"),(177, "98.999999999999999999"),
      (178, "99.000000000000000000"),(179, "99.000000000000000001"),(180, "99.000000000000000009"),(181, "99.099999999999999999"),(182, "99.900000000000000000"),(183, "99.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_37_36 values (184, "0.000000000000000000000000000000000000"),(185, "0.000000000000000000000000000000000001"),(186, "0.000000000000000000000000000000000009"),(187, "0.099999999999999999999999999999999999"),(188, "0.900000000000000000000000000000000000"),(189, "0.900000000000000000000000000000000001"),(190, "0.999999999999999999999999999999999998"),(191, "0.999999999999999999999999999999999999"),(192, "8.000000000000000000000000000000000000"),(193, "8.000000000000000000000000000000000001"),
      (194, "8.000000000000000000000000000000000009"),(195, "8.099999999999999999999999999999999999"),(196, "8.900000000000000000000000000000000000"),(197, "8.900000000000000000000000000000000001"),(198, "8.999999999999999999999999999999999998"),(199, "8.999999999999999999999999999999999999"),(200, "9.000000000000000000000000000000000000"),(201, "9.000000000000000000000000000000000001"),(202, "9.000000000000000000000000000000000009"),(203, "9.099999999999999999999999999999999999"),
      (204, "9.900000000000000000000000000000000000"),(205, "9.900000000000000000000000000000000001"),(206, "9.999999999999999999999999999999999998"),(207, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_37_37 values (208, "0.0000000000000000000000000000000000000"),(209, "0.0000000000000000000000000000000000001"),(210, "0.0000000000000000000000000000000000009"),(211, "0.0999999999999999999999999999999999999"),(212, "0.9000000000000000000000000000000000000"),(213, "0.9000000000000000000000000000000000001"),(214, "0.9999999999999999999999999999999999998"),(215, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_38_0 values (216, "0"),(217, "9"),(218, "90"),(219, "91"),(220, "98"),(221, "99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_38_1 values (222, "0.0"),(223, "0.1"),(224, "0.8"),(225, "0.9"),(226, "9.0"),(227, "9.1"),(228, "9.8"),(229, "9.9"),(230, "90.0"),(231, "90.1"),
      (232, "90.8"),(233, "90.9"),(234, "91.0"),(235, "91.1"),(236, "91.8"),(237, "91.9"),(238, "98.0"),(239, "98.1"),(240, "98.8"),(241, "98.9"),
      (242, "99.0"),(243, "99.1"),(244, "99.8"),(245, "99.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_38_19 values (246, "0.0000000000000000000"),(247, "0.0000000000000000001"),(248, "0.0000000000000000009"),(249, "0.0999999999999999999"),(250, "0.9000000000000000000"),(251, "0.9000000000000000001"),(252, "0.9999999999999999998"),(253, "0.9999999999999999999"),(254, "9.0000000000000000000"),(255, "9.0000000000000000001"),
      (256, "9.0000000000000000009"),(257, "9.0999999999999999999"),(258, "9.9000000000000000000"),(259, "9.9000000000000000001"),(260, "9.9999999999999999998"),(261, "9.9999999999999999999"),(262, "90.0000000000000000000"),(263, "90.0000000000000000001"),(264, "90.0000000000000000009"),(265, "90.0999999999999999999"),
      (266, "90.9000000000000000000"),(267, "90.9000000000000000001"),(268, "90.9999999999999999998"),(269, "90.9999999999999999999"),(270, "91.0000000000000000000"),(271, "91.0000000000000000001"),(272, "91.0000000000000000009"),(273, "91.0999999999999999999"),(274, "91.9000000000000000000"),(275, "91.9000000000000000001"),
      (276, "91.9999999999999999998"),(277, "91.9999999999999999999"),(278, "98.0000000000000000000"),(279, "98.0000000000000000001"),(280, "98.0000000000000000009"),(281, "98.0999999999999999999"),(282, "98.9000000000000000000"),(283, "98.9000000000000000001"),(284, "98.9999999999999999998"),(285, "98.9999999999999999999"),
      (286, "99.0000000000000000000"),(287, "99.0000000000000000001"),(288, "99.0000000000000000009"),(289, "99.0999999999999999999"),(290, "99.9000000000000000000"),(291, "99.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_38_37 values (292, "0.0000000000000000000000000000000000000"),(293, "0.0000000000000000000000000000000000001"),(294, "0.0000000000000000000000000000000000009"),(295, "0.0999999999999999999999999999999999999"),(296, "0.9000000000000000000000000000000000000"),(297, "0.9000000000000000000000000000000000001"),(298, "0.9999999999999999999999999999999999998"),(299, "0.9999999999999999999999999999999999999"),(300, "8.0000000000000000000000000000000000000"),(301, "8.0000000000000000000000000000000000001"),
      (302, "8.0000000000000000000000000000000000009"),(303, "8.0999999999999999999999999999999999999"),(304, "8.9000000000000000000000000000000000000"),(305, "8.9000000000000000000000000000000000001"),(306, "8.9999999999999999999999999999999999998"),(307, "8.9999999999999999999999999999999999999"),(308, "9.0000000000000000000000000000000000000"),(309, "9.0000000000000000000000000000000000001"),(310, "9.0000000000000000000000000000000000009"),(311, "9.0999999999999999999999999999999999999"),
      (312, "9.9000000000000000000000000000000000000"),(313, "9.9000000000000000000000000000000000001"),(314, "9.9999999999999999999999999999999999998"),(315, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal32_4_2_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_decimal128v3_38_38 values (316, "0.00000000000000000000000000000000000000"),(317, "0.00000000000000000000000000000000000001"),(318, "0.00000000000000000000000000000000000009"),(319, "0.09999999999999999999999999999999999999"),(320, "0.90000000000000000000000000000000000000"),(321, "0.90000000000000000000000000000000000001"),(322, "0.99999999999999999999999999999999999998"),(323, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_decimal128v3_38_38 order by 1;'

}