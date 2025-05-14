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


suite("test_cast_to_decimal32_4_0_from_decimal128v3") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_19_0 values (0, "0"),(1, "999"),(2, "9000"),(3, "9001"),(4, "9998"),(5, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_19_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "999.0"),(11, "999.1"),(12, "999.8"),(13, "999.9"),(14, "9000.0"),(15, "9000.1"),
      (16, "9000.8"),(17, "9000.9"),(18, "9001.0"),(19, "9001.1"),(20, "9001.8"),(21, "9001.9"),(22, "9998.0"),(23, "9998.1"),(24, "9998.8"),(25, "9998.9"),
      (26, "9999.0"),(27, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_19_9 values (28, "0.000000000"),(29, "0.000000001"),(30, "0.000000009"),(31, "0.099999999"),(32, "0.900000000"),(33, "0.900000001"),(34, "0.999999998"),(35, "0.999999999"),(36, "999.000000000"),(37, "999.000000001"),
      (38, "999.000000009"),(39, "999.099999999"),(40, "999.900000000"),(41, "999.900000001"),(42, "999.999999998"),(43, "999.999999999"),(44, "9000.000000000"),(45, "9000.000000001"),(46, "9000.000000009"),(47, "9000.099999999"),
      (48, "9000.900000000"),(49, "9000.900000001"),(50, "9000.999999998"),(51, "9000.999999999"),(52, "9001.000000000"),(53, "9001.000000001"),(54, "9001.000000009"),(55, "9001.099999999"),(56, "9001.900000000"),(57, "9001.900000001"),
      (58, "9001.999999998"),(59, "9001.999999999"),(60, "9998.000000000"),(61, "9998.000000001"),(62, "9998.000000009"),(63, "9998.099999999"),(64, "9998.900000000"),(65, "9998.900000001"),(66, "9998.999999998"),(67, "9998.999999999"),
      (68, "9999.000000000"),(69, "9999.000000001"),(70, "9999.000000009"),(71, "9999.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_19_18 values (72, "0.000000000000000000"),(73, "0.000000000000000001"),(74, "0.000000000000000009"),(75, "0.099999999999999999"),(76, "0.900000000000000000"),(77, "0.900000000000000001"),(78, "0.999999999999999998"),(79, "0.999999999999999999"),(80, "8.000000000000000000"),(81, "8.000000000000000001"),
      (82, "8.000000000000000009"),(83, "8.099999999999999999"),(84, "8.900000000000000000"),(85, "8.900000000000000001"),(86, "8.999999999999999998"),(87, "8.999999999999999999"),(88, "9.000000000000000000"),(89, "9.000000000000000001"),(90, "9.000000000000000009"),(91, "9.099999999999999999"),
      (92, "9.900000000000000000"),(93, "9.900000000000000001"),(94, "9.999999999999999998"),(95, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_19_19 values (96, "0.0000000000000000000"),(97, "0.0000000000000000001"),(98, "0.0000000000000000009"),(99, "0.0999999999999999999"),(100, "0.9000000000000000000"),(101, "0.9000000000000000001"),(102, "0.9999999999999999998"),(103, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_37_0 values (104, "0"),(105, "999"),(106, "9000"),(107, "9001"),(108, "9998"),(109, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_37_1 values (110, "0.0"),(111, "0.1"),(112, "0.8"),(113, "0.9"),(114, "999.0"),(115, "999.1"),(116, "999.8"),(117, "999.9"),(118, "9000.0"),(119, "9000.1"),
      (120, "9000.8"),(121, "9000.9"),(122, "9001.0"),(123, "9001.1"),(124, "9001.8"),(125, "9001.9"),(126, "9998.0"),(127, "9998.1"),(128, "9998.8"),(129, "9998.9"),
      (130, "9999.0"),(131, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_37_18 values (132, "0.000000000000000000"),(133, "0.000000000000000001"),(134, "0.000000000000000009"),(135, "0.099999999999999999"),(136, "0.900000000000000000"),(137, "0.900000000000000001"),(138, "0.999999999999999998"),(139, "0.999999999999999999"),(140, "999.000000000000000000"),(141, "999.000000000000000001"),
      (142, "999.000000000000000009"),(143, "999.099999999999999999"),(144, "999.900000000000000000"),(145, "999.900000000000000001"),(146, "999.999999999999999998"),(147, "999.999999999999999999"),(148, "9000.000000000000000000"),(149, "9000.000000000000000001"),(150, "9000.000000000000000009"),(151, "9000.099999999999999999"),
      (152, "9000.900000000000000000"),(153, "9000.900000000000000001"),(154, "9000.999999999999999998"),(155, "9000.999999999999999999"),(156, "9001.000000000000000000"),(157, "9001.000000000000000001"),(158, "9001.000000000000000009"),(159, "9001.099999999999999999"),(160, "9001.900000000000000000"),(161, "9001.900000000000000001"),
      (162, "9001.999999999999999998"),(163, "9001.999999999999999999"),(164, "9998.000000000000000000"),(165, "9998.000000000000000001"),(166, "9998.000000000000000009"),(167, "9998.099999999999999999"),(168, "9998.900000000000000000"),(169, "9998.900000000000000001"),(170, "9998.999999999999999998"),(171, "9998.999999999999999999"),
      (172, "9999.000000000000000000"),(173, "9999.000000000000000001"),(174, "9999.000000000000000009"),(175, "9999.099999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_37_36 values (176, "0.000000000000000000000000000000000000"),(177, "0.000000000000000000000000000000000001"),(178, "0.000000000000000000000000000000000009"),(179, "0.099999999999999999999999999999999999"),(180, "0.900000000000000000000000000000000000"),(181, "0.900000000000000000000000000000000001"),(182, "0.999999999999999999999999999999999998"),(183, "0.999999999999999999999999999999999999"),(184, "8.000000000000000000000000000000000000"),(185, "8.000000000000000000000000000000000001"),
      (186, "8.000000000000000000000000000000000009"),(187, "8.099999999999999999999999999999999999"),(188, "8.900000000000000000000000000000000000"),(189, "8.900000000000000000000000000000000001"),(190, "8.999999999999999999999999999999999998"),(191, "8.999999999999999999999999999999999999"),(192, "9.000000000000000000000000000000000000"),(193, "9.000000000000000000000000000000000001"),(194, "9.000000000000000000000000000000000009"),(195, "9.099999999999999999999999999999999999"),
      (196, "9.900000000000000000000000000000000000"),(197, "9.900000000000000000000000000000000001"),(198, "9.999999999999999999999999999999999998"),(199, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_37_37 values (200, "0.0000000000000000000000000000000000000"),(201, "0.0000000000000000000000000000000000001"),(202, "0.0000000000000000000000000000000000009"),(203, "0.0999999999999999999999999999999999999"),(204, "0.9000000000000000000000000000000000000"),(205, "0.9000000000000000000000000000000000001"),(206, "0.9999999999999999999999999999999999998"),(207, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_38_0 values (208, "0"),(209, "999"),(210, "9000"),(211, "9001"),(212, "9998"),(213, "9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_38_1 values (214, "0.0"),(215, "0.1"),(216, "0.8"),(217, "0.9"),(218, "999.0"),(219, "999.1"),(220, "999.8"),(221, "999.9"),(222, "9000.0"),(223, "9000.1"),
      (224, "9000.8"),(225, "9000.9"),(226, "9001.0"),(227, "9001.1"),(228, "9001.8"),(229, "9001.9"),(230, "9998.0"),(231, "9998.1"),(232, "9998.8"),(233, "9998.9"),
      (234, "9999.0"),(235, "9999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_38_19 values (236, "0.0000000000000000000"),(237, "0.0000000000000000001"),(238, "0.0000000000000000009"),(239, "0.0999999999999999999"),(240, "0.9000000000000000000"),(241, "0.9000000000000000001"),(242, "0.9999999999999999998"),(243, "0.9999999999999999999"),(244, "999.0000000000000000000"),(245, "999.0000000000000000001"),
      (246, "999.0000000000000000009"),(247, "999.0999999999999999999"),(248, "999.9000000000000000000"),(249, "999.9000000000000000001"),(250, "999.9999999999999999998"),(251, "999.9999999999999999999"),(252, "9000.0000000000000000000"),(253, "9000.0000000000000000001"),(254, "9000.0000000000000000009"),(255, "9000.0999999999999999999"),
      (256, "9000.9000000000000000000"),(257, "9000.9000000000000000001"),(258, "9000.9999999999999999998"),(259, "9000.9999999999999999999"),(260, "9001.0000000000000000000"),(261, "9001.0000000000000000001"),(262, "9001.0000000000000000009"),(263, "9001.0999999999999999999"),(264, "9001.9000000000000000000"),(265, "9001.9000000000000000001"),
      (266, "9001.9999999999999999998"),(267, "9001.9999999999999999999"),(268, "9998.0000000000000000000"),(269, "9998.0000000000000000001"),(270, "9998.0000000000000000009"),(271, "9998.0999999999999999999"),(272, "9998.9000000000000000000"),(273, "9998.9000000000000000001"),(274, "9998.9999999999999999998"),(275, "9998.9999999999999999999"),
      (276, "9999.0000000000000000000"),(277, "9999.0000000000000000001"),(278, "9999.0000000000000000009"),(279, "9999.0999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_38_37 values (280, "0.0000000000000000000000000000000000000"),(281, "0.0000000000000000000000000000000000001"),(282, "0.0000000000000000000000000000000000009"),(283, "0.0999999999999999999999999999999999999"),(284, "0.9000000000000000000000000000000000000"),(285, "0.9000000000000000000000000000000000001"),(286, "0.9999999999999999999999999999999999998"),(287, "0.9999999999999999999999999999999999999"),(288, "8.0000000000000000000000000000000000000"),(289, "8.0000000000000000000000000000000000001"),
      (290, "8.0000000000000000000000000000000000009"),(291, "8.0999999999999999999999999999999999999"),(292, "8.9000000000000000000000000000000000000"),(293, "8.9000000000000000000000000000000000001"),(294, "8.9999999999999999999999999999999999998"),(295, "8.9999999999999999999999999999999999999"),(296, "9.0000000000000000000000000000000000000"),(297, "9.0000000000000000000000000000000000001"),(298, "9.0000000000000000000000000000000000009"),(299, "9.0999999999999999999999999999999999999"),
      (300, "9.9000000000000000000000000000000000000"),(301, "9.9000000000000000000000000000000000001"),(302, "9.9999999999999999999999999999999999998"),(303, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal32_4_0_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_decimal128v3_38_38 values (304, "0.00000000000000000000000000000000000000"),(305, "0.00000000000000000000000000000000000001"),(306, "0.00000000000000000000000000000000000009"),(307, "0.09999999999999999999999999999999999999"),(308, "0.90000000000000000000000000000000000000"),(309, "0.90000000000000000000000000000000000001"),(310, "0.99999999999999999999999999999999999998"),(311, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_decimal128v3_38_38 order by 1;'

}