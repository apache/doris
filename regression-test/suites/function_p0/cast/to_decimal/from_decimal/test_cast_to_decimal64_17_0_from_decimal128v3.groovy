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


suite("test_cast_to_decimal64_17_0_from_decimal128v3") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_19_0 values (0, "0"),(1, "9999999999999999"),(2, "90000000000000000"),(3, "90000000000000001"),(4, "99999999999999998"),(5, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_19_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "9999999999999999.0"),(11, "9999999999999999.1"),(12, "9999999999999999.8"),(13, "9999999999999999.9"),(14, "90000000000000000.0"),(15, "90000000000000000.1"),
      (16, "90000000000000000.8"),(17, "90000000000000000.9"),(18, "90000000000000001.0"),(19, "90000000000000001.1"),(20, "90000000000000001.8"),(21, "90000000000000001.9"),(22, "99999999999999998.0"),(23, "99999999999999998.1"),(24, "99999999999999998.8"),(25, "99999999999999998.9"),
      (26, "99999999999999999.0"),(27, "99999999999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_19_9 values (28, "0.000000000"),(29, "0.000000001"),(30, "0.000000009"),(31, "0.099999999"),(32, "0.900000000"),(33, "0.900000001"),(34, "0.999999998"),(35, "0.999999999"),(36, "999999999.000000000"),(37, "999999999.000000001"),
      (38, "999999999.000000009"),(39, "999999999.099999999"),(40, "999999999.900000000"),(41, "999999999.900000001"),(42, "999999999.999999998"),(43, "999999999.999999999"),(44, "9000000000.000000000"),(45, "9000000000.000000001"),(46, "9000000000.000000009"),(47, "9000000000.099999999"),
      (48, "9000000000.900000000"),(49, "9000000000.900000001"),(50, "9000000000.999999998"),(51, "9000000000.999999999"),(52, "9000000001.000000000"),(53, "9000000001.000000001"),(54, "9000000001.000000009"),(55, "9000000001.099999999"),(56, "9000000001.900000000"),(57, "9000000001.900000001"),
      (58, "9000000001.999999998"),(59, "9000000001.999999999"),(60, "9999999998.000000000"),(61, "9999999998.000000001"),(62, "9999999998.000000009"),(63, "9999999998.099999999"),(64, "9999999998.900000000"),(65, "9999999998.900000001"),(66, "9999999998.999999998"),(67, "9999999998.999999999"),
      (68, "9999999999.000000000"),(69, "9999999999.000000001"),(70, "9999999999.000000009"),(71, "9999999999.099999999"),(72, "9999999999.900000000"),(73, "9999999999.900000001"),(74, "9999999999.999999998"),(75, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_19_18 values (76, "0.000000000000000000"),(77, "0.000000000000000001"),(78, "0.000000000000000009"),(79, "0.099999999999999999"),(80, "0.900000000000000000"),(81, "0.900000000000000001"),(82, "0.999999999999999998"),(83, "0.999999999999999999"),(84, "8.000000000000000000"),(85, "8.000000000000000001"),
      (86, "8.000000000000000009"),(87, "8.099999999999999999"),(88, "8.900000000000000000"),(89, "8.900000000000000001"),(90, "8.999999999999999998"),(91, "8.999999999999999999"),(92, "9.000000000000000000"),(93, "9.000000000000000001"),(94, "9.000000000000000009"),(95, "9.099999999999999999"),
      (96, "9.900000000000000000"),(97, "9.900000000000000001"),(98, "9.999999999999999998"),(99, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_19_19 values (100, "0.0000000000000000000"),(101, "0.0000000000000000001"),(102, "0.0000000000000000009"),(103, "0.0999999999999999999"),(104, "0.9000000000000000000"),(105, "0.9000000000000000001"),(106, "0.9999999999999999998"),(107, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_37_0 values (108, "0"),(109, "9999999999999999"),(110, "90000000000000000"),(111, "90000000000000001"),(112, "99999999999999998"),(113, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_37_1 values (114, "0.0"),(115, "0.1"),(116, "0.8"),(117, "0.9"),(118, "9999999999999999.0"),(119, "9999999999999999.1"),(120, "9999999999999999.8"),(121, "9999999999999999.9"),(122, "90000000000000000.0"),(123, "90000000000000000.1"),
      (124, "90000000000000000.8"),(125, "90000000000000000.9"),(126, "90000000000000001.0"),(127, "90000000000000001.1"),(128, "90000000000000001.8"),(129, "90000000000000001.9"),(130, "99999999999999998.0"),(131, "99999999999999998.1"),(132, "99999999999999998.8"),(133, "99999999999999998.9"),
      (134, "99999999999999999.0"),(135, "99999999999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_37_18 values (136, "0.000000000000000000"),(137, "0.000000000000000001"),(138, "0.000000000000000009"),(139, "0.099999999999999999"),(140, "0.900000000000000000"),(141, "0.900000000000000001"),(142, "0.999999999999999998"),(143, "0.999999999999999999"),(144, "9999999999999999.000000000000000000"),(145, "9999999999999999.000000000000000001"),
      (146, "9999999999999999.000000000000000009"),(147, "9999999999999999.099999999999999999"),(148, "9999999999999999.900000000000000000"),(149, "9999999999999999.900000000000000001"),(150, "9999999999999999.999999999999999998"),(151, "9999999999999999.999999999999999999"),(152, "90000000000000000.000000000000000000"),(153, "90000000000000000.000000000000000001"),(154, "90000000000000000.000000000000000009"),(155, "90000000000000000.099999999999999999"),
      (156, "90000000000000000.900000000000000000"),(157, "90000000000000000.900000000000000001"),(158, "90000000000000000.999999999999999998"),(159, "90000000000000000.999999999999999999"),(160, "90000000000000001.000000000000000000"),(161, "90000000000000001.000000000000000001"),(162, "90000000000000001.000000000000000009"),(163, "90000000000000001.099999999999999999"),(164, "90000000000000001.900000000000000000"),(165, "90000000000000001.900000000000000001"),
      (166, "90000000000000001.999999999999999998"),(167, "90000000000000001.999999999999999999"),(168, "99999999999999998.000000000000000000"),(169, "99999999999999998.000000000000000001"),(170, "99999999999999998.000000000000000009"),(171, "99999999999999998.099999999999999999"),(172, "99999999999999998.900000000000000000"),(173, "99999999999999998.900000000000000001"),(174, "99999999999999998.999999999999999998"),(175, "99999999999999998.999999999999999999"),
      (176, "99999999999999999.000000000000000000"),(177, "99999999999999999.000000000000000001"),(178, "99999999999999999.000000000000000009"),(179, "99999999999999999.099999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_37_36 values (180, "0.000000000000000000000000000000000000"),(181, "0.000000000000000000000000000000000001"),(182, "0.000000000000000000000000000000000009"),(183, "0.099999999999999999999999999999999999"),(184, "0.900000000000000000000000000000000000"),(185, "0.900000000000000000000000000000000001"),(186, "0.999999999999999999999999999999999998"),(187, "0.999999999999999999999999999999999999"),(188, "8.000000000000000000000000000000000000"),(189, "8.000000000000000000000000000000000001"),
      (190, "8.000000000000000000000000000000000009"),(191, "8.099999999999999999999999999999999999"),(192, "8.900000000000000000000000000000000000"),(193, "8.900000000000000000000000000000000001"),(194, "8.999999999999999999999999999999999998"),(195, "8.999999999999999999999999999999999999"),(196, "9.000000000000000000000000000000000000"),(197, "9.000000000000000000000000000000000001"),(198, "9.000000000000000000000000000000000009"),(199, "9.099999999999999999999999999999999999"),
      (200, "9.900000000000000000000000000000000000"),(201, "9.900000000000000000000000000000000001"),(202, "9.999999999999999999999999999999999998"),(203, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_37_37 values (204, "0.0000000000000000000000000000000000000"),(205, "0.0000000000000000000000000000000000001"),(206, "0.0000000000000000000000000000000000009"),(207, "0.0999999999999999999999999999999999999"),(208, "0.9000000000000000000000000000000000000"),(209, "0.9000000000000000000000000000000000001"),(210, "0.9999999999999999999999999999999999998"),(211, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_38_0 values (212, "0"),(213, "9999999999999999"),(214, "90000000000000000"),(215, "90000000000000001"),(216, "99999999999999998"),(217, "99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_38_1 values (218, "0.0"),(219, "0.1"),(220, "0.8"),(221, "0.9"),(222, "9999999999999999.0"),(223, "9999999999999999.1"),(224, "9999999999999999.8"),(225, "9999999999999999.9"),(226, "90000000000000000.0"),(227, "90000000000000000.1"),
      (228, "90000000000000000.8"),(229, "90000000000000000.9"),(230, "90000000000000001.0"),(231, "90000000000000001.1"),(232, "90000000000000001.8"),(233, "90000000000000001.9"),(234, "99999999999999998.0"),(235, "99999999999999998.1"),(236, "99999999999999998.8"),(237, "99999999999999998.9"),
      (238, "99999999999999999.0"),(239, "99999999999999999.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_38_19 values (240, "0.0000000000000000000"),(241, "0.0000000000000000001"),(242, "0.0000000000000000009"),(243, "0.0999999999999999999"),(244, "0.9000000000000000000"),(245, "0.9000000000000000001"),(246, "0.9999999999999999998"),(247, "0.9999999999999999999"),(248, "9999999999999999.0000000000000000000"),(249, "9999999999999999.0000000000000000001"),
      (250, "9999999999999999.0000000000000000009"),(251, "9999999999999999.0999999999999999999"),(252, "9999999999999999.9000000000000000000"),(253, "9999999999999999.9000000000000000001"),(254, "9999999999999999.9999999999999999998"),(255, "9999999999999999.9999999999999999999"),(256, "90000000000000000.0000000000000000000"),(257, "90000000000000000.0000000000000000001"),(258, "90000000000000000.0000000000000000009"),(259, "90000000000000000.0999999999999999999"),
      (260, "90000000000000000.9000000000000000000"),(261, "90000000000000000.9000000000000000001"),(262, "90000000000000000.9999999999999999998"),(263, "90000000000000000.9999999999999999999"),(264, "90000000000000001.0000000000000000000"),(265, "90000000000000001.0000000000000000001"),(266, "90000000000000001.0000000000000000009"),(267, "90000000000000001.0999999999999999999"),(268, "90000000000000001.9000000000000000000"),(269, "90000000000000001.9000000000000000001"),
      (270, "90000000000000001.9999999999999999998"),(271, "90000000000000001.9999999999999999999"),(272, "99999999999999998.0000000000000000000"),(273, "99999999999999998.0000000000000000001"),(274, "99999999999999998.0000000000000000009"),(275, "99999999999999998.0999999999999999999"),(276, "99999999999999998.9000000000000000000"),(277, "99999999999999998.9000000000000000001"),(278, "99999999999999998.9999999999999999998"),(279, "99999999999999998.9999999999999999999"),
      (280, "99999999999999999.0000000000000000000"),(281, "99999999999999999.0000000000000000001"),(282, "99999999999999999.0000000000000000009"),(283, "99999999999999999.0999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_38_37 values (284, "0.0000000000000000000000000000000000000"),(285, "0.0000000000000000000000000000000000001"),(286, "0.0000000000000000000000000000000000009"),(287, "0.0999999999999999999999999999999999999"),(288, "0.9000000000000000000000000000000000000"),(289, "0.9000000000000000000000000000000000001"),(290, "0.9999999999999999999999999999999999998"),(291, "0.9999999999999999999999999999999999999"),(292, "8.0000000000000000000000000000000000000"),(293, "8.0000000000000000000000000000000000001"),
      (294, "8.0000000000000000000000000000000000009"),(295, "8.0999999999999999999999999999999999999"),(296, "8.9000000000000000000000000000000000000"),(297, "8.9000000000000000000000000000000000001"),(298, "8.9999999999999999999999999999999999998"),(299, "8.9999999999999999999999999999999999999"),(300, "9.0000000000000000000000000000000000000"),(301, "9.0000000000000000000000000000000000001"),(302, "9.0000000000000000000000000000000000009"),(303, "9.0999999999999999999999999999999999999"),
      (304, "9.9000000000000000000000000000000000000"),(305, "9.9000000000000000000000000000000000001"),(306, "9.9999999999999999999999999999999999998"),(307, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal64_17_0_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal64_17_0_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_17_0_from_decimal128v3_38_38 values (308, "0.00000000000000000000000000000000000000"),(309, "0.00000000000000000000000000000000000001"),(310, "0.00000000000000000000000000000000000009"),(311, "0.09999999999999999999999999999999999999"),(312, "0.90000000000000000000000000000000000000"),(313, "0.90000000000000000000000000000000000001"),(314, "0.99999999999999999999999999999999999998"),(315, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(17, 0)) from test_cast_to_decimal64_17_0_from_decimal128v3_38_38 order by 1;'

}