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


suite("test_cast_to_decimal128i_19_9_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_0 values (0, "0"),(1, "999999999"),(2, "9000000000"),(3, "9000000001"),(4, "9999999998"),(5, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_1 values (6, "0.0"),(7, "0.1"),(8, "0.8"),(9, "0.9"),(10, "999999999.0"),(11, "999999999.1"),(12, "999999999.8"),(13, "999999999.9"),(14, "9000000000.0"),(15, "9000000000.1"),
      (16, "9000000000.8"),(17, "9000000000.9"),(18, "9000000001.0"),(19, "9000000001.1"),(20, "9000000001.8"),(21, "9000000001.9"),(22, "9999999998.0"),(23, "9999999998.1"),(24, "9999999998.8"),(25, "9999999998.9"),
      (26, "9999999999.0"),(27, "9999999999.1"),(28, "9999999999.8"),(29, "9999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_9 values (30, "0.000000000"),(31, "0.000000001"),(32, "0.000000009"),(33, "0.099999999"),(34, "0.900000000"),(35, "0.900000001"),(36, "0.999999998"),(37, "0.999999999"),(38, "999999999.000000000"),(39, "999999999.000000001"),
      (40, "999999999.000000009"),(41, "999999999.099999999"),(42, "999999999.900000000"),(43, "999999999.900000001"),(44, "999999999.999999998"),(45, "999999999.999999999"),(46, "9000000000.000000000"),(47, "9000000000.000000001"),(48, "9000000000.000000009"),(49, "9000000000.099999999"),
      (50, "9000000000.900000000"),(51, "9000000000.900000001"),(52, "9000000000.999999998"),(53, "9000000000.999999999"),(54, "9000000001.000000000"),(55, "9000000001.000000001"),(56, "9000000001.000000009"),(57, "9000000001.099999999"),(58, "9000000001.900000000"),(59, "9000000001.900000001"),
      (60, "9000000001.999999998"),(61, "9000000001.999999999"),(62, "9999999998.000000000"),(63, "9999999998.000000001"),(64, "9999999998.000000009"),(65, "9999999998.099999999"),(66, "9999999998.900000000"),(67, "9999999998.900000001"),(68, "9999999998.999999998"),(69, "9999999998.999999999"),
      (70, "9999999999.000000000"),(71, "9999999999.000000001"),(72, "9999999999.000000009"),(73, "9999999999.099999999"),(74, "9999999999.900000000"),(75, "9999999999.900000001"),(76, "9999999999.999999998"),(77, "9999999999.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_18 values (78, "0.000000000000000000"),(79, "0.000000000000000001"),(80, "0.000000000000000009"),(81, "0.099999999999999999"),(82, "0.900000000000000000"),(83, "0.900000000000000001"),(84, "0.999999999999999998"),(85, "0.999999999999999999"),(86, "8.000000000000000000"),(87, "8.000000000000000001"),
      (88, "8.000000000000000009"),(89, "8.099999999999999999"),(90, "8.900000000000000000"),(91, "8.900000000000000001"),(92, "8.999999999999999998"),(93, "8.999999999999999999"),(94, "9.000000000000000000"),(95, "9.000000000000000001"),(96, "9.000000000000000009"),(97, "9.099999999999999999"),
      (98, "9.900000000000000000"),(99, "9.900000000000000001"),(100, "9.999999999999999998"),(101, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_19_19 values (102, "0.0000000000000000000"),(103, "0.0000000000000000001"),(104, "0.0000000000000000009"),(105, "0.0999999999999999999"),(106, "0.9000000000000000000"),(107, "0.9000000000000000001"),(108, "0.9999999999999999998"),(109, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_0 values (110, "0"),(111, "999999999"),(112, "9000000000"),(113, "9000000001"),(114, "9999999998"),(115, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_1 values (116, "0.0"),(117, "0.1"),(118, "0.8"),(119, "0.9"),(120, "999999999.0"),(121, "999999999.1"),(122, "999999999.8"),(123, "999999999.9"),(124, "9000000000.0"),(125, "9000000000.1"),
      (126, "9000000000.8"),(127, "9000000000.9"),(128, "9000000001.0"),(129, "9000000001.1"),(130, "9000000001.8"),(131, "9000000001.9"),(132, "9999999998.0"),(133, "9999999998.1"),(134, "9999999998.8"),(135, "9999999998.9"),
      (136, "9999999999.0"),(137, "9999999999.1"),(138, "9999999999.8"),(139, "9999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_18 values (140, "0.000000000000000000"),(141, "0.000000000000000001"),(142, "0.000000000000000009"),(143, "0.099999999999999999"),(144, "0.900000000000000000"),(145, "0.900000000000000001"),(146, "0.999999999999999998"),(147, "0.999999999999999999"),(148, "999999999.000000000000000000"),(149, "999999999.000000000000000001"),
      (150, "999999999.000000000000000009"),(151, "999999999.099999999999999999"),(152, "999999999.900000000000000000"),(153, "999999999.900000000000000001"),(154, "999999999.999999999999999998"),(155, "999999999.999999999999999999"),(156, "9000000000.000000000000000000"),(157, "9000000000.000000000000000001"),(158, "9000000000.000000000000000009"),(159, "9000000000.099999999999999999"),
      (160, "9000000000.900000000000000000"),(161, "9000000000.900000000000000001"),(162, "9000000000.999999999999999998"),(163, "9000000000.999999999999999999"),(164, "9000000001.000000000000000000"),(165, "9000000001.000000000000000001"),(166, "9000000001.000000000000000009"),(167, "9000000001.099999999999999999"),(168, "9000000001.900000000000000000"),(169, "9000000001.900000000000000001"),
      (170, "9000000001.999999999999999998"),(171, "9000000001.999999999999999999"),(172, "9999999998.000000000000000000"),(173, "9999999998.000000000000000001"),(174, "9999999998.000000000000000009"),(175, "9999999998.099999999999999999"),(176, "9999999998.900000000000000000"),(177, "9999999998.900000000000000001"),(178, "9999999998.999999999999999998"),(179, "9999999998.999999999999999999"),
      (180, "9999999999.000000000000000000"),(181, "9999999999.000000000000000001"),(182, "9999999999.000000000000000009"),(183, "9999999999.099999999999999999"),(184, "9999999999.900000000000000000"),(185, "9999999999.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_36 values (186, "0.000000000000000000000000000000000000"),(187, "0.000000000000000000000000000000000001"),(188, "0.000000000000000000000000000000000009"),(189, "0.099999999999999999999999999999999999"),(190, "0.900000000000000000000000000000000000"),(191, "0.900000000000000000000000000000000001"),(192, "0.999999999999999999999999999999999998"),(193, "0.999999999999999999999999999999999999"),(194, "8.000000000000000000000000000000000000"),(195, "8.000000000000000000000000000000000001"),
      (196, "8.000000000000000000000000000000000009"),(197, "8.099999999999999999999999999999999999"),(198, "8.900000000000000000000000000000000000"),(199, "8.900000000000000000000000000000000001"),(200, "8.999999999999999999999999999999999998"),(201, "8.999999999999999999999999999999999999"),(202, "9.000000000000000000000000000000000000"),(203, "9.000000000000000000000000000000000001"),(204, "9.000000000000000000000000000000000009"),(205, "9.099999999999999999999999999999999999"),
      (206, "9.900000000000000000000000000000000000"),(207, "9.900000000000000000000000000000000001"),(208, "9.999999999999999999999999999999999998"),(209, "9.999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_37_37 values (210, "0.0000000000000000000000000000000000000"),(211, "0.0000000000000000000000000000000000001"),(212, "0.0000000000000000000000000000000000009"),(213, "0.0999999999999999999999999999999999999"),(214, "0.9000000000000000000000000000000000000"),(215, "0.9000000000000000000000000000000000001"),(216, "0.9999999999999999999999999999999999998"),(217, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_0 values (218, "0"),(219, "999999999"),(220, "9000000000"),(221, "9000000001"),(222, "9999999998"),(223, "9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_1 values (224, "0.0"),(225, "0.1"),(226, "0.8"),(227, "0.9"),(228, "999999999.0"),(229, "999999999.1"),(230, "999999999.8"),(231, "999999999.9"),(232, "9000000000.0"),(233, "9000000000.1"),
      (234, "9000000000.8"),(235, "9000000000.9"),(236, "9000000001.0"),(237, "9000000001.1"),(238, "9000000001.8"),(239, "9000000001.9"),(240, "9999999998.0"),(241, "9999999998.1"),(242, "9999999998.8"),(243, "9999999998.9"),
      (244, "9999999999.0"),(245, "9999999999.1"),(246, "9999999999.8"),(247, "9999999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_19 values (248, "0.0000000000000000000"),(249, "0.0000000000000000001"),(250, "0.0000000000000000009"),(251, "0.0999999999999999999"),(252, "0.9000000000000000000"),(253, "0.9000000000000000001"),(254, "0.9999999999999999998"),(255, "0.9999999999999999999"),(256, "999999999.0000000000000000000"),(257, "999999999.0000000000000000001"),
      (258, "999999999.0000000000000000009"),(259, "999999999.0999999999999999999"),(260, "999999999.9000000000000000000"),(261, "999999999.9000000000000000001"),(262, "999999999.9999999999999999998"),(263, "999999999.9999999999999999999"),(264, "9000000000.0000000000000000000"),(265, "9000000000.0000000000000000001"),(266, "9000000000.0000000000000000009"),(267, "9000000000.0999999999999999999"),
      (268, "9000000000.9000000000000000000"),(269, "9000000000.9000000000000000001"),(270, "9000000000.9999999999999999998"),(271, "9000000000.9999999999999999999"),(272, "9000000001.0000000000000000000"),(273, "9000000001.0000000000000000001"),(274, "9000000001.0000000000000000009"),(275, "9000000001.0999999999999999999"),(276, "9000000001.9000000000000000000"),(277, "9000000001.9000000000000000001"),
      (278, "9000000001.9999999999999999998"),(279, "9000000001.9999999999999999999"),(280, "9999999998.0000000000000000000"),(281, "9999999998.0000000000000000001"),(282, "9999999998.0000000000000000009"),(283, "9999999998.0999999999999999999"),(284, "9999999998.9000000000000000000"),(285, "9999999998.9000000000000000001"),(286, "9999999998.9999999999999999998"),(287, "9999999998.9999999999999999999"),
      (288, "9999999999.0000000000000000000"),(289, "9999999999.0000000000000000001"),(290, "9999999999.0000000000000000009"),(291, "9999999999.0999999999999999999"),(292, "9999999999.9000000000000000000"),(293, "9999999999.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_37 values (294, "0.0000000000000000000000000000000000000"),(295, "0.0000000000000000000000000000000000001"),(296, "0.0000000000000000000000000000000000009"),(297, "0.0999999999999999999999999999999999999"),(298, "0.9000000000000000000000000000000000000"),(299, "0.9000000000000000000000000000000000001"),(300, "0.9999999999999999999999999999999999998"),(301, "0.9999999999999999999999999999999999999"),(302, "8.0000000000000000000000000000000000000"),(303, "8.0000000000000000000000000000000000001"),
      (304, "8.0000000000000000000000000000000000009"),(305, "8.0999999999999999999999999999999999999"),(306, "8.9000000000000000000000000000000000000"),(307, "8.9000000000000000000000000000000000001"),(308, "8.9999999999999999999999999999999999998"),(309, "8.9999999999999999999999999999999999999"),(310, "9.0000000000000000000000000000000000000"),(311, "9.0000000000000000000000000000000000001"),(312, "9.0000000000000000000000000000000000009"),(313, "9.0999999999999999999999999999999999999"),
      (314, "9.9000000000000000000000000000000000000"),(315, "9.9000000000000000000000000000000000001"),(316, "9.9999999999999999999999999999999999998"),(317, "9.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal128i_19_9_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_decimal128i_38_38 values (318, "0.00000000000000000000000000000000000000"),(319, "0.00000000000000000000000000000000000001"),(320, "0.00000000000000000000000000000000000009"),(321, "0.09999999999999999999999999999999999999"),(322, "0.90000000000000000000000000000000000000"),(323, "0.90000000000000000000000000000000000001"),(324, "0.99999999999999999999999999999999999998"),(325, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_decimal128i_38_38 order by 1;'

}