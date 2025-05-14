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


suite("test_cast_to_decimal128v3_38_37_from_decimal64") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_10_0;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_10_0(f1 int, f2 decimalv3(10, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_10_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_10_1;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_10_1(f1 int, f2 decimalv3(10, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_10_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1"),
      (13, "9.8"),(14, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_10_5;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_10_5(f1 int, f2 decimalv3(10, 5)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_10_5 values (15, "0.00000"),(16, "0.00001"),(17, "0.00009"),(18, "0.09999"),(19, "0.90000"),(20, "0.90001"),(21, "0.99998"),(22, "0.99999"),(23, "8.00000"),(24, "8.00001"),
      (25, "8.00009"),(26, "8.09999"),(27, "8.90000"),(28, "8.90001"),(29, "8.99998"),(30, "8.99999"),(31, "9.00000"),(32, "9.00001"),(33, "9.00009"),(34, "9.09999"),
      (35, "9.90000"),(36, "9.90001"),(37, "9.99998"),(38, "9.99999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_5 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_10_9;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_10_9(f1 int, f2 decimalv3(10, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_10_9 values (39, "0.000000000"),(40, "0.000000001"),(41, "0.000000009"),(42, "0.099999999"),(43, "0.900000000"),(44, "0.900000001"),(45, "0.999999998"),(46, "0.999999999"),(47, "8.000000000"),(48, "8.000000001"),
      (49, "8.000000009"),(50, "8.099999999"),(51, "8.900000000"),(52, "8.900000001"),(53, "8.999999998"),(54, "8.999999999"),(55, "9.000000000"),(56, "9.000000001"),(57, "9.000000009"),(58, "9.099999999"),
      (59, "9.900000000"),(60, "9.900000001"),(61, "9.999999998"),(62, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_10_10;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_10_10(f1 int, f2 decimalv3(10, 10)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_10_10 values (63, "0.0000000000"),(64, "0.0000000001"),(65, "0.0000000009"),(66, "0.0999999999"),(67, "0.9000000000"),(68, "0.9000000001"),(69, "0.9999999998"),(70, "0.9999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_10 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_10_10 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_17_0;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_17_0(f1 int, f2 decimalv3(17, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_17_0 values (71, "0"),(72, "8"),(73, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_17_1;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_17_1(f1 int, f2 decimalv3(17, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_17_1 values (74, "0.0"),(75, "0.1"),(76, "0.8"),(77, "0.9"),(78, "8.0"),(79, "8.1"),(80, "8.8"),(81, "8.9"),(82, "9.0"),(83, "9.1"),
      (84, "9.8"),(85, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_17_8;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_17_8(f1 int, f2 decimalv3(17, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_17_8 values (86, "0.00000000"),(87, "0.00000001"),(88, "0.00000009"),(89, "0.09999999"),(90, "0.90000000"),(91, "0.90000001"),(92, "0.99999998"),(93, "0.99999999"),(94, "8.00000000"),(95, "8.00000001"),
      (96, "8.00000009"),(97, "8.09999999"),(98, "8.90000000"),(99, "8.90000001"),(100, "8.99999998"),(101, "8.99999999"),(102, "9.00000000"),(103, "9.00000001"),(104, "9.00000009"),(105, "9.09999999"),
      (106, "9.90000000"),(107, "9.90000001"),(108, "9.99999998"),(109, "9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_17_16;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_17_16(f1 int, f2 decimalv3(17, 16)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_17_16 values (110, "0.0000000000000000"),(111, "0.0000000000000001"),(112, "0.0000000000000009"),(113, "0.0999999999999999"),(114, "0.9000000000000000"),(115, "0.9000000000000001"),(116, "0.9999999999999998"),(117, "0.9999999999999999"),(118, "8.0000000000000000"),(119, "8.0000000000000001"),
      (120, "8.0000000000000009"),(121, "8.0999999999999999"),(122, "8.9000000000000000"),(123, "8.9000000000000001"),(124, "8.9999999999999998"),(125, "8.9999999999999999"),(126, "9.0000000000000000"),(127, "9.0000000000000001"),(128, "9.0000000000000009"),(129, "9.0999999999999999"),
      (130, "9.9000000000000000"),(131, "9.9000000000000001"),(132, "9.9999999999999998"),(133, "9.9999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_17_17;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_17_17(f1 int, f2 decimalv3(17, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_17_17 values (134, "0.00000000000000000"),(135, "0.00000000000000001"),(136, "0.00000000000000009"),(137, "0.09999999999999999"),(138, "0.90000000000000000"),(139, "0.90000000000000001"),(140, "0.99999999999999998"),(141, "0.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_17_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_18_0;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_18_0(f1 int, f2 decimalv3(18, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_18_0 values (142, "0"),(143, "8"),(144, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_18_1;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_18_1(f1 int, f2 decimalv3(18, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_18_1 values (145, "0.0"),(146, "0.1"),(147, "0.8"),(148, "0.9"),(149, "8.0"),(150, "8.1"),(151, "8.8"),(152, "8.9"),(153, "9.0"),(154, "9.1"),
      (155, "9.8"),(156, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_18_9;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_18_9(f1 int, f2 decimalv3(18, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_18_9 values (157, "0.000000000"),(158, "0.000000001"),(159, "0.000000009"),(160, "0.099999999"),(161, "0.900000000"),(162, "0.900000001"),(163, "0.999999998"),(164, "0.999999999"),(165, "8.000000000"),(166, "8.000000001"),
      (167, "8.000000009"),(168, "8.099999999"),(169, "8.900000000"),(170, "8.900000001"),(171, "8.999999998"),(172, "8.999999999"),(173, "9.000000000"),(174, "9.000000001"),(175, "9.000000009"),(176, "9.099999999"),
      (177, "9.900000000"),(178, "9.900000001"),(179, "9.999999998"),(180, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_18_17;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_18_17(f1 int, f2 decimalv3(18, 17)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_18_17 values (181, "0.00000000000000000"),(182, "0.00000000000000001"),(183, "0.00000000000000009"),(184, "0.09999999999999999"),(185, "0.90000000000000000"),(186, "0.90000000000000001"),(187, "0.99999999999999998"),(188, "0.99999999999999999"),(189, "8.00000000000000000"),(190, "8.00000000000000001"),
      (191, "8.00000000000000009"),(192, "8.09999999999999999"),(193, "8.90000000000000000"),(194, "8.90000000000000001"),(195, "8.99999999999999998"),(196, "8.99999999999999999"),(197, "9.00000000000000000"),(198, "9.00000000000000001"),(199, "9.00000000000000009"),(200, "9.09999999999999999"),
      (201, "9.90000000000000000"),(202, "9.90000000000000001"),(203, "9.99999999999999998"),(204, "9.99999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_17 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_17 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_decimal64_18_18;"
    sql "create table test_cast_to_decimal128v3_38_37_from_decimal64_18_18(f1 int, f2 decimalv3(18, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_decimal64_18_18 values (205, "0.000000000000000000"),(206, "0.000000000000000001"),(207, "0.000000000000000009"),(208, "0.099999999999999999"),(209, "0.900000000000000000"),(210, "0.900000000000000001"),(211, "0.999999999999999998"),(212, "0.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_decimal64_18_18 order by 1;'

}