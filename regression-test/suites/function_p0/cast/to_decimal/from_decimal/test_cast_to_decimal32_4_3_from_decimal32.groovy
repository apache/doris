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


suite("test_cast_to_decimal32_4_3_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_0 values (7, "0"),(8, "8"),(9, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_1 values (10, "0.0"),(11, "0.1"),(12, "0.8"),(13, "0.9"),(14, "8.0"),(15, "8.1"),(16, "8.8"),(17, "8.9"),(18, "9.0"),(19, "9.1"),
      (20, "9.8"),(21, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_2 values (22, "0.00"),(23, "0.01"),(24, "0.09"),(25, "0.90"),(26, "0.91"),(27, "0.98"),(28, "0.99"),(29, "8.00"),(30, "8.01"),(31, "8.09"),
      (32, "8.90"),(33, "8.91"),(34, "8.98"),(35, "8.99"),(36, "9.00"),(37, "9.01"),(38, "9.09"),(39, "9.90"),(40, "9.91"),(41, "9.98"),
      (42, "9.99");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_3 values (43, "0.000"),(44, "0.001"),(45, "0.009"),(46, "0.099"),(47, "0.900"),(48, "0.901"),(49, "0.998"),(50, "0.999"),(51, "8.000"),(52, "8.001"),
      (53, "8.009"),(54, "8.099"),(55, "8.900"),(56, "8.901"),(57, "8.998"),(58, "8.999"),(59, "9.000"),(60, "9.001"),(61, "9.009"),(62, "9.099"),
      (63, "9.900"),(64, "9.901"),(65, "9.998"),(66, "9.999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_4_4 values (67, "0.0000"),(68, "0.0001"),(69, "0.0009"),(70, "0.0999"),(71, "0.9000"),(72, "0.9001"),(73, "0.9998"),(74, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_0 values (75, "0"),(76, "8"),(77, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_1 values (78, "0.0"),(79, "0.1"),(80, "0.8"),(81, "0.9"),(82, "8.0"),(83, "8.1"),(84, "8.8"),(85, "8.9"),(86, "9.0"),(87, "9.1"),
      (88, "9.8"),(89, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_4 values (90, "0.0000"),(91, "0.0001"),(92, "0.0009"),(93, "0.0999"),(94, "0.9000"),(95, "0.9001"),(96, "0.9998"),(97, "0.9999"),(98, "8.0000"),(99, "8.0001"),
      (100, "8.0009"),(101, "8.0999"),(102, "8.9000"),(103, "8.9001"),(104, "8.9998"),(105, "8.9999"),(106, "9.0000"),(107, "9.0001"),(108, "9.0009"),(109, "9.0999"),
      (110, "9.9000"),(111, "9.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_7 values (112, "0.0000000"),(113, "0.0000001"),(114, "0.0000009"),(115, "0.0999999"),(116, "0.9000000"),(117, "0.9000001"),(118, "0.9999998"),(119, "0.9999999"),(120, "8.0000000"),(121, "8.0000001"),
      (122, "8.0000009"),(123, "8.0999999"),(124, "8.9000000"),(125, "8.9000001"),(126, "8.9999998"),(127, "8.9999999"),(128, "9.0000000"),(129, "9.0000001"),(130, "9.0000009"),(131, "9.0999999"),
      (132, "9.9000000"),(133, "9.9000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_8_8 values (134, "0.00000000"),(135, "0.00000001"),(136, "0.00000009"),(137, "0.09999999"),(138, "0.90000000"),(139, "0.90000001"),(140, "0.99999998"),(141, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_0 values (142, "0"),(143, "8"),(144, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_1 values (145, "0.0"),(146, "0.1"),(147, "0.8"),(148, "0.9"),(149, "8.0"),(150, "8.1"),(151, "8.8"),(152, "8.9"),(153, "9.0"),(154, "9.1"),
      (155, "9.8"),(156, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_4 values (157, "0.0000"),(158, "0.0001"),(159, "0.0009"),(160, "0.0999"),(161, "0.9000"),(162, "0.9001"),(163, "0.9998"),(164, "0.9999"),(165, "8.0000"),(166, "8.0001"),
      (167, "8.0009"),(168, "8.0999"),(169, "8.9000"),(170, "8.9001"),(171, "8.9998"),(172, "8.9999"),(173, "9.0000"),(174, "9.0001"),(175, "9.0009"),(176, "9.0999"),
      (177, "9.9000"),(178, "9.9001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_8 values (179, "0.00000000"),(180, "0.00000001"),(181, "0.00000009"),(182, "0.09999999"),(183, "0.90000000"),(184, "0.90000001"),(185, "0.99999998"),(186, "0.99999999"),(187, "8.00000000"),(188, "8.00000001"),
      (189, "8.00000009"),(190, "8.09999999"),(191, "8.90000000"),(192, "8.90000001"),(193, "8.99999998"),(194, "8.99999999"),(195, "9.00000000"),(196, "9.00000001"),(197, "9.00000009"),(198, "9.09999999"),
      (199, "9.90000000"),(200, "9.90000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_4_3_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_decimal32_9_9 values (201, "0.000000000"),(202, "0.000000001"),(203, "0.000000009"),(204, "0.099999999"),(205, "0.900000000"),(206, "0.900000001"),(207, "0.999999998"),(208, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_decimal32_9_9 order by 1;'

}