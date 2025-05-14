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


suite("test_cast_to_decimal128v3_19_18_from_decimal128v3") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_19_0;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_19_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_19_1;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_19_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1"),
      (13, "9.8"),(14, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_19_9;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_19_9 values (15, "0.000000000"),(16, "0.000000001"),(17, "0.000000009"),(18, "0.099999999"),(19, "0.900000000"),(20, "0.900000001"),(21, "0.999999998"),(22, "0.999999999"),(23, "8.000000000"),(24, "8.000000001"),
      (25, "8.000000009"),(26, "8.099999999"),(27, "8.900000000"),(28, "8.900000001"),(29, "8.999999998"),(30, "8.999999999"),(31, "9.000000000"),(32, "9.000000001"),(33, "9.000000009"),(34, "9.099999999"),
      (35, "9.900000000"),(36, "9.900000001"),(37, "9.999999998"),(38, "9.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_19_18;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_19_18 values (39, "0.000000000000000000"),(40, "0.000000000000000001"),(41, "0.000000000000000009"),(42, "0.099999999999999999"),(43, "0.900000000000000000"),(44, "0.900000000000000001"),(45, "0.999999999999999998"),(46, "0.999999999999999999"),(47, "8.000000000000000000"),(48, "8.000000000000000001"),
      (49, "8.000000000000000009"),(50, "8.099999999999999999"),(51, "8.900000000000000000"),(52, "8.900000000000000001"),(53, "8.999999999999999998"),(54, "8.999999999999999999"),(55, "9.000000000000000000"),(56, "9.000000000000000001"),(57, "9.000000000000000009"),(58, "9.099999999999999999"),
      (59, "9.900000000000000000"),(60, "9.900000000000000001"),(61, "9.999999999999999998"),(62, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_19_19;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_19_19 values (63, "0.0000000000000000000"),(64, "0.0000000000000000001"),(65, "0.0000000000000000009"),(66, "0.0999999999999999999"),(67, "0.9000000000000000000"),(68, "0.9000000000000000001"),(69, "0.9999999999999999998"),(70, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_37_0;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_37_0 values (71, "0"),(72, "8"),(73, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_37_1;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_37_1 values (74, "0.0"),(75, "0.1"),(76, "0.8"),(77, "0.9"),(78, "8.0"),(79, "8.1"),(80, "8.8"),(81, "8.9"),(82, "9.0"),(83, "9.1"),
      (84, "9.8"),(85, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_37_18;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_37_18 values (86, "0.000000000000000000"),(87, "0.000000000000000001"),(88, "0.000000000000000009"),(89, "0.099999999999999999"),(90, "0.900000000000000000"),(91, "0.900000000000000001"),(92, "0.999999999999999998"),(93, "0.999999999999999999"),(94, "8.000000000000000000"),(95, "8.000000000000000001"),
      (96, "8.000000000000000009"),(97, "8.099999999999999999"),(98, "8.900000000000000000"),(99, "8.900000000000000001"),(100, "8.999999999999999998"),(101, "8.999999999999999999"),(102, "9.000000000000000000"),(103, "9.000000000000000001"),(104, "9.000000000000000009"),(105, "9.099999999999999999"),
      (106, "9.900000000000000000"),(107, "9.900000000000000001"),(108, "9.999999999999999998"),(109, "9.999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_37_36;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_37_36 values (110, "0.000000000000000000000000000000000000"),(111, "0.000000000000000000000000000000000001"),(112, "0.000000000000000000000000000000000009"),(113, "0.099999999999999999999999999999999999"),(114, "0.900000000000000000000000000000000000"),(115, "0.900000000000000000000000000000000001"),(116, "0.999999999999999999999999999999999998"),(117, "0.999999999999999999999999999999999999"),(118, "8.000000000000000000000000000000000000"),(119, "8.000000000000000000000000000000000001"),
      (120, "8.000000000000000000000000000000000009"),(121, "8.099999999999999999999999999999999999"),(122, "8.900000000000000000000000000000000000"),(123, "8.900000000000000000000000000000000001"),(124, "8.999999999999999999999999999999999998"),(125, "8.999999999999999999999999999999999999"),(126, "9.000000000000000000000000000000000000"),(127, "9.000000000000000000000000000000000001"),(128, "9.000000000000000000000000000000000009"),(129, "9.099999999999999999999999999999999999"),
      (130, "9.900000000000000000000000000000000000"),(131, "9.900000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_37_37;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_37_37 values (132, "0.0000000000000000000000000000000000000"),(133, "0.0000000000000000000000000000000000001"),(134, "0.0000000000000000000000000000000000009"),(135, "0.0999999999999999999999999999999999999"),(136, "0.9000000000000000000000000000000000000"),(137, "0.9000000000000000000000000000000000001"),(138, "0.9999999999999999999999999999999999998"),(139, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_38_0;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_38_0 values (140, "0"),(141, "8"),(142, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_38_1;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_38_1 values (143, "0.0"),(144, "0.1"),(145, "0.8"),(146, "0.9"),(147, "8.0"),(148, "8.1"),(149, "8.8"),(150, "8.9"),(151, "9.0"),(152, "9.1"),
      (153, "9.8"),(154, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_38_19;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_38_19 values (155, "0.0000000000000000000"),(156, "0.0000000000000000001"),(157, "0.0000000000000000009"),(158, "0.0999999999999999999"),(159, "0.9000000000000000000"),(160, "0.9000000000000000001"),(161, "0.9999999999999999998"),(162, "0.9999999999999999999"),(163, "8.0000000000000000000"),(164, "8.0000000000000000001"),
      (165, "8.0000000000000000009"),(166, "8.0999999999999999999"),(167, "8.9000000000000000000"),(168, "8.9000000000000000001"),(169, "8.9999999999999999998"),(170, "8.9999999999999999999"),(171, "9.0000000000000000000"),(172, "9.0000000000000000001"),(173, "9.0000000000000000009"),(174, "9.0999999999999999999"),
      (175, "9.9000000000000000000"),(176, "9.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_38_37;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_38_37 values (177, "0.0000000000000000000000000000000000000"),(178, "0.0000000000000000000000000000000000001"),(179, "0.0000000000000000000000000000000000009"),(180, "0.0999999999999999999999999999999999999"),(181, "0.9000000000000000000000000000000000000"),(182, "0.9000000000000000000000000000000000001"),(183, "0.9999999999999999999999999999999999998"),(184, "0.9999999999999999999999999999999999999"),(185, "8.0000000000000000000000000000000000000"),(186, "8.0000000000000000000000000000000000001"),
      (187, "8.0000000000000000000000000000000000009"),(188, "8.0999999999999999999999999999999999999"),(189, "8.9000000000000000000000000000000000000"),(190, "8.9000000000000000000000000000000000001"),(191, "8.9999999999999999999999999999999999998"),(192, "8.9999999999999999999999999999999999999"),(193, "9.0000000000000000000000000000000000000"),(194, "9.0000000000000000000000000000000000001"),(195, "9.0000000000000000000000000000000000009"),(196, "9.0999999999999999999999999999999999999"),
      (197, "9.9000000000000000000000000000000000000"),(198, "9.9000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_19_18_from_decimal128v3_38_38;"
    sql "create table test_cast_to_decimal128v3_19_18_from_decimal128v3_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_19_18_from_decimal128v3_38_38 values (199, "0.00000000000000000000000000000000000000"),(200, "0.00000000000000000000000000000000000001"),(201, "0.00000000000000000000000000000000000009"),(202, "0.09999999999999999999999999999999999999"),(203, "0.90000000000000000000000000000000000000"),(204, "0.90000000000000000000000000000000000001"),(205, "0.99999999999999999999999999999999999998"),(206, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128v3_19_18_from_decimal128v3_38_38 order by 1;'

}