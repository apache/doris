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


suite("test_cast_to_decimal32_8_7_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_19_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_19_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1"),
      (13, "9.8"),(14, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_19_9 values (15, "0.000000000"),(16, "0.000000001"),(17, "0.000000009"),(18, "0.099999999"),(19, "0.900000000"),(20, "0.900000001"),(21, "0.999999998"),(22, "0.999999999"),(23, "8.000000000"),(24, "8.000000001"),
      (25, "8.000000009"),(26, "8.099999999"),(27, "8.900000000"),(28, "8.900000001"),(29, "8.999999998"),(30, "8.999999999"),(31, "9.000000000"),(32, "9.000000001"),(33, "9.000000009"),(34, "9.099999999"),
      (35, "9.900000000"),(36, "9.900000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_19_18 values (37, "0.000000000000000000"),(38, "0.000000000000000001"),(39, "0.000000000000000009"),(40, "0.099999999999999999"),(41, "0.900000000000000000"),(42, "0.900000000000000001"),(43, "0.999999999999999998"),(44, "0.999999999999999999"),(45, "8.000000000000000000"),(46, "8.000000000000000001"),
      (47, "8.000000000000000009"),(48, "8.099999999999999999"),(49, "8.900000000000000000"),(50, "8.900000000000000001"),(51, "8.999999999999999998"),(52, "8.999999999999999999"),(53, "9.000000000000000000"),(54, "9.000000000000000001"),(55, "9.000000000000000009"),(56, "9.099999999999999999"),
      (57, "9.900000000000000000"),(58, "9.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_19_19 values (59, "0.0000000000000000000"),(60, "0.0000000000000000001"),(61, "0.0000000000000000009"),(62, "0.0999999999999999999"),(63, "0.9000000000000000000"),(64, "0.9000000000000000001"),(65, "0.9999999999999999998"),(66, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_37_0 values (67, "0"),(68, "8"),(69, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_37_1 values (70, "0.0"),(71, "0.1"),(72, "0.8"),(73, "0.9"),(74, "8.0"),(75, "8.1"),(76, "8.8"),(77, "8.9"),(78, "9.0"),(79, "9.1"),
      (80, "9.8"),(81, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_37_18 values (82, "0.000000000000000000"),(83, "0.000000000000000001"),(84, "0.000000000000000009"),(85, "0.099999999999999999"),(86, "0.900000000000000000"),(87, "0.900000000000000001"),(88, "0.999999999999999998"),(89, "0.999999999999999999"),(90, "8.000000000000000000"),(91, "8.000000000000000001"),
      (92, "8.000000000000000009"),(93, "8.099999999999999999"),(94, "8.900000000000000000"),(95, "8.900000000000000001"),(96, "8.999999999999999998"),(97, "8.999999999999999999"),(98, "9.000000000000000000"),(99, "9.000000000000000001"),(100, "9.000000000000000009"),(101, "9.099999999999999999"),
      (102, "9.900000000000000000"),(103, "9.900000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_37_36 values (104, "0.000000000000000000000000000000000000"),(105, "0.000000000000000000000000000000000001"),(106, "0.000000000000000000000000000000000009"),(107, "0.099999999999999999999999999999999999"),(108, "0.900000000000000000000000000000000000"),(109, "0.900000000000000000000000000000000001"),(110, "0.999999999999999999999999999999999998"),(111, "0.999999999999999999999999999999999999"),(112, "8.000000000000000000000000000000000000"),(113, "8.000000000000000000000000000000000001"),
      (114, "8.000000000000000000000000000000000009"),(115, "8.099999999999999999999999999999999999"),(116, "8.900000000000000000000000000000000000"),(117, "8.900000000000000000000000000000000001"),(118, "8.999999999999999999999999999999999998"),(119, "8.999999999999999999999999999999999999"),(120, "9.000000000000000000000000000000000000"),(121, "9.000000000000000000000000000000000001"),(122, "9.000000000000000000000000000000000009"),(123, "9.099999999999999999999999999999999999"),
      (124, "9.900000000000000000000000000000000000"),(125, "9.900000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_37_37 values (126, "0.0000000000000000000000000000000000000"),(127, "0.0000000000000000000000000000000000001"),(128, "0.0000000000000000000000000000000000009"),(129, "0.0999999999999999999999999999999999999"),(130, "0.9000000000000000000000000000000000000"),(131, "0.9000000000000000000000000000000000001"),(132, "0.9999999999999999999999999999999999998"),(133, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_38_0 values (134, "0"),(135, "8"),(136, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_38_1 values (137, "0.0"),(138, "0.1"),(139, "0.8"),(140, "0.9"),(141, "8.0"),(142, "8.1"),(143, "8.8"),(144, "8.9"),(145, "9.0"),(146, "9.1"),
      (147, "9.8"),(148, "9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_38_19 values (149, "0.0000000000000000000"),(150, "0.0000000000000000001"),(151, "0.0000000000000000009"),(152, "0.0999999999999999999"),(153, "0.9000000000000000000"),(154, "0.9000000000000000001"),(155, "0.9999999999999999998"),(156, "0.9999999999999999999"),(157, "8.0000000000000000000"),(158, "8.0000000000000000001"),
      (159, "8.0000000000000000009"),(160, "8.0999999999999999999"),(161, "8.9000000000000000000"),(162, "8.9000000000000000001"),(163, "8.9999999999999999998"),(164, "8.9999999999999999999"),(165, "9.0000000000000000000"),(166, "9.0000000000000000001"),(167, "9.0000000000000000009"),(168, "9.0999999999999999999"),
      (169, "9.9000000000000000000"),(170, "9.9000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_38_37 values (171, "0.0000000000000000000000000000000000000"),(172, "0.0000000000000000000000000000000000001"),(173, "0.0000000000000000000000000000000000009"),(174, "0.0999999999999999999999999999999999999"),(175, "0.9000000000000000000000000000000000000"),(176, "0.9000000000000000000000000000000000001"),(177, "0.9999999999999999999999999999999999998"),(178, "0.9999999999999999999999999999999999999"),(179, "8.0000000000000000000000000000000000000"),(180, "8.0000000000000000000000000000000000001"),
      (181, "8.0000000000000000000000000000000000009"),(182, "8.0999999999999999999999999999999999999"),(183, "8.9000000000000000000000000000000000000"),(184, "8.9000000000000000000000000000000000001"),(185, "8.9999999999999999999999999999999999998"),(186, "8.9999999999999999999999999999999999999"),(187, "9.0000000000000000000000000000000000000"),(188, "9.0000000000000000000000000000000000001"),(189, "9.0000000000000000000000000000000000009"),(190, "9.0999999999999999999999999999999999999"),
      (191, "9.9000000000000000000000000000000000000"),(192, "9.9000000000000000000000000000000000001");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_8_7_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal32_8_7_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_8_7_from_decimal128i_38_38 values (193, "0.00000000000000000000000000000000000000"),(194, "0.00000000000000000000000000000000000001"),(195, "0.00000000000000000000000000000000000009"),(196, "0.09999999999999999999999999999999999999"),(197, "0.90000000000000000000000000000000000000"),(198, "0.90000000000000000000000000000000000001"),(199, "0.99999999999999999999999999999999999998"),(200, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(8, 7)) from test_cast_to_decimal32_8_7_from_decimal128i_38_38 order by 1;'

}