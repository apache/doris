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


suite("test_cast_to_decimal32_1_0_from_decimal128i") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_0(f1 int, f2 decimalv3(19, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_1(f1 int, f2 decimalv3(19, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9"),(7, "8.0"),(8, "8.1"),(9, "8.8"),(10, "8.9"),(11, "9.0"),(12, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_9(f1 int, f2 decimalv3(19, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_9 values (13, "0.000000000"),(14, "0.000000001"),(15, "0.000000009"),(16, "0.099999999"),(17, "0.900000000"),(18, "0.900000001"),(19, "0.999999998"),(20, "0.999999999"),(21, "8.000000000"),(22, "8.000000001"),
      (23, "8.000000009"),(24, "8.099999999"),(25, "8.900000000"),(26, "8.900000001"),(27, "8.999999998"),(28, "8.999999999"),(29, "9.000000000"),(30, "9.000000001"),(31, "9.000000009"),(32, "9.099999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_9 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_18;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_18(f1 int, f2 decimalv3(19, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_18 values (33, "0.000000000000000000"),(34, "0.000000000000000001"),(35, "0.000000000000000009"),(36, "0.099999999999999999"),(37, "0.900000000000000000"),(38, "0.900000000000000001"),(39, "0.999999999999999998"),(40, "0.999999999999999999"),(41, "8.000000000000000000"),(42, "8.000000000000000001"),
      (43, "8.000000000000000009"),(44, "8.099999999999999999"),(45, "8.900000000000000000"),(46, "8.900000000000000001"),(47, "8.999999999999999998"),(48, "8.999999999999999999"),(49, "9.000000000000000000"),(50, "9.000000000000000001"),(51, "9.000000000000000009"),(52, "9.099999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_19_19;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_19_19(f1 int, f2 decimalv3(19, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_19_19 values (53, "0.0000000000000000000"),(54, "0.0000000000000000001"),(55, "0.0000000000000000009"),(56, "0.0999999999999999999"),(57, "0.9000000000000000000"),(58, "0.9000000000000000001"),(59, "0.9999999999999999998"),(60, "0.9999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_19_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_0(f1 int, f2 decimalv3(37, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_0 values (61, "0"),(62, "8"),(63, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_1(f1 int, f2 decimalv3(37, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_1 values (64, "0.0"),(65, "0.1"),(66, "0.8"),(67, "0.9"),(68, "8.0"),(69, "8.1"),(70, "8.8"),(71, "8.9"),(72, "9.0"),(73, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_18;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_18(f1 int, f2 decimalv3(37, 18)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_18 values (74, "0.000000000000000000"),(75, "0.000000000000000001"),(76, "0.000000000000000009"),(77, "0.099999999999999999"),(78, "0.900000000000000000"),(79, "0.900000000000000001"),(80, "0.999999999999999998"),(81, "0.999999999999999999"),(82, "8.000000000000000000"),(83, "8.000000000000000001"),
      (84, "8.000000000000000009"),(85, "8.099999999999999999"),(86, "8.900000000000000000"),(87, "8.900000000000000001"),(88, "8.999999999999999998"),(89, "8.999999999999999999"),(90, "9.000000000000000000"),(91, "9.000000000000000001"),(92, "9.000000000000000009"),(93, "9.099999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_18 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_18 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_36;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_36(f1 int, f2 decimalv3(37, 36)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_36 values (94, "0.000000000000000000000000000000000000"),(95, "0.000000000000000000000000000000000001"),(96, "0.000000000000000000000000000000000009"),(97, "0.099999999999999999999999999999999999"),(98, "0.900000000000000000000000000000000000"),(99, "0.900000000000000000000000000000000001"),(100, "0.999999999999999999999999999999999998"),(101, "0.999999999999999999999999999999999999"),(102, "8.000000000000000000000000000000000000"),(103, "8.000000000000000000000000000000000001"),
      (104, "8.000000000000000000000000000000000009"),(105, "8.099999999999999999999999999999999999"),(106, "8.900000000000000000000000000000000000"),(107, "8.900000000000000000000000000000000001"),(108, "8.999999999999999999999999999999999998"),(109, "8.999999999999999999999999999999999999"),(110, "9.000000000000000000000000000000000000"),(111, "9.000000000000000000000000000000000001"),(112, "9.000000000000000000000000000000000009"),(113, "9.099999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_36 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_36 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_37_37;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_37_37(f1 int, f2 decimalv3(37, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_37_37 values (114, "0.0000000000000000000000000000000000000"),(115, "0.0000000000000000000000000000000000001"),(116, "0.0000000000000000000000000000000000009"),(117, "0.0999999999999999999999999999999999999"),(118, "0.9000000000000000000000000000000000000"),(119, "0.9000000000000000000000000000000000001"),(120, "0.9999999999999999999999999999999999998"),(121, "0.9999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_37_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_0(f1 int, f2 decimalv3(38, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_0 values (122, "0"),(123, "8"),(124, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_1(f1 int, f2 decimalv3(38, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_1 values (125, "0.0"),(126, "0.1"),(127, "0.8"),(128, "0.9"),(129, "8.0"),(130, "8.1"),(131, "8.8"),(132, "8.9"),(133, "9.0"),(134, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_19;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_19(f1 int, f2 decimalv3(38, 19)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_19 values (135, "0.0000000000000000000"),(136, "0.0000000000000000001"),(137, "0.0000000000000000009"),(138, "0.0999999999999999999"),(139, "0.9000000000000000000"),(140, "0.9000000000000000001"),(141, "0.9999999999999999998"),(142, "0.9999999999999999999"),(143, "8.0000000000000000000"),(144, "8.0000000000000000001"),
      (145, "8.0000000000000000009"),(146, "8.0999999999999999999"),(147, "8.9000000000000000000"),(148, "8.9000000000000000001"),(149, "8.9999999999999999998"),(150, "8.9999999999999999999"),(151, "9.0000000000000000000"),(152, "9.0000000000000000001"),(153, "9.0000000000000000009"),(154, "9.0999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_19 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_19 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_37;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_37(f1 int, f2 decimalv3(38, 37)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_37 values (155, "0.0000000000000000000000000000000000000"),(156, "0.0000000000000000000000000000000000001"),(157, "0.0000000000000000000000000000000000009"),(158, "0.0999999999999999999999999999999999999"),(159, "0.9000000000000000000000000000000000000"),(160, "0.9000000000000000000000000000000000001"),(161, "0.9999999999999999999999999999999999998"),(162, "0.9999999999999999999999999999999999999"),(163, "8.0000000000000000000000000000000000000"),(164, "8.0000000000000000000000000000000000001"),
      (165, "8.0000000000000000000000000000000000009"),(166, "8.0999999999999999999999999999999999999"),(167, "8.9000000000000000000000000000000000000"),(168, "8.9000000000000000000000000000000000001"),(169, "8.9999999999999999999999999999999999998"),(170, "8.9999999999999999999999999999999999999"),(171, "9.0000000000000000000000000000000000000"),(172, "9.0000000000000000000000000000000000001"),(173, "9.0000000000000000000000000000000000009"),(174, "9.0999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_37 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_37 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal128i_38_38;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal128i_38_38(f1 int, f2 decimalv3(38, 38)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal128i_38_38 values (175, "0.00000000000000000000000000000000000000"),(176, "0.00000000000000000000000000000000000001"),(177, "0.00000000000000000000000000000000000009"),(178, "0.09999999999999999999999999999999999999"),(179, "0.90000000000000000000000000000000000000"),(180, "0.90000000000000000000000000000000000001"),(181, "0.99999999999999999999999999999999999998"),(182, "0.99999999999999999999999999999999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_38 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal128i_38_38 order by 1;'

}