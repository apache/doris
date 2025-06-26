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


suite("test_cast_to_decimal32_1_0_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_1_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_1_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_1_0 values (0, "0"),(1, "8"),(2, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_1_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_1_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_1_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_1_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_1_1 values (3, "0.0"),(4, "0.1"),(5, "0.8"),(6, "0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_1_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_1_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_0(f1 int, f2 decimalv3(4, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_0 values (7, "0"),(8, "8"),(9, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_1(f1 int, f2 decimalv3(4, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_1 values (10, "0.0"),(11, "0.1"),(12, "0.8"),(13, "0.9"),(14, "8.0"),(15, "8.1"),(16, "8.8"),(17, "8.9"),(18, "9.0"),(19, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_2;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_2(f1 int, f2 decimalv3(4, 2)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_2 values (20, "0.00"),(21, "0.01"),(22, "0.09"),(23, "0.90"),(24, "0.91"),(25, "0.98"),(26, "0.99"),(27, "8.00"),(28, "8.01"),(29, "8.09"),
      (30, "8.90"),(31, "8.91"),(32, "8.98"),(33, "8.99"),(34, "9.00"),(35, "9.01"),(36, "9.09");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_2 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_3;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_3(f1 int, f2 decimalv3(4, 3)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_3 values (37, "0.000"),(38, "0.001"),(39, "0.009"),(40, "0.099"),(41, "0.900"),(42, "0.901"),(43, "0.998"),(44, "0.999"),(45, "8.000"),(46, "8.001"),
      (47, "8.009"),(48, "8.099"),(49, "8.900"),(50, "8.901"),(51, "8.998"),(52, "8.999"),(53, "9.000"),(54, "9.001"),(55, "9.009"),(56, "9.099");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_3 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_4_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_4_4(f1 int, f2 decimalv3(4, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_4_4 values (57, "0.0000"),(58, "0.0001"),(59, "0.0009"),(60, "0.0999"),(61, "0.9000"),(62, "0.9001"),(63, "0.9998"),(64, "0.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_4_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_0(f1 int, f2 decimalv3(8, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_0 values (65, "0"),(66, "8"),(67, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_1(f1 int, f2 decimalv3(8, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_1 values (68, "0.0"),(69, "0.1"),(70, "0.8"),(71, "0.9"),(72, "8.0"),(73, "8.1"),(74, "8.8"),(75, "8.9"),(76, "9.0"),(77, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_4(f1 int, f2 decimalv3(8, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_4 values (78, "0.0000"),(79, "0.0001"),(80, "0.0009"),(81, "0.0999"),(82, "0.9000"),(83, "0.9001"),(84, "0.9998"),(85, "0.9999"),(86, "8.0000"),(87, "8.0001"),
      (88, "8.0009"),(89, "8.0999"),(90, "8.9000"),(91, "8.9001"),(92, "8.9998"),(93, "8.9999"),(94, "9.0000"),(95, "9.0001"),(96, "9.0009"),(97, "9.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_7;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_7(f1 int, f2 decimalv3(8, 7)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_7 values (98, "0.0000000"),(99, "0.0000001"),(100, "0.0000009"),(101, "0.0999999"),(102, "0.9000000"),(103, "0.9000001"),(104, "0.9999998"),(105, "0.9999999"),(106, "8.0000000"),(107, "8.0000001"),
      (108, "8.0000009"),(109, "8.0999999"),(110, "8.9000000"),(111, "8.9000001"),(112, "8.9999998"),(113, "8.9999999"),(114, "9.0000000"),(115, "9.0000001"),(116, "9.0000009"),(117, "9.0999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_7 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_7 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_8_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_8_8(f1 int, f2 decimalv3(8, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_8_8 values (118, "0.00000000"),(119, "0.00000001"),(120, "0.00000009"),(121, "0.09999999"),(122, "0.90000000"),(123, "0.90000001"),(124, "0.99999998"),(125, "0.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_8_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_0;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_0(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_0 values (126, "0"),(127, "8"),(128, "9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_0 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_1;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_1(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_1 values (129, "0.0"),(130, "0.1"),(131, "0.8"),(132, "0.9"),(133, "8.0"),(134, "8.1"),(135, "8.8"),(136, "8.9"),(137, "9.0"),(138, "9.1");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_1 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_4;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_4 values (139, "0.0000"),(140, "0.0001"),(141, "0.0009"),(142, "0.0999"),(143, "0.9000"),(144, "0.9001"),(145, "0.9998"),(146, "0.9999"),(147, "8.0000"),(148, "8.0001"),
      (149, "8.0009"),(150, "8.0999"),(151, "8.9000"),(152, "8.9001"),(153, "8.9998"),(154, "8.9999"),(155, "9.0000"),(156, "9.0001"),(157, "9.0009"),(158, "9.0999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_4 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_8;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_8(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_8 values (159, "0.00000000"),(160, "0.00000001"),(161, "0.00000009"),(162, "0.09999999"),(163, "0.90000000"),(164, "0.90000001"),(165, "0.99999998"),(166, "0.99999999"),(167, "8.00000000"),(168, "8.00000001"),
      (169, "8.00000009"),(170, "8.09999999"),(171, "8.90000000"),(172, "8.90000001"),(173, "8.99999998"),(174, "8.99999999"),(175, "9.00000000"),(176, "9.00000001"),(177, "9.00000009"),(178, "9.09999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_8 order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_decimal32_9_9;"
    sql "create table test_cast_to_decimal32_1_0_from_decimal32_9_9(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_decimal32_9_9 values (179, "0.000000000"),(180, "0.000000001"),(181, "0.000000009"),(182, "0.099999999"),(183, "0.900000000"),(184, "0.900000001"),(185, "0.999999998"),(186, "0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_9 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_decimal32_9_9 order by 1;'

}