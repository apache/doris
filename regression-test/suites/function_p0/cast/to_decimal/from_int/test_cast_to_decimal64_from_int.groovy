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


suite("test_cast_to_decimal64_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal64_1_0_from_tinyint;"
    sql "create table test_cast_to_decimal64_1_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_1_0_from_tinyint values (0, -9),(1, -8),(2, -1),(3, 0),(4, 1),(5, 8),(6, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_0_from_tinyint;"
    sql "create table test_cast_to_decimal64_9_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_0_from_tinyint values (7, -128),(8, -127),(9, -9),(10, -1),(11, 0),(12, 1),(13, 9),(14, 126),(15, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_4_from_tinyint;"
    sql "create table test_cast_to_decimal64_9_4_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_4_from_tinyint values (16, -128),(17, -127),(18, -9),(19, -1),(20, 0),(21, 1),(22, 9),(23, 126),(24, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_8_from_tinyint;"
    sql "create table test_cast_to_decimal64_9_8_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_8_from_tinyint values (25, -9),(26, -8),(27, -1),(28, 0),(29, 1),(30, 8),(31, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_tinyint;"
    sql "create table test_cast_to_decimal64_18_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_tinyint values (32, -128),(33, -127),(34, -9),(35, -1),(36, 0),(37, 1),(38, 9),(39, 126),(40, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_tinyint;"
    sql "create table test_cast_to_decimal64_18_9_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_tinyint values (41, -128),(42, -127),(43, -9),(44, -1),(45, 0),(46, 1),(47, 9),(48, 126),(49, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_tinyint;"
    sql "create table test_cast_to_decimal64_18_17_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_tinyint values (50, -9),(51, -8),(52, -1),(53, 0),(54, 1),(55, 8),(56, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_1_0_from_smallint;"
    sql "create table test_cast_to_decimal64_1_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_1_0_from_smallint values (57, -9),(58, -8),(59, -1),(60, 0),(61, 1),(62, 8),(63, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_0_from_smallint;"
    sql "create table test_cast_to_decimal64_9_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_0_from_smallint values (64, -32768),(65, -32767),(66, -9),(67, -1),(68, 0),(69, 1),(70, 9),(71, 32766),(72, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_4_from_smallint;"
    sql "create table test_cast_to_decimal64_9_4_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_4_from_smallint values (73, -32768),(74, -32767),(75, -9),(76, -1),(77, 0),(78, 1),(79, 9),(80, 32766),(81, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_8_from_smallint;"
    sql "create table test_cast_to_decimal64_9_8_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_8_from_smallint values (82, -9),(83, -8),(84, -1),(85, 0),(86, 1),(87, 8),(88, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_smallint;"
    sql "create table test_cast_to_decimal64_18_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_smallint values (89, -32768),(90, -32767),(91, -9),(92, -1),(93, 0),(94, 1),(95, 9),(96, 32766),(97, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_smallint;"
    sql "create table test_cast_to_decimal64_18_9_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_smallint values (98, -32768),(99, -32767),(100, -9),(101, -1),(102, 0),(103, 1),(104, 9),(105, 32766),(106, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_smallint;"
    sql "create table test_cast_to_decimal64_18_17_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_smallint values (107, -9),(108, -8),(109, -1),(110, 0),(111, 1),(112, 8),(113, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_1_0_from_int;"
    sql "create table test_cast_to_decimal64_1_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_1_0_from_int values (114, -9),(115, -8),(116, -1),(117, 0),(118, 1),(119, 8),(120, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_0_from_int;"
    sql "create table test_cast_to_decimal64_9_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_0_from_int values (121, -999999999),(122, -900000001),(123, -900000000),(124, -99999999),(125, -9),(126, -1),(127, 0),(128, 1),(129, 9),(130, 99999999),
      (131, 900000000),(132, 900000001),(133, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_4_from_int;"
    sql "create table test_cast_to_decimal64_9_4_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_4_from_int values (134, -99999),(135, -90001),(136, -90000),(137, -9999),(138, -9),(139, -1),(140, 0),(141, 1),(142, 9),(143, 9999),
      (144, 90000),(145, 90001),(146, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_8_from_int;"
    sql "create table test_cast_to_decimal64_9_8_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_8_from_int values (147, -9),(148, -8),(149, -1),(150, 0),(151, 1),(152, 8),(153, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_int;"
    sql "create table test_cast_to_decimal64_18_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_int values (154, -2147483648),(155, -2147483647),(156, -9),(157, -1),(158, 0),(159, 1),(160, 9),(161, 2147483646),(162, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_int;"
    sql "create table test_cast_to_decimal64_18_9_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_int values (163, -999999999),(164, -900000001),(165, -900000000),(166, -99999999),(167, -9),(168, -1),(169, 0),(170, 1),(171, 9),(172, 99999999),
      (173, 900000000),(174, 900000001),(175, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_int;"
    sql "create table test_cast_to_decimal64_18_17_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_int values (176, -9),(177, -8),(178, -1),(179, 0),(180, 1),(181, 8),(182, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal64_1_0_from_bigint;"
    sql "create table test_cast_to_decimal64_1_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_1_0_from_bigint values (183, -9),(184, -8),(185, -1),(186, 0),(187, 1),(188, 8),(189, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_0_from_bigint;"
    sql "create table test_cast_to_decimal64_9_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_0_from_bigint values (190, -999999999),(191, -900000001),(192, -900000000),(193, -99999999),(194, -9),(195, -1),(196, 0),(197, 1),(198, 9),(199, 99999999),
      (200, 900000000),(201, 900000001),(202, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_4_from_bigint;"
    sql "create table test_cast_to_decimal64_9_4_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_4_from_bigint values (203, -99999),(204, -90001),(205, -90000),(206, -9999),(207, -9),(208, -1),(209, 0),(210, 1),(211, 9),(212, 9999),
      (213, 90000),(214, 90001),(215, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_8_from_bigint;"
    sql "create table test_cast_to_decimal64_9_8_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_8_from_bigint values (216, -9),(217, -8),(218, -1),(219, 0),(220, 1),(221, 8),(222, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_bigint;"
    sql "create table test_cast_to_decimal64_18_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_bigint values (223, -999999999999999999),(224, -900000000000000001),(225, -900000000000000000),(226, -99999999999999999),(227, -9),(228, -1),(229, 0),(230, 1),(231, 9),(232, 99999999999999999),
      (233, 900000000000000000),(234, 900000000000000001),(235, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_bigint;"
    sql "create table test_cast_to_decimal64_18_9_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_bigint values (236, -999999999),(237, -900000001),(238, -900000000),(239, -99999999),(240, -9),(241, -1),(242, 0),(243, 1),(244, 9),(245, 99999999),
      (246, 900000000),(247, 900000001),(248, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_bigint;"
    sql "create table test_cast_to_decimal64_18_17_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_bigint values (249, -9),(250, -8),(251, -1),(252, 0),(253, 1),(254, 8),(255, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_1_0_from_largeint;"
    sql "create table test_cast_to_decimal64_1_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_1_0_from_largeint values (256, -9),(257, -8),(258, -1),(259, 0),(260, 1),(261, 8),(262, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal64_1_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_0_from_largeint;"
    sql "create table test_cast_to_decimal64_9_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_0_from_largeint values (263, -999999999),(264, -900000001),(265, -900000000),(266, -99999999),(267, -9),(268, -1),(269, 0),(270, 1),(271, 9),(272, 99999999),
      (273, 900000000),(274, 900000001),(275, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal64_9_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_4_from_largeint;"
    sql "create table test_cast_to_decimal64_9_4_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_4_from_largeint values (276, -99999),(277, -90001),(278, -90000),(279, -9999),(280, -9),(281, -1),(282, 0),(283, 1),(284, 9),(285, 9999),
      (286, 90000),(287, 90001),(288, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal64_9_4_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_9_8_from_largeint;"
    sql "create table test_cast_to_decimal64_9_8_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_9_8_from_largeint values (289, -9),(290, -8),(291, -1),(292, 0),(293, 1),(294, 8),(295, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal64_9_8_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_0_from_largeint;"
    sql "create table test_cast_to_decimal64_18_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_0_from_largeint values (296, -999999999999999999),(297, -900000000000000001),(298, -900000000000000000),(299, -99999999999999999),(300, -9),(301, -1),(302, 0),(303, 1),(304, 9),(305, 99999999999999999),
      (306, 900000000000000000),(307, 900000000000000001),(308, 999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(18, 0)) from test_cast_to_decimal64_18_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_9_from_largeint;"
    sql "create table test_cast_to_decimal64_18_9_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_9_from_largeint values (309, -999999999),(310, -900000001),(311, -900000000),(312, -99999999),(313, -9),(314, -1),(315, 0),(316, 1),(317, 9),(318, 99999999),
      (319, 900000000),(320, 900000001),(321, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(18, 9)) from test_cast_to_decimal64_18_9_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal64_18_17_from_largeint;"
    sql "create table test_cast_to_decimal64_18_17_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal64_18_17_from_largeint values (322, -9),(323, -8),(324, -1),(325, 0),(326, 1),(327, 8),(328, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(18, 17)) from test_cast_to_decimal64_18_17_from_largeint order by 1;'

}