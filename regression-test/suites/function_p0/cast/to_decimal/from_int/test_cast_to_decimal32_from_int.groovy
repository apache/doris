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


suite("test_cast_to_decimal32_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_1_0_from_tinyint;"
    sql "create table test_cast_to_decimal32_1_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_tinyint values (0, -9),(1, -8),(2, -1),(3, 0),(4, 1),(5, 8),(6, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_tinyint;"
    sql "create table test_cast_to_decimal32_4_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_tinyint values (7, -128),(8, -127),(9, -9),(10, -1),(11, 0),(12, 1),(13, 9),(14, 126),(15, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_tinyint;"
    sql "create table test_cast_to_decimal32_4_2_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_tinyint values (16, -99),(17, -91),(18, -90),(19, -9),(20, -1),(21, 0),(22, 1),(23, 9),(24, 90),(25, 91),
      (26, 99);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_tinyint;"
    sql "create table test_cast_to_decimal32_4_3_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_tinyint values (27, -9),(28, -8),(29, -1),(30, 0),(31, 1),(32, 8),(33, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_tinyint;"
    sql "create table test_cast_to_decimal32_9_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_tinyint values (34, -128),(35, -127),(36, -9),(37, -1),(38, 0),(39, 1),(40, 9),(41, 126),(42, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_tinyint;"
    sql "create table test_cast_to_decimal32_9_4_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_tinyint values (43, -128),(44, -127),(45, -9),(46, -1),(47, 0),(48, 1),(49, 9),(50, 126),(51, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_tinyint;"
    sql "create table test_cast_to_decimal32_9_8_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_tinyint values (52, -9),(53, -8),(54, -1),(55, 0),(56, 1),(57, 8),(58, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_smallint;"
    sql "create table test_cast_to_decimal32_1_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_smallint values (59, -9),(60, -8),(61, -1),(62, 0),(63, 1),(64, 8),(65, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_smallint;"
    sql "create table test_cast_to_decimal32_4_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_smallint values (66, -9999),(67, -9001),(68, -9000),(69, -999),(70, -9),(71, -1),(72, 0),(73, 1),(74, 9),(75, 999),
      (76, 9000),(77, 9001),(78, 9999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_smallint;"
    sql "create table test_cast_to_decimal32_4_2_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_smallint values (79, -99),(80, -91),(81, -90),(82, -9),(83, -1),(84, 0),(85, 1),(86, 9),(87, 90),(88, 91),
      (89, 99);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_smallint;"
    sql "create table test_cast_to_decimal32_4_3_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_smallint values (90, -9),(91, -8),(92, -1),(93, 0),(94, 1),(95, 8),(96, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_smallint;"
    sql "create table test_cast_to_decimal32_9_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_smallint values (97, -32768),(98, -32767),(99, -9),(100, -1),(101, 0),(102, 1),(103, 9),(104, 32766),(105, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_smallint;"
    sql "create table test_cast_to_decimal32_9_4_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_smallint values (106, -32768),(107, -32767),(108, -9),(109, -1),(110, 0),(111, 1),(112, 9),(113, 32766),(114, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_smallint;"
    sql "create table test_cast_to_decimal32_9_8_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_smallint values (115, -9),(116, -8),(117, -1),(118, 0),(119, 1),(120, 8),(121, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_int;"
    sql "create table test_cast_to_decimal32_1_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_int values (122, -9),(123, -8),(124, -1),(125, 0),(126, 1),(127, 8),(128, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_int;"
    sql "create table test_cast_to_decimal32_4_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_int values (129, -9999),(130, -9001),(131, -9000),(132, -999),(133, -9),(134, -1),(135, 0),(136, 1),(137, 9),(138, 999),
      (139, 9000),(140, 9001),(141, 9999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_int;"
    sql "create table test_cast_to_decimal32_4_2_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_int values (142, -99),(143, -91),(144, -90),(145, -9),(146, -1),(147, 0),(148, 1),(149, 9),(150, 90),(151, 91),
      (152, 99);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_int;"
    sql "create table test_cast_to_decimal32_4_3_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_int values (153, -9),(154, -8),(155, -1),(156, 0),(157, 1),(158, 8),(159, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_int;"
    sql "create table test_cast_to_decimal32_9_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_int values (160, -999999999),(161, -900000001),(162, -900000000),(163, -99999999),(164, -9),(165, -1),(166, 0),(167, 1),(168, 9),(169, 99999999),
      (170, 900000000),(171, 900000001),(172, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_int;"
    sql "create table test_cast_to_decimal32_9_4_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_int values (173, -99999),(174, -90001),(175, -90000),(176, -9999),(177, -9),(178, -1),(179, 0),(180, 1),(181, 9),(182, 9999),
      (183, 90000),(184, 90001),(185, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_int;"
    sql "create table test_cast_to_decimal32_9_8_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_int values (186, -9),(187, -8),(188, -1),(189, 0),(190, 1),(191, 8),(192, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_bigint;"
    sql "create table test_cast_to_decimal32_1_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_bigint values (193, -9),(194, -8),(195, -1),(196, 0),(197, 1),(198, 8),(199, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_bigint;"
    sql "create table test_cast_to_decimal32_4_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_bigint values (200, -9999),(201, -9001),(202, -9000),(203, -999),(204, -9),(205, -1),(206, 0),(207, 1),(208, 9),(209, 999),
      (210, 9000),(211, 9001),(212, 9999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_bigint;"
    sql "create table test_cast_to_decimal32_4_2_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_bigint values (213, -99),(214, -91),(215, -90),(216, -9),(217, -1),(218, 0),(219, 1),(220, 9),(221, 90),(222, 91),
      (223, 99);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_bigint;"
    sql "create table test_cast_to_decimal32_4_3_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_bigint values (224, -9),(225, -8),(226, -1),(227, 0),(228, 1),(229, 8),(230, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_bigint;"
    sql "create table test_cast_to_decimal32_9_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_bigint values (231, -999999999),(232, -900000001),(233, -900000000),(234, -99999999),(235, -9),(236, -1),(237, 0),(238, 1),(239, 9),(240, 99999999),
      (241, 900000000),(242, 900000001),(243, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_bigint;"
    sql "create table test_cast_to_decimal32_9_4_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_bigint values (244, -99999),(245, -90001),(246, -90000),(247, -9999),(248, -9),(249, -1),(250, 0),(251, 1),(252, 9),(253, 9999),
      (254, 90000),(255, 90001),(256, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_bigint;"
    sql "create table test_cast_to_decimal32_9_8_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_bigint values (257, -9),(258, -8),(259, -1),(260, 0),(261, 1),(262, 8),(263, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_1_0_from_largeint;"
    sql "create table test_cast_to_decimal32_1_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_1_0_from_largeint values (264, -9),(265, -8),(266, -1),(267, 0),(268, 1),(269, 8),(270, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal32_1_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_0_from_largeint;"
    sql "create table test_cast_to_decimal32_4_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_0_from_largeint values (271, -9999),(272, -9001),(273, -9000),(274, -999),(275, -9),(276, -1),(277, 0),(278, 1),(279, 9),(280, 999),
      (281, 9000),(282, 9001),(283, 9999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(4, 0)) from test_cast_to_decimal32_4_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_2_from_largeint;"
    sql "create table test_cast_to_decimal32_4_2_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_2_from_largeint values (284, -99),(285, -91),(286, -90),(287, -9),(288, -1),(289, 0),(290, 1),(291, 9),(292, 90),(293, 91),
      (294, 99);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(4, 2)) from test_cast_to_decimal32_4_2_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_4_3_from_largeint;"
    sql "create table test_cast_to_decimal32_4_3_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_4_3_from_largeint values (295, -9),(296, -8),(297, -1),(298, 0),(299, 1),(300, 8),(301, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(4, 3)) from test_cast_to_decimal32_4_3_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_0_from_largeint;"
    sql "create table test_cast_to_decimal32_9_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_largeint values (302, -999999999),(303, -900000001),(304, -900000000),(305, -99999999),(306, -9),(307, -1),(308, 0),(309, 1),(310, 9),(311, 99999999),
      (312, 900000000),(313, 900000001),(314, 999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_4_from_largeint;"
    sql "create table test_cast_to_decimal32_9_4_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_4_from_largeint values (315, -99999),(316, -90001),(317, -90000),(318, -9999),(319, -9),(320, -1),(321, 0),(322, 1),(323, 9),(324, 9999),
      (325, 90000),(326, 90001),(327, 99999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(9, 4)) from test_cast_to_decimal32_9_4_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal32_9_8_from_largeint;"
    sql "create table test_cast_to_decimal32_9_8_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_8_from_largeint values (328, -9),(329, -8),(330, -1),(331, 0),(332, 1),(333, 8),(334, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(9, 8)) from test_cast_to_decimal32_9_8_from_largeint order by 1;'

}