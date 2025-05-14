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


suite("test_cast_to_decimal128v2_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v2_1_0_from_int8;"
    sql "create table test_cast_to_decimal128v2_1_0_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_1_0_from_int8 values (0, -9),(1, -8),(2, -1),(3, 0),(4, 1),(5, 8),(6, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_0_from_int8;"
    sql "create table test_cast_to_decimal128v2_13_0_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_0_from_int8 values (7, -128),(8, -127),(9, -9),(10, -1),(11, 0),(12, 1),(13, 9),(14, 126),(15, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_6_from_int8;"
    sql "create table test_cast_to_decimal128v2_13_6_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_6_from_int8 values (16, -128),(17, -127),(18, -9),(19, -1),(20, 0),(21, 1),(22, 9),(23, 126),(24, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_12_from_int8;"
    sql "create table test_cast_to_decimal128v2_13_12_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_12_from_int8 values (25, -9),(26, -8),(27, -1),(28, 0),(29, 1),(30, 8),(31, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_0_from_int8;"
    sql "create table test_cast_to_decimal128v2_27_0_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_0_from_int8 values (32, -128),(33, -127),(34, -9),(35, -1),(36, 0),(37, 1),(38, 9),(39, 126),(40, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_13_from_int8;"
    sql "create table test_cast_to_decimal128v2_27_13_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_13_from_int8 values (41, -128),(42, -127),(43, -9),(44, -1),(45, 0),(46, 1),(47, 9),(48, 126),(49, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_26_from_int8;"
    sql "create table test_cast_to_decimal128v2_27_26_from_int8(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_26_from_int8 values (50, -9),(51, -8),(52, -1),(53, 0),(54, 1),(55, 8),(56, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int8 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int8 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_1_0_from_int16;"
    sql "create table test_cast_to_decimal128v2_1_0_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_1_0_from_int16 values (57, -9),(58, -8),(59, -1),(60, 0),(61, 1),(62, 8),(63, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_0_from_int16;"
    sql "create table test_cast_to_decimal128v2_13_0_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_0_from_int16 values (64, -32768),(65, -32767),(66, -9),(67, -1),(68, 0),(69, 1),(70, 9),(71, 32766),(72, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_6_from_int16;"
    sql "create table test_cast_to_decimal128v2_13_6_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_6_from_int16 values (73, -32768),(74, -32767),(75, -9),(76, -1),(77, 0),(78, 1),(79, 9),(80, 32766),(81, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_12_from_int16;"
    sql "create table test_cast_to_decimal128v2_13_12_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_12_from_int16 values (82, -9),(83, -8),(84, -1),(85, 0),(86, 1),(87, 8),(88, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_0_from_int16;"
    sql "create table test_cast_to_decimal128v2_27_0_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_0_from_int16 values (89, -32768),(90, -32767),(91, -9),(92, -1),(93, 0),(94, 1),(95, 9),(96, 32766),(97, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_13_from_int16;"
    sql "create table test_cast_to_decimal128v2_27_13_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_13_from_int16 values (98, -32768),(99, -32767),(100, -9),(101, -1),(102, 0),(103, 1),(104, 9),(105, 32766),(106, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_26_from_int16;"
    sql "create table test_cast_to_decimal128v2_27_26_from_int16(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_26_from_int16 values (107, -9),(108, -8),(109, -1),(110, 0),(111, 1),(112, 8),(113, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int16 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int16 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_1_0_from_int32;"
    sql "create table test_cast_to_decimal128v2_1_0_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_1_0_from_int32 values (114, -9),(115, -8),(116, -1),(117, 0),(118, 1),(119, 8),(120, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_0_from_int32;"
    sql "create table test_cast_to_decimal128v2_13_0_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_0_from_int32 values (121, -2147483648),(122, -2147483647),(123, -9),(124, -1),(125, 0),(126, 1),(127, 9),(128, 2147483646),(129, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_6_from_int32;"
    sql "create table test_cast_to_decimal128v2_13_6_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_6_from_int32 values (130, -9999999),(131, -9000001),(132, -9000000),(133, -999999),(134, -9),(135, -1),(136, 0),(137, 1),(138, 9),(139, 999999),
      (140, 9000000),(141, 9000001),(142, 9999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_12_from_int32;"
    sql "create table test_cast_to_decimal128v2_13_12_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_12_from_int32 values (143, -9),(144, -8),(145, -1),(146, 0),(147, 1),(148, 8),(149, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_0_from_int32;"
    sql "create table test_cast_to_decimal128v2_27_0_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_0_from_int32 values (150, -2147483648),(151, -2147483647),(152, -9),(153, -1),(154, 0),(155, 1),(156, 9),(157, 2147483646),(158, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_13_from_int32;"
    sql "create table test_cast_to_decimal128v2_27_13_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_13_from_int32 values (159, -2147483648),(160, -2147483647),(161, -9),(162, -1),(163, 0),(164, 1),(165, 9),(166, 2147483646),(167, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_26_from_int32;"
    sql "create table test_cast_to_decimal128v2_27_26_from_int32(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_26_from_int32 values (168, -9),(169, -8),(170, -1),(171, 0),(172, 1),(173, 8),(174, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_1_0_from_int64;"
    sql "create table test_cast_to_decimal128v2_1_0_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_1_0_from_int64 values (175, -9),(176, -8),(177, -1),(178, 0),(179, 1),(180, 8),(181, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_0_from_int64;"
    sql "create table test_cast_to_decimal128v2_13_0_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_0_from_int64 values (182, -9999999999999),(183, -9000000000001),(184, -9000000000000),(185, -999999999999),(186, -9),(187, -1),(188, 0),(189, 1),(190, 9),(191, 999999999999),
      (192, 9000000000000),(193, 9000000000001),(194, 9999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_6_from_int64;"
    sql "create table test_cast_to_decimal128v2_13_6_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_6_from_int64 values (195, -9999999),(196, -9000001),(197, -9000000),(198, -999999),(199, -9),(200, -1),(201, 0),(202, 1),(203, 9),(204, 999999),
      (205, 9000000),(206, 9000001),(207, 9999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_12_from_int64;"
    sql "create table test_cast_to_decimal128v2_13_12_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_12_from_int64 values (208, -9),(209, -8),(210, -1),(211, 0),(212, 1),(213, 8),(214, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_0_from_int64;"
    sql "create table test_cast_to_decimal128v2_27_0_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_0_from_int64 values (215, -9223372036854775808),(216, -9223372036854775807),(217, -9),(218, -1),(219, 0),(220, 1),(221, 9),(222, 9223372036854775806),(223, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_13_from_int64;"
    sql "create table test_cast_to_decimal128v2_27_13_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_13_from_int64 values (224, -99999999999999),(225, -90000000000001),(226, -90000000000000),(227, -9999999999999),(228, -9),(229, -1),(230, 0),(231, 1),(232, 9),(233, 9999999999999),
      (234, 90000000000000),(235, 90000000000001),(236, 99999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_26_from_int64;"
    sql "create table test_cast_to_decimal128v2_27_26_from_int64(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_26_from_int64 values (237, -9),(238, -8),(239, -1),(240, 0),(241, 1),(242, 8),(243, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int64 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int64 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_1_0_from_int128;"
    sql "create table test_cast_to_decimal128v2_1_0_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_1_0_from_int128 values (244, -9),(245, -8),(246, -1),(247, 0),(248, 1),(249, 8),(250, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128v2_1_0_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_0_from_int128;"
    sql "create table test_cast_to_decimal128v2_13_0_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_0_from_int128 values (251, -9999999999999),(252, -9000000000001),(253, -9000000000000),(254, -999999999999),(255, -9),(256, -1),(257, 0),(258, 1),(259, 9),(260, 999999999999),
      (261, 9000000000000),(262, 9000000000001),(263, 9999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(13, 0)) from test_cast_to_decimal128v2_13_0_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_6_from_int128;"
    sql "create table test_cast_to_decimal128v2_13_6_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_6_from_int128 values (264, -9999999),(265, -9000001),(266, -9000000),(267, -999999),(268, -9),(269, -1),(270, 0),(271, 1),(272, 9),(273, 999999),
      (274, 9000000),(275, 9000001),(276, 9999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(13, 6)) from test_cast_to_decimal128v2_13_6_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_13_12_from_int128;"
    sql "create table test_cast_to_decimal128v2_13_12_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_13_12_from_int128 values (277, -9),(278, -8),(279, -1),(280, 0),(281, 1),(282, 8),(283, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(13, 12)) from test_cast_to_decimal128v2_13_12_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_0_from_int128;"
    sql "create table test_cast_to_decimal128v2_27_0_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_0_from_int128 values (284, -999999999999999999999999999),(285, -900000000000000000000000001),(286, -900000000000000000000000000),(287, -99999999999999999999999999),(288, -9),(289, -1),(290, 0),(291, 1),(292, 9),(293, 99999999999999999999999999),
      (294, 900000000000000000000000000),(295, 900000000000000000000000001),(296, 999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(27, 0)) from test_cast_to_decimal128v2_27_0_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_13_from_int128;"
    sql "create table test_cast_to_decimal128v2_27_13_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_13_from_int128 values (297, -99999999999999),(298, -90000000000001),(299, -90000000000000),(300, -9999999999999),(301, -9),(302, -1),(303, 0),(304, 1),(305, 9),(306, 9999999999999),
      (307, 90000000000000),(308, 90000000000001),(309, 99999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(27, 13)) from test_cast_to_decimal128v2_27_13_from_int128 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v2_27_26_from_int128;"
    sql "create table test_cast_to_decimal128v2_27_26_from_int128(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v2_27_26_from_int128 values (310, -9),(311, -8),(312, -1),(313, 0),(314, 1),(315, 8),(316, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int128 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(27, 26)) from test_cast_to_decimal128v2_27_26_from_int128 order by 1;'

}