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


suite("test_cast_to_decimal256_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_cast_to_decimal256_1_0_from_tinyint;"
    sql "create table test_cast_to_decimal256_1_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_1_0_from_tinyint values (0, -9),(1, -8),(2, -1),(3, 0),(4, 1),(5, 8),(6, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_0_from_tinyint;"
    sql "create table test_cast_to_decimal256_38_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_0_from_tinyint values (7, -128),(8, -127),(9, -9),(10, -1),(11, 0),(12, 1),(13, 9),(14, 126),(15, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_19_from_tinyint;"
    sql "create table test_cast_to_decimal256_38_19_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_19_from_tinyint values (16, -128),(17, -127),(18, -9),(19, -1),(20, 0),(21, 1),(22, 9),(23, 126),(24, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_37_from_tinyint;"
    sql "create table test_cast_to_decimal256_38_37_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_37_from_tinyint values (25, -9),(26, -8),(27, -1),(28, 0),(29, 1),(30, 8),(31, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_0_from_tinyint;"
    sql "create table test_cast_to_decimal256_76_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_tinyint values (32, -128),(33, -127),(34, -9),(35, -1),(36, 0),(37, 1),(38, 9),(39, 126),(40, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_tinyint;"
    sql "create table test_cast_to_decimal256_76_38_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_tinyint values (41, -128),(42, -127),(43, -9),(44, -1),(45, 0),(46, 1),(47, 9),(48, 126),(49, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_tinyint;"
    sql "create table test_cast_to_decimal256_76_75_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_tinyint values (50, -9),(51, -8),(52, -1),(53, 0),(54, 1),(55, 8),(56, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_1_0_from_smallint;"
    sql "create table test_cast_to_decimal256_1_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_1_0_from_smallint values (57, -9),(58, -8),(59, -1),(60, 0),(61, 1),(62, 8),(63, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_0_from_smallint;"
    sql "create table test_cast_to_decimal256_38_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_0_from_smallint values (64, -32768),(65, -32767),(66, -9),(67, -1),(68, 0),(69, 1),(70, 9),(71, 32766),(72, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_19_from_smallint;"
    sql "create table test_cast_to_decimal256_38_19_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_19_from_smallint values (73, -32768),(74, -32767),(75, -9),(76, -1),(77, 0),(78, 1),(79, 9),(80, 32766),(81, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_37_from_smallint;"
    sql "create table test_cast_to_decimal256_38_37_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_37_from_smallint values (82, -9),(83, -8),(84, -1),(85, 0),(86, 1),(87, 8),(88, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_0_from_smallint;"
    sql "create table test_cast_to_decimal256_76_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_smallint values (89, -32768),(90, -32767),(91, -9),(92, -1),(93, 0),(94, 1),(95, 9),(96, 32766),(97, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_smallint;"
    sql "create table test_cast_to_decimal256_76_38_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_smallint values (98, -32768),(99, -32767),(100, -9),(101, -1),(102, 0),(103, 1),(104, 9),(105, 32766),(106, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_smallint;"
    sql "create table test_cast_to_decimal256_76_75_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_smallint values (107, -9),(108, -8),(109, -1),(110, 0),(111, 1),(112, 8),(113, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_1_0_from_int;"
    sql "create table test_cast_to_decimal256_1_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_1_0_from_int values (114, -9),(115, -8),(116, -1),(117, 0),(118, 1),(119, 8),(120, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_0_from_int;"
    sql "create table test_cast_to_decimal256_38_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_0_from_int values (121, -2147483648),(122, -2147483647),(123, -9),(124, -1),(125, 0),(126, 1),(127, 9),(128, 2147483646),(129, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_19_from_int;"
    sql "create table test_cast_to_decimal256_38_19_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_19_from_int values (130, -2147483648),(131, -2147483647),(132, -9),(133, -1),(134, 0),(135, 1),(136, 9),(137, 2147483646),(138, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_37_from_int;"
    sql "create table test_cast_to_decimal256_38_37_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_37_from_int values (139, -9),(140, -8),(141, -1),(142, 0),(143, 1),(144, 8),(145, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_0_from_int;"
    sql "create table test_cast_to_decimal256_76_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_int values (146, -2147483648),(147, -2147483647),(148, -9),(149, -1),(150, 0),(151, 1),(152, 9),(153, 2147483646),(154, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_int;"
    sql "create table test_cast_to_decimal256_76_38_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_int values (155, -2147483648),(156, -2147483647),(157, -9),(158, -1),(159, 0),(160, 1),(161, 9),(162, 2147483646),(163, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_int;"
    sql "create table test_cast_to_decimal256_76_75_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_int values (164, -9),(165, -8),(166, -1),(167, 0),(168, 1),(169, 8),(170, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal256_1_0_from_bigint;"
    sql "create table test_cast_to_decimal256_1_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_1_0_from_bigint values (171, -9),(172, -8),(173, -1),(174, 0),(175, 1),(176, 8),(177, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_0_from_bigint;"
    sql "create table test_cast_to_decimal256_38_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_0_from_bigint values (178, -9223372036854775808),(179, -9223372036854775807),(180, -9),(181, -1),(182, 0),(183, 1),(184, 9),(185, 9223372036854775806),(186, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_19_from_bigint;"
    sql "create table test_cast_to_decimal256_38_19_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_19_from_bigint values (187, -9223372036854775808),(188, -9223372036854775807),(189, -9),(190, -1),(191, 0),(192, 1),(193, 9),(194, 9223372036854775806),(195, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_37_from_bigint;"
    sql "create table test_cast_to_decimal256_38_37_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_37_from_bigint values (196, -9),(197, -8),(198, -1),(199, 0),(200, 1),(201, 8),(202, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_0_from_bigint;"
    sql "create table test_cast_to_decimal256_76_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_bigint values (203, -9223372036854775808),(204, -9223372036854775807),(205, -9),(206, -1),(207, 0),(208, 1),(209, 9),(210, 9223372036854775806),(211, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_bigint;"
    sql "create table test_cast_to_decimal256_76_38_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_bigint values (212, -9223372036854775808),(213, -9223372036854775807),(214, -9),(215, -1),(216, 0),(217, 1),(218, 9),(219, 9223372036854775806),(220, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_bigint;"
    sql "create table test_cast_to_decimal256_76_75_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_bigint values (221, -9),(222, -8),(223, -1),(224, 0),(225, 1),(226, 8),(227, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_1_0_from_largeint;"
    sql "create table test_cast_to_decimal256_1_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_1_0_from_largeint values (228, -9),(229, -8),(230, -1),(231, 0),(232, 1),(233, 8),(234, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal256_1_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_0_from_largeint;"
    sql "create table test_cast_to_decimal256_38_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_0_from_largeint values (235, -99999999999999999999999999999999999999),(236, -90000000000000000000000000000000000001),(237, -90000000000000000000000000000000000000),(238, -9999999999999999999999999999999999999),(239, -9),(240, -1),(241, 0),(242, 1),(243, 9),(244, 9999999999999999999999999999999999999),
      (245, 90000000000000000000000000000000000000),(246, 90000000000000000000000000000000000001),(247, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal256_38_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_19_from_largeint;"
    sql "create table test_cast_to_decimal256_38_19_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_19_from_largeint values (248, -9999999999999999999),(249, -9000000000000000001),(250, -9000000000000000000),(251, -999999999999999999),(252, -9),(253, -1),(254, 0),(255, 1),(256, 9),(257, 999999999999999999),
      (258, 9000000000000000000),(259, 9000000000000000001),(260, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal256_38_19_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_38_37_from_largeint;"
    sql "create table test_cast_to_decimal256_38_37_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_38_37_from_largeint values (261, -9),(262, -8),(263, -1),(264, 0),(265, 1),(266, 8),(267, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal256_38_37_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_0_from_largeint;"
    sql "create table test_cast_to_decimal256_76_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_0_from_largeint values (268, -170141183460469231731687303715884105728),(269, -170141183460469231731687303715884105727),(270, -9),(271, -1),(272, 0),(273, 1),(274, 9),(275, 170141183460469231731687303715884105726),(276, 170141183460469231731687303715884105727);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(76, 0)) from test_cast_to_decimal256_76_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_38_from_largeint;"
    sql "create table test_cast_to_decimal256_76_38_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_38_from_largeint values (277, -99999999999999999999999999999999999999),(278, -90000000000000000000000000000000000001),(279, -90000000000000000000000000000000000000),(280, -9999999999999999999999999999999999999),(281, -9),(282, -1),(283, 0),(284, 1),(285, 9),(286, 9999999999999999999999999999999999999),
      (287, 90000000000000000000000000000000000000),(288, 90000000000000000000000000000000000001),(289, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(76, 38)) from test_cast_to_decimal256_76_38_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal256_76_75_from_largeint;"
    sql "create table test_cast_to_decimal256_76_75_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal256_76_75_from_largeint values (290, -9),(291, -8),(292, -1),(293, 0),(294, 1),(295, 8),(296, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(76, 75)) from test_cast_to_decimal256_76_75_from_largeint order by 1;'

}