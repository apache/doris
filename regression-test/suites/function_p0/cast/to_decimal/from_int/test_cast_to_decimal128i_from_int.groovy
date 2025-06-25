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


suite("test_cast_to_decimal128i_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128i_1_0_from_tinyint;"
    sql "create table test_cast_to_decimal128i_1_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_1_0_from_tinyint values (0, -9),(1, -8),(2, -1),(3, 0),(4, 1),(5, 8),(6, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_tinyint;"
    sql "create table test_cast_to_decimal128i_19_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_tinyint values (7, -128),(8, -127),(9, -9),(10, -1),(11, 0),(12, 1),(13, 9),(14, 126),(15, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_tinyint;"
    sql "create table test_cast_to_decimal128i_19_9_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_tinyint values (16, -128),(17, -127),(18, -9),(19, -1),(20, 0),(21, 1),(22, 9),(23, 126),(24, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_tinyint;"
    sql "create table test_cast_to_decimal128i_19_18_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_tinyint values (25, -9),(26, -8),(27, -1),(28, 0),(29, 1),(30, 8),(31, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_tinyint;"
    sql "create table test_cast_to_decimal128i_38_0_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_tinyint values (32, -128),(33, -127),(34, -9),(35, -1),(36, 0),(37, 1),(38, 9),(39, 126),(40, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_tinyint;"
    sql "create table test_cast_to_decimal128i_38_19_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_tinyint values (41, -128),(42, -127),(43, -9),(44, -1),(45, 0),(46, 1),(47, 9),(48, 126),(49, 127);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_tinyint;"
    sql "create table test_cast_to_decimal128i_38_37_from_tinyint(f1 int, f2 tinyint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_tinyint values (50, -9),(51, -8),(52, -1),(53, 0),(54, 1),(55, 8),(56, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_tinyint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_tinyint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_1_0_from_smallint;"
    sql "create table test_cast_to_decimal128i_1_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_1_0_from_smallint values (57, -9),(58, -8),(59, -1),(60, 0),(61, 1),(62, 8),(63, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_7_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_7_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_smallint;"
    sql "create table test_cast_to_decimal128i_19_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_smallint values (64, -32768),(65, -32767),(66, -9),(67, -1),(68, 0),(69, 1),(70, 9),(71, 32766),(72, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_8_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_8_non_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_smallint;"
    sql "create table test_cast_to_decimal128i_19_9_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_smallint values (73, -32768),(74, -32767),(75, -9),(76, -1),(77, 0),(78, 1),(79, 9),(80, 32766),(81, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_9_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_9_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_smallint;"
    sql "create table test_cast_to_decimal128i_19_18_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_smallint values (82, -9),(83, -8),(84, -1),(85, 0),(86, 1),(87, 8),(88, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_10_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_10_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_smallint;"
    sql "create table test_cast_to_decimal128i_38_0_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_smallint values (89, -32768),(90, -32767),(91, -9),(92, -1),(93, 0),(94, 1),(95, 9),(96, 32766),(97, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_11_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_11_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_smallint;"
    sql "create table test_cast_to_decimal128i_38_19_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_smallint values (98, -32768),(99, -32767),(100, -9),(101, -1),(102, 0),(103, 1),(104, 9),(105, 32766),(106, 32767);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_12_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_12_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_smallint;"
    sql "create table test_cast_to_decimal128i_38_37_from_smallint(f1 int, f2 smallint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_smallint values (107, -9),(108, -8),(109, -1),(110, 0),(111, 1),(112, 8),(113, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_13_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_smallint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_13_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_smallint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_1_0_from_int;"
    sql "create table test_cast_to_decimal128i_1_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_1_0_from_int values (114, -9),(115, -8),(116, -1),(117, 0),(118, 1),(119, 8),(120, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_14_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_14_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_int;"
    sql "create table test_cast_to_decimal128i_19_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_int values (121, -2147483648),(122, -2147483647),(123, -9),(124, -1),(125, 0),(126, 1),(127, 9),(128, 2147483646),(129, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_15_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_int;"
    sql "create table test_cast_to_decimal128i_19_9_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_int values (130, -2147483648),(131, -2147483647),(132, -9),(133, -1),(134, 0),(135, 1),(136, 9),(137, 2147483646),(138, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_16_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_16_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_int;"
    sql "create table test_cast_to_decimal128i_19_18_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_int values (139, -9),(140, -8),(141, -1),(142, 0),(143, 1),(144, 8),(145, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_17_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_17_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_int;"
    sql "create table test_cast_to_decimal128i_38_0_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_int values (146, -2147483648),(147, -2147483647),(148, -9),(149, -1),(150, 0),(151, 1),(152, 9),(153, 2147483646),(154, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_18_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_18_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_int;"
    sql "create table test_cast_to_decimal128i_38_19_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_int values (155, -2147483648),(156, -2147483647),(157, -9),(158, -1),(159, 0),(160, 1),(161, 9),(162, 2147483646),(163, 2147483647);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_19_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_19_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_int;"
    sql "create table test_cast_to_decimal128i_38_37_from_int(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_int values (164, -9),(165, -8),(166, -1),(167, 0),(168, 1),(169, 8),(170, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_20_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_int order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_20_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_int order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_1_0_from_bigint;"
    sql "create table test_cast_to_decimal128i_1_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_1_0_from_bigint values (171, -9),(172, -8),(173, -1),(174, 0),(175, 1),(176, 8),(177, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_21_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_21_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_bigint;"
    sql "create table test_cast_to_decimal128i_19_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_bigint values (178, -9223372036854775808),(179, -9223372036854775807),(180, -9),(181, -1),(182, 0),(183, 1),(184, 9),(185, 9223372036854775806),(186, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_22_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_22_non_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_bigint;"
    sql "create table test_cast_to_decimal128i_19_9_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_bigint values (187, -9999999999),(188, -9000000001),(189, -9000000000),(190, -999999999),(191, -9),(192, -1),(193, 0),(194, 1),(195, 9),(196, 999999999),
      (197, 9000000000),(198, 9000000001),(199, 9999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_23_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_23_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_bigint;"
    sql "create table test_cast_to_decimal128i_19_18_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_bigint values (200, -9),(201, -8),(202, -1),(203, 0),(204, 1),(205, 8),(206, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_24_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_24_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_bigint;"
    sql "create table test_cast_to_decimal128i_38_0_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_bigint values (207, -9223372036854775808),(208, -9223372036854775807),(209, -9),(210, -1),(211, 0),(212, 1),(213, 9),(214, 9223372036854775806),(215, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_25_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_25_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_bigint;"
    sql "create table test_cast_to_decimal128i_38_19_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_bigint values (216, -9223372036854775808),(217, -9223372036854775807),(218, -9),(219, -1),(220, 0),(221, 1),(222, 9),(223, 9223372036854775806),(224, 9223372036854775807);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_26_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_26_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_bigint;"
    sql "create table test_cast_to_decimal128i_38_37_from_bigint(f1 int, f2 bigint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_bigint values (225, -9),(226, -8),(227, -1),(228, 0),(229, 1),(230, 8),(231, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_27_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_bigint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_27_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_bigint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_1_0_from_largeint;"
    sql "create table test_cast_to_decimal128i_1_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_1_0_from_largeint values (232, -9),(233, -8),(234, -1),(235, 0),(236, 1),(237, 8),(238, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_28_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_28_non_strict 'select f1, cast(f2 as decimalv3(1, 0)) from test_cast_to_decimal128i_1_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_0_from_largeint;"
    sql "create table test_cast_to_decimal128i_19_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_0_from_largeint values (239, -9999999999999999999),(240, -9000000000000000001),(241, -9000000000000000000),(242, -999999999999999999),(243, -9),(244, -1),(245, 0),(246, 1),(247, 9),(248, 999999999999999999),
      (249, 9000000000000000000),(250, 9000000000000000001),(251, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_29_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_29_non_strict 'select f1, cast(f2 as decimalv3(19, 0)) from test_cast_to_decimal128i_19_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_9_from_largeint;"
    sql "create table test_cast_to_decimal128i_19_9_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_9_from_largeint values (252, -9999999999),(253, -9000000001),(254, -9000000000),(255, -999999999),(256, -9),(257, -1),(258, 0),(259, 1),(260, 9),(261, 999999999),
      (262, 9000000000),(263, 9000000001),(264, 9999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_30_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_30_non_strict 'select f1, cast(f2 as decimalv3(19, 9)) from test_cast_to_decimal128i_19_9_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_19_18_from_largeint;"
    sql "create table test_cast_to_decimal128i_19_18_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_19_18_from_largeint values (265, -9),(266, -8),(267, -1),(268, 0),(269, 1),(270, 8),(271, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_31_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_31_non_strict 'select f1, cast(f2 as decimalv3(19, 18)) from test_cast_to_decimal128i_19_18_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_0_from_largeint;"
    sql "create table test_cast_to_decimal128i_38_0_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_0_from_largeint values (272, -99999999999999999999999999999999999999),(273, -90000000000000000000000000000000000001),(274, -90000000000000000000000000000000000000),(275, -9999999999999999999999999999999999999),(276, -9),(277, -1),(278, 0),(279, 1),(280, 9),(281, 9999999999999999999999999999999999999),
      (282, 90000000000000000000000000000000000000),(283, 90000000000000000000000000000000000001),(284, 99999999999999999999999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_32_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_32_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128i_38_0_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_19_from_largeint;"
    sql "create table test_cast_to_decimal128i_38_19_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_19_from_largeint values (285, -9999999999999999999),(286, -9000000000000000001),(287, -9000000000000000000),(288, -999999999999999999),(289, -9),(290, -1),(291, 0),(292, 1),(293, 9),(294, 999999999999999999),
      (295, 9000000000000000000),(296, 9000000000000000001),(297, 9999999999999999999);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_33_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_33_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128i_38_19_from_largeint order by 1;'

    sql "drop table if exists test_cast_to_decimal128i_38_37_from_largeint;"
    sql "create table test_cast_to_decimal128i_38_37_from_largeint(f1 int, f2 largeint) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128i_38_37_from_largeint values (298, -9),(299, -8),(300, -1),(301, 0),(302, 1),(303, 8),(304, 9);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_34_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_largeint order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128i_38_37_from_largeint order by 1;'

}