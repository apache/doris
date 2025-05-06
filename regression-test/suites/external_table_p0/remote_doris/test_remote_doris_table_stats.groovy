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

suite("test_remote_doris_table_stats", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    sql """DROP DATABASE IF EXISTS test_remote_doris_table_stats_db"""

    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_table_stats_db"""

    sql """
        CREATE TABLE `test_remote_doris_table_stats_db`.`test_remote_doris_table_stats_t1` (
          `id` datetime(3) NOT NULL,
          `c_boolean` boolean NULL DEFAULT 'true',
          `c_tinyint` tinyint NULL DEFAULT 1,
          `c_smallint` smallint NULL DEFAULT 1,
          `c_int` int NULL DEFAULT 1,
          `c_bigint` bigint NULL DEFAULT 1,
          `c_largeint` largeint NULL DEFAULT 1,
          `c_float` float NULL DEFAULT 1,
          `c_double` double NULL DEFAULT 1,
          `c_decimal9` decimal(9,0) NULL DEFAULT 1,
          `c_decimal18` decimal(18,0) NULL DEFAULT 1,
          `c_decimal32` decimal(32,0) NULL DEFAULT 1,
          `c_date` date NULL DEFAULT '2025-08-18',
          `c_datetime` datetime NULL DEFAULT '2025-08-18 20:00:00',
          `c_char` char(1) NULL DEFAULT 'd',
          `c_varchar` varchar(65533) NULL DEFAULT 'd',
          `c_string` text NULL DEFAULT 'd',
          `c_array_s` array<text> NULL,
          `c_map` MAP<STRING, INT> NULL,
          `c_struct` STRUCT<f1:INT,f2:FLOAT,f3:STRING>  NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_table_stats_catalog`
    """

    sql """
    CREATE CATALOG `test_remote_doris_all_types_select_catalog` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}'
        );
    """

    sql """
        INSERT INTO `test_remote_doris_table_stats_db`.`test_remote_doris_table_stats_t1` values('2025-05-18 01:00:00.000', true, -128, -32768, -2147483648, -9223372036854775808, -1234567890123456790, -123.456, -123456.789, -123457, -123456789012346, -1234567890123456789012345678, '1970-01-01', '0000-01-01 00:00:00', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `test_remote_doris_table_stats_db`.`test_remote_doris_table_stats_t1` values('2025-05-18 02:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    sql """
        INSERT INTO `test_remote_doris_table_stats_db`.`test_remote_doris_table_stats_t1` values('2025-05-18 03:00:00.000', false, 127, 32767, 2147483647, 9223372036854775807, 1234567890123456789, 123.456, 123456.789, 123457, 123456789012346, 1234567890123456789012345678, '9999-12-31', '9999-12-31 23:59:59', '', '', '', [], {}, {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `test_remote_doris_table_stats_db`.`test_remote_doris_table_stats_t1` values('2025-05-18 04:00:00.000', true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '2023-10-01', '2023-10-01 12:34:56', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"});
    """

    sql """use test_remote_doris_table_stats_catalog.test_remote_doris_table_stats_db"""
    sql """analyze table test_remote_doris_table_stats_t1 with sync"""

    def result = sql """ show table stats test_remote_doris_table_stats_t1; """
    println(result[0][2])

    sql """DROP DATABASE IF EXISTS test_remote_doris_table_stats_db"""
    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_table_stats_catalog`
    """
}
