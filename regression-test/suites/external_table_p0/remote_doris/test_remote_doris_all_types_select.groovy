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

suite("test_remote_doris_all_types_select", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    sql """DROP DATABASE IF EXISTS test_remote_doris_all_types_select_db"""

    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_all_types_select_db"""

    sql """
        CREATE TABLE `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` (
          `id` datetime(3) NOT NULL,
          `c_boolean` boolean NULL,
          `c_tinyint` tinyint NULL,
          `c_smallint` smallint NULL,
          `c_int` int NULL,
          `c_bigint` bigint NULL,
          `c_largeint` largeint NULL,
          `c_float` float NULL,
          `c_double` double NULL,
          `c_decimal9` decimal(9,0) NULL,
          `c_decimal18` decimal(18,0) NULL,
          `c_decimal32` decimal(32,0) NULL,
          `c_date` date NULL,
          `c_datetime` datetime NULL,
          `c_char` char(1) NULL,
          `c_varchar` varchar(65533) NULL,
          `c_string` text NULL,
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
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` values('2025-05-18 01:00:00.000', true, -128, -32768, -2147483648, -9223372036854775808, -1234567890123456790, -123.456, -123456.789, -123457, -123456789012346, -1234567890123456789012345678, '1970-01-01', '0000-01-01 00:00:00', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` values('2025-05-18 02:00:00.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` values('2025-05-18 03:00:00.000', false, 127, 32767, 2147483647, 9223372036854775807, 1234567890123456789, 123.456, 123456.789, 123457, 123456789012346, 1234567890123456789012345678, '9999-12-31', '9999-12-31 23:59:59', '', '', '', [], {}, {11, 3.14, "Emily"})
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` values('2025-05-18 04:00:00.000', true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '2023-10-01', '2023-10-01 12:34:56', 'A', 'Hello', 'Hello, Doris!', '["apple", "banana", "orange"]', {"Emily":101,"age":25} , {11, 3.14, "Emily"});
    """

    sql """
        CREATE TABLE `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` (
          `id` datetime(3) NOT NULL,
          `a_boolean` array<boolean> NULL,
          `a_tinyint` array<tinyint> NULL,
          `a_smallint` array<smallint> NULL,
          `a_int` array<int> NULL,
          `a_bigint` array<bigint> NULL,
          `a_largeint` array<largeint> NULL,
          `a_float` array<float> NULL,
          `a_double` array<double> NULL,
          `a_decimal9` array<decimal(9,0)> NULL,
          `a_decimal18` array<decimal(18,0)> NULL,
          `a_decimal32` array<decimal(32,0)> NULL,
          `a_date` array<date> NULL,
          `a_datetime` array<datetime> NULL,
          `a_char` array<char(1)> NULL,
          `a_varchar` array<varchar(65533)> NULL,
          `a_string` array<text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` values('2025-05-18 01:00:00.000', [true], [-128], [-32768], [-2147483648], [-9223372036854775808], [-1234567890123456790], [-123.456], [-123456.789], [-123457], [-123456789012346], [-1234567890123456789012345678], ['0000-01-01'], ['0000-01-01 00:00:00'], ['A'], ['Hello'], ['Hello, Doris!'])
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` values('2025-05-18 02:00:00.000', [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL], [NULL])
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` values('2025-05-18 03:00:00.000', [false], [127], [32767], [2147483647], [9223372036854775807], [1234567890123456789], [123.456], [123456.789], [123457], [123456789012346], [1234567890123456789012345678], ['9999-12-31'], ['9999-12-31 23:59:59'], [''], [''], [''])
    """
    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` values('2025-05-18 04:00:00.000', [true], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], ['2023-10-01'], ['2023-10-01 12:34:56'], ['A'], ['Hello'], ['Hello, Doris!']);
    """

    sql """
        CREATE TABLE `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t3` (
          `id` datetime NOT NULL,
          `datetime_0` datetime(0) NULL,
          `datetime_1` datetime(1) NULL,
          `datetime_3` datetime(2) NULL,
          `datetime_4` datetime(3) NULL,
          `datetime_5` datetime(4) NULL,
          `datetime_6` datetime(5) NULL,
          `datetime_7` datetime(6) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t3` values('2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111', '2025-05-18 01:00:00.111111');
    """


    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_all_types_select_catalog`
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

    qt_sql """
        select * from `test_remote_doris_all_types_select_catalog`.`test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t` order by id
    """

    qt_sql """
        select * from `test_remote_doris_all_types_select_catalog`.`test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t2` order by id
    """

    qt_sql """
        select * from `test_remote_doris_all_types_select_catalog`.`test_remote_doris_all_types_select_db`.`test_remote_doris_all_types_select_t3` order by id
    """

    sql """ DROP DATABASE IF EXISTS test_remote_doris_all_types_select_db """
    sql """ DROP CATALOG IF EXISTS `test_remote_doris_all_types_select_catalog` """
}
