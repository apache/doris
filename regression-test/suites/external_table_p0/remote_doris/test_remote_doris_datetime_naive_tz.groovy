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

// Regression for apache/doris#65741 (federation datetime correctness).
// When a DATETIME / DATETIMEV2 value is read back through a type=doris catalog with
// use_arrow_flight=true, the wall-clock value must round-trip unchanged with no timezone shift.
// This holds under both the default timezone-aware mapping and the opt-in timezone-naive one,
// because DataTypeDateTimeV2SerDe::read_column_from_arrow interprets the Arrow timestamp by its
// own timezone metadata (empty -> UTC, otherwise the session timezone).
suite("test_remote_doris_datetime_naive_tz", "p0,external") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    String remote_doris_thrift_port = context.config.otherConfigs.get("extFeThriftPort")

    def showres = sql "show frontends";
    remote_doris_arrow_port = showres[0][6]
    remote_doris_http_port = showres[0][3]
    remote_doris_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${remote_doris_arrow_port}, http: ${remote_doris_http_port}, thrift: ${remote_doris_thrift_port}")

    sql """DROP DATABASE IF EXISTS test_remote_doris_datetime_naive_tz_db"""
    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_datetime_naive_tz_db"""

    sql """
        CREATE TABLE `test_remote_doris_datetime_naive_tz_db`.`t` (
          `k` int NOT NULL,
          `dt0` datetime(0) NULL,
          `dt3` datetime(3) NULL,
          `dt6` datetime(6) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Row 1 = the exact value from the issue report.
    sql """
        INSERT INTO `test_remote_doris_datetime_naive_tz_db`.`t` values
            (1, '2026-07-02 01:36:22', '2026-07-02 01:36:22.069', '2026-07-02 01:36:22.069504')
    """
    // Row 2 = a late-night value: a session-timezone shift (e.g. +08:00) would push it to the
    // next day (07:30), so any regression on the receive side is glaringly visible.
    sql """
        INSERT INTO `test_remote_doris_datetime_naive_tz_db`.`t` values
            (2, '2026-07-02 23:30:45', '2026-07-02 23:30:45.654', '2026-07-02 23:30:45.654321')
    """

    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_datetime_naive_tz_catalog`
    """

    sql """
        CREATE CATALOG `test_remote_doris_datetime_naive_tz_catalog` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    qt_sql """
        select k, dt0, dt3, dt6 from `test_remote_doris_datetime_naive_tz_catalog`.`test_remote_doris_datetime_naive_tz_db`.`t` order by k
    """

    // Test objects are intentionally left in place after the assertions for failure
    // investigation; the pre-setup DROP ... IF EXISTS above keeps reruns deterministic.
}
