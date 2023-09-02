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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("window_funnel") {
    sql "SET enable_nereids_planner=false"
    def tableName = "windowfunnel_test"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                xwho varchar(50) NULL COMMENT 'xwho',
                xwhen datetime COMMENT 'xwhen',
                xwhat int NULL COMMENT 'xwhat'
            )
            DUPLICATE KEY(xwho)
            DISTRIBUTED BY HASH(xwho) BUCKETS 3
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 16:15:01', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 19:05:04', 4)"

    qt_window_funnel """ select
                             window_funnel(
                                1,
                                'default',
                                t.xwhen,
                                t.xwhat = 1,
                                t.xwhat = 2
                             ) AS level
                        from ${tableName} t;
                 """
    qt_window_funnel """ select
                             window_funnel(
                                20000,
                                'default',
                                t.xwhen,
                                t.xwhat = 1,
                                t.xwhat = 2
                             ) AS level
                        from ${tableName} t;
                 """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 16:15:01.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 19:05:04.111111', 4)"

    qt_window_funnel """
        select
            window_funnel(
                1,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
                ) AS level
        from ${tableName} t;
    """
    qt_window_funnel """
        select
            window_funnel(
                20000,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    for (String backendId in backendId_to_backendIP.keySet()) {
        String be_host = backendId_to_backendIP[backendId]
        String be_http_port = backendId_to_backendHttpPort[backendId]
        curl("POST", "http://${be_host}:${be_http_port}/api/update_config?enable_window_funnel_function_v2=true")
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:03.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 14:15:01.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    qt_window_funnel_deduplication """
        select
            window_funnel(
                20000,
                'deduplication',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3,
                t.xwhat = 4
                ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 14:15:01.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    qt_window_funnel_fixed """
        select
            window_funnel(
                20000,
                'fixed',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 4,
                t.xwhat = 3
                ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 14:15:01.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    qt_window_funnel_fixed """
        select
            window_funnel(
                20000,
                'fixed',
                t.xwhen,
                t.xwhat = 4,
                t.xwhat = 3,
                t.xwhat = 2,
                t.xwhat = 1
                ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:03.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    qt_window_funnel_increase """
        select
            window_funnel(
                20000,
                'increase',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3,
                t.xwhat = 4
                ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            xwho varchar(50) NULL COMMENT 'xwho',
            xwhen datetimev2(3) COMMENT 'xwhen',
            xwhat int NULL COMMENT 'xwhat'
        )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "INSERT into ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 3)"
    sql "INSERT INTO ${tableName} (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    qt_window_funnel_increase """
        select
            window_funnel(
                20000,
                'increase',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3,
                t.xwhat = 4
                ) AS level
        from ${tableName} t;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
