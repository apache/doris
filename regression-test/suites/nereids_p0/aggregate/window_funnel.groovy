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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("window_funnel") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
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

    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe?conf_item=be_exec_version")

    def command = strBuilder.toString()
    def process = command.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    def configJson = response.data.rows
    def beExecVersion = 0
    for (Object conf: configJson) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "be_exec_version") {
            beExecVersion = ((Map<String, String>) conf).get("Value").toInteger()
        }
    }
    if (beExecVersion < 3) {
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
        qt_window_funnel_deduplication_compat """
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
        logger.warn("Be exec version(${beExecVersion}) is less than 3, skip window_funnel mode test")
        return
    } else {
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
        qt_window_funnel_deduplication_compat """
            select
                window_funnel(
                    20000,
                    'default',
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

    sql """
        CREATE TABLE windowfunnel_test (
                    `xwho` varchar(50) NULL COMMENT 'xwho',
                    `xwhen` datetime COMMENT 'xwhen',
                    `xwhat` int NULL COMMENT 'xwhat'
                    )
        DUPLICATE KEY(xwho)
        DISTRIBUTED BY HASH(xwho) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT into windowfunnel_test (xwho, xwhen, xwhat) values ('1', '2022-03-12 10:41:00', 1),
                                                           ('1', '2022-03-12 13:28:02', 2),
                                                           ('1', '2022-03-12 16:15:01', 3),
                                                           ('1', '2022-03-12 19:05:04', 4);
    """
    qt_window_funnel_neq """
        select window_funnel(3600 * 24, 'default', t.xwhen, t.xwhat = 1, t.xwhat != 2,t.xwhat=3 ) AS level from windowfunnel_test t;
    """

    sql """ DROP TABLE IF EXISTS windowfunnel_test """
    sql """
        CREATE TABLE windowfunnel_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 3),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    // test default mode
    qt_window_funnel_default0 """
        SELECT
            user_id,
            window_funnel(3600 * 3, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    // in 5 minutes
    qt_window_funnel_default1 """
        SELECT
            user_id,
            window_funnel(300, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    // in 30 seconds
    qt_window_funnel_default2 """
        SELECT
            user_id,
            window_funnel(30, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    qt_window_funnel_default3 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name = '登录',event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    qt_window_funnel_default4 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name = '访问',event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    qt_window_funnel_default5 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name = '登录', event_name = '登录', event_name = '登录', event_name = '登录',event_name = '登录', event_name = '登录') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    // complicate expressions
    qt_window_funnel_default6 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name != '登陆', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
            GROUP BY user_id
            order BY user_id;
    """
    qt_window_funnel_default7 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name != '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id;
    """
    qt_window_funnel_default8 """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp,
                          event_name = '登录' AND phone_brand in ('HONOR', 'XIAOMI', 'VIVO') AND tab_num not in (4, 5),
                          event_name = '访问' AND tab_num not in (4, 5),
                          event_name = '下单' AND tab_num not in (6, 7),
                          event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id;
    """

    sql """ DROP TABLE IF EXISTS windowfunnel_test """
    sql """
        CREATE TABLE windowfunnel_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    // test multiple matched event list, output the longest match
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 3),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5),
            (100123, '登录', '2022-05-14 13:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 13:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 13:04:00', "HONOR", 3),
            (100123, '付款', '2022-05-14 13:10:00', 'HONOR', 4),
            (100126, '登录', '2022-05-15 14:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 14:01:00', 'HONOR', 2),
            (100126, '下单', '2022-05-15 14:02:00', 'HONOR', 3),
            (100126, '付款', '2022-05-15 14:03:00', 'HONOR', 4);
    """
    qt_window_funnel_default9 """
        SELECT
            user_id,
            window_funnel(3600, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """

    // test deduplication mode
    sql """ DROP TABLE IF EXISTS windowfunnel_test """
    sql """
        CREATE TABLE windowfunnel_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '登录', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录1', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录2', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录3', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录4', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录5', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_deduplication0 """
        SELECT
            user_id,
            window_funnel(3600, "deduplication", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    sql """ truncate table windowfunnel_test; """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '登录1', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录2', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录3', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录4', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录5', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '访问', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_deduplication1 """
        SELECT
            user_id,
            window_funnel(3600, "deduplication", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    sql """ truncate table windowfunnel_test; """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '登录1', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录2', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录3', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录4', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '登录5', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '下单', '2022-05-14 10:04:00', 'HONOR', 3),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_deduplication2 """
        SELECT
            user_id,
            window_funnel(3600, "deduplication", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """


    // test fixed mode
    sql """ truncate table windowfunnel_test; """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '登录', '2022-05-14 10:03:00', 'HONOR', 3),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_fixed0 """
        SELECT
            user_id,
            window_funnel(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_test """
    sql """
        CREATE TABLE windowfunnel_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(event_timestamp) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '登录2', '2022-05-14 10:03:00', 'HONOR', 3),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_fixed1 """
        SELECT
            user_id,
            window_funnel(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """

    // test increase mode
    sql """ DROP TABLE IF EXISTS windowfunnel_test """
    sql """
        CREATE TABLE windowfunnel_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:04:00', 'HONOR', 4),
            (100125, '登录', '2022-05-15 11:00:00', 'XIAOMI', 1),
            (100125, '访问', '2022-05-15 11:01:00', 'XIAOMI', 2),
            (100125, '下单', '2022-05-15 11:02:00', 'XIAOMI', 6),
            (100126, '登录', '2022-05-15 12:00:00', 'IPHONE', 1),
            (100126, '访问', '2022-05-15 12:01:00', 'HONOR', 2),
            (100127, '登录', '2022-05-15 11:30:00', 'VIVO', 1),
            (100127, '访问', '2022-05-15 11:31:00', 'VIVO', 5);
    """
    qt_window_funnel_increase0 """
        SELECT
            user_id,
            window_funnel(3600, "increase", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_test
        GROUP BY user_id
        order BY user_id
    """

}
