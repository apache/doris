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

suite("window_funnel_v2") {

    // ==================== Basic DEFAULT mode tests ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE IF NOT EXISTS windowfunnel_v2_test (
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
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 16:15:01', 3)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 19:05:04', 4)"

    // window=1 second, only event1 matches (events too far apart)
    order_qt_v2_default_small_window """
        select
            window_funnel(
                1,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from windowfunnel_v2_test t;
    """
    // window=20000 seconds, both events match
    order_qt_v2_default_large_window """
        select
            window_funnel(
                20000,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from windowfunnel_v2_test t;
    """

    // ==================== DateTimeV2 precision test ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE IF NOT EXISTS windowfunnel_v2_test (
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
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 16:15:01.111111', 3)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 19:05:04.111111', 4)"

    order_qt_v2_datetimev2_small """
        select
            window_funnel(
                1,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from windowfunnel_v2_test t;
    """
    order_qt_v2_datetimev2_large """
        select
            window_funnel(
                20000,
                'default',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2
            ) AS level
        from windowfunnel_v2_test t;
    """

    // ==================== Multi-user default mode tests ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE windowfunnel_v2_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
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
    // 3 hour window
    order_qt_v2_multi_user_default0 """
        SELECT
            user_id,
            window_funnel(3600 * 3, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """
    // 5 minute window
    order_qt_v2_multi_user_default1 """
        SELECT
            user_id,
            window_funnel(300, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """
    // 30 second window
    order_qt_v2_multi_user_default2 """
        SELECT
            user_id,
            window_funnel(30, "default", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """
    // complicate expressions with != condition
    order_qt_v2_default_neq """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp, event_name = '登录', event_name != '登陆', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id;
    """
    // Complex filter conditions
    order_qt_v2_default_complex """
        SELECT
            user_id,
            window_funnel(3600000000, "default", event_timestamp,
                          event_name = '登录' AND phone_brand in ('HONOR', 'XIAOMI', 'VIVO') AND tab_num not in (4, 5),
                          event_name = '访问' AND tab_num not in (4, 5),
                          event_name = '下单' AND tab_num not in (6, 7),
                          event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id;
    """

    // ==================== DEDUPLICATION mode tests ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE windowfunnel_v2_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
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
    order_qt_v2_deduplication0 """
        SELECT
            user_id,
            window_funnel(3600, "deduplication", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // Test dedup with duplicate event2 (访问)
    sql """ truncate table windowfunnel_v2_test; """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '登录1', '2022-05-14 10:04:00', 'HONOR', 3),
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
    order_qt_v2_deduplication1 """
        SELECT
            user_id,
            window_funnel(3600, "deduplication", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // ==================== FIXED mode tests (StarRocks-style semantics) ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE windowfunnel_v2_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
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
    // Note: In V2 fixed mode (StarRocks-style), unmatched rows don't break the chain.
    // The chain only breaks when a matched event's predecessor level hasn't been matched.
    order_qt_v2_fixed0 """
        SELECT
            user_id,
            window_funnel(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // Test fixed mode where event order in conditions doesn't match data order
    sql """ truncate table windowfunnel_v2_test; """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4);
    """
    order_qt_v2_fixed_reorder """
        select
            window_funnel(
                20000,
                'fixed',
                t.event_timestamp,
                t.event_name = '登录',
                t.event_name = '访问',
                t.event_name = '付款',
                t.event_name = '下单'
            ) AS level
        from windowfunnel_v2_test t;
    """

    // ==================== INCREASE mode tests ====================
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE windowfunnel_v2_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
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
    order_qt_v2_increase0 """
        SELECT
            user_id,
            window_funnel(3600, "increase", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // Test increase mode with same-timestamp events
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE IF NOT EXISTS windowfunnel_v2_test (
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
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:41:00.111111', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:28:02.111111', 3)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 15:05:04.111111', 4)"
    order_qt_v2_increase_same_ts """
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
        from windowfunnel_v2_test t;
    """

    // ==================== V2 FIXED mode key difference from V1 ====================
    // In V1, unmatched rows (rows that match no event condition) break the chain in FIXED mode.
    // In V2, unmatched rows are not stored, so only matched events with level jumps break the chain.
    // This test shows the behavioral difference.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE windowfunnel_v2_test(
            user_id BIGINT,
            event_name VARCHAR(64),
            event_timestamp datetime,
            phone_brand varchar(64),
            tab_num int
        ) distributed by hash(user_id) buckets 3 properties("replication_num"="1");
    """
    sql """
        INSERT INTO windowfunnel_v2_test VALUES
            (100123, '登录', '2022-05-14 10:01:00', 'HONOR', 1),
            (100123, '访问', '2022-05-14 10:02:00', 'HONOR', 2),
            (100123, '登录2', '2022-05-14 10:03:00', 'HONOR', 3),
            (100123, '下单', '2022-05-14 10:04:00', "HONOR", 4),
            (100123, '付款', '2022-05-14 10:10:00', 'HONOR', 4);
    """
    // V2 fixed mode: 登录2 doesn't match any condition, so it's not stored.
    // The chain 登录->访问->下单->付款 is unbroken because there are no level jumps.
    // V1 would return 2 here (登录2 physically breaks adjacency), V2 returns 4.
    order_qt_v2_fixed_vs_v1 """
        SELECT
            user_id,
            window_funnel(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // ==================== Test using window_funnel_v2 explicit name ====================
    order_qt_v2_explicit_name """
        SELECT
            user_id,
            window_funnel_v2(3600, "fixed", event_timestamp, event_name = '登录', event_name = '访问', event_name = '下单', event_name = '付款') AS level
        FROM windowfunnel_v2_test
        GROUP BY user_id
        order BY user_id
    """

    // ==================== INCREASE mode: event-0 re-occurrence bug fix ====================
    // Regression test for the bug where a later event-0 overwrites events_timestamp[0]
    // and breaks the INCREASE mode strict-increase check for an already-valid chain.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE IF NOT EXISTS windowfunnel_v2_test (
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
    // Case 1: Old chain (from t=0) is valid and completes all 3 levels.
    // The duplicate event-0 at t=50 should not destroy it.
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:50.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:50.000', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:01:00.000', 3)"
    order_qt_v2_increase_event0_overwrite """
        select
            window_funnel(
                100,
                'increase',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3
            ) AS level
        from windowfunnel_v2_test t;
    """

    // Case 2: Old chain can't complete (window too small), new chain is better.
    sql """ truncate table windowfunnel_v2_test; """
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:50.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:51.000', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:52.000', 3)"
    order_qt_v2_increase_new_chain_better """
        select
            window_funnel(
                5,
                'increase',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3
            ) AS level
        from windowfunnel_v2_test t;
    """

    // Case 3: Old chain is better (reached level 2), new chain only reaches level 1.
    sql """ truncate table windowfunnel_v2_test; """
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:10.000', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:50.000', 1)"
    order_qt_v2_increase_old_chain_better """
        select
            window_funnel(
                100,
                'increase',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3
            ) AS level
        from windowfunnel_v2_test t;
    """

    // ==================== DEDUPLICATION mode: same-row multi-event bug fix ====================
    // Regression test for the bug where deduplication mode incorrectly breaks the chain
    // when a single row matches multiple conditions including E0.
    // V1 returns 3, V2 was returning 2 before the fix.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
    sql """
        CREATE TABLE IF NOT EXISTS windowfunnel_v2_test (
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
    // Case 1: Rows matching both E0+E1 or E0+E2 simultaneously.
    // Row 0 (t=10:00): xwhat=1 → matches E0 (xwhat=1) and E1 (xwhat!=2)
    // Row 1 (t=11:00): xwhat=1 → matches E0 (xwhat=1) and E1 (xwhat!=2)
    // Row 2 (t=12:00): xwhat=3 → matches E0 (xwhat=1 false, but see below), E1 (xwhat!=2) and E2 (xwhat=3)
    // Using conditions: xwhat=1, xwhat!=2, xwhat=3
    // Row 0: E0=T(1=1), E1=T(1!=2), E2=F(1!=3) → same-row E0+E1
    // Row 1: E0=T(1=1), E1=T(1!=2), E2=F(1!=3) → same-row E0+E1
    // Row 2: E0=F(3!=1), E1=T(3!=2), E2=T(3=3) → same-row E1+E2
    // Expected chain: E0@row0 → E1@row1 → E2@row2 = 3
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 11:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 12:00:00.000', 3)"
    order_qt_v2_dedup_same_row_multi_event """
        select
            window_funnel(
                86400,
                'deduplication',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat != 2,
                t.xwhat = 3
            ) AS level
        from windowfunnel_v2_test t;
    """

    // Case 2: True duplicate from a different row should still break the chain.
    sql """ truncate table windowfunnel_v2_test; """
    sql "INSERT into windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 10:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 11:00:00.000', 2)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 12:00:00.000', 1)"
    sql "INSERT INTO windowfunnel_v2_test (xwho, xwhen, xwhat) VALUES('1', '2022-03-12 13:00:00.000', 3)"
    order_qt_v2_dedup_true_dup_breaks """
        select
            window_funnel(
                86400,
                'deduplication',
                t.xwhen,
                t.xwhat = 1,
                t.xwhat = 2,
                t.xwhat = 3
            ) AS level
        from windowfunnel_v2_test t;
    """

    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """

    // ==================== FIXED mode: same-row multi-condition level jump fix ====================
    // When a row matches both a high-index condition (e.g., c3) and a low-index condition (e.g., c2),
    // the high-index event was processed first (descending order) and triggered a spurious level jump
    // before the low-index event could advance the chain. This test verifies the fix.

    // Minimal reproduction: 3 conditions, 2 rows
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_multi_cond """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_multi_cond (
            ts datetime,
            val int
        ) DUPLICATE KEY(ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_multi_cond VALUES
            ('2020-01-01 00:00:00', 3),
            ('2020-01-02 00:00:00', 1);
    """
    // Row 1: val=3 -> c1(>2)=T, c2(<5)=T, c3(>0)=T  (starts chain)
    // Row 2: val=1 -> c1=F, c2(<5)=T, c3(>0)=T
    // Bug: E2(c3) from row 2 triggered level jump before E1(c2) could advance.
    // Fix: same-row look-ahead detects E1 can advance, skips level jump. Result=2.
    order_qt_v2_fixed_same_row_level_jump """
        SELECT window_funnel(86400, 'fixed', ts, val > 2, val < 5, val > 0) AS level
        FROM windowfunnel_v2_fixed_multi_cond;
    """

    // 4 conditions with overlapping column-based predicates
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_multi_cond """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_multi_cond (
            col_date date NULL,
            col_int int NULL,
            col_datetime datetime NULL,
            pk int
        ) DUPLICATE KEY(col_date, col_int)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_multi_cond (pk, col_int, col_date, col_datetime) VALUES
            (0,2,'2011-03-24','2009-09-17 17:25:39'),(1,null,'2023-12-10','2001-09-19 20:24:22'),
            (2,1,null,'2003-04-04 02:59:52'),(3,8,'2023-12-16','2004-02-13 03:59:09'),
            (4,5,'2023-12-18','2015-09-09 23:02:51'),(5,1,'2023-12-14','2001-10-21 07:04:03'),
            (6,3,'2023-12-10','2009-04-07 10:55:27'),(7,null,'2023-12-14','2018-11-02 09:18:56'),
            (8,1,'2023-12-11','2015-05-03 17:34:12'),(9,1,'2023-12-12','2010-12-25 06:21:52'),
            (10,3,null,'2008-06-02 22:49:15'),(11,0,'2023-12-12','2010-11-13 22:32:47'),
            (12,1,'2023-12-16','2019-04-28 20:36:03'),(13,null,null,'2019-01-01 21:24:32'),
            (14,3,'2023-12-15','2006-08-17 21:47:52'),(15,3,null,'2015-12-23 05:58:29'),
            (16,5,'2013-09-15','2018-08-20 22:15:56'),(17,5,'2023-12-11','2017-01-17 11:34:56'),
            (18,7,'2023-12-18','2016-01-16 13:00:40'),(19,null,'2023-12-13','2003-03-16 01:28:28'),
            (20,0,'2023-12-16','2006-08-28 22:48:19'),(21,8,'2013-10-28','2007-01-19 07:52:53'),
            (22,3,'2023-12-17','2001-06-05 18:32:05'),(23,2,'2023-12-17','2001-05-15 22:39:45'),
            (24,null,'2023-12-18','2015-02-06 18:44:55'),(25,null,'2023-12-11','2004-02-22 19:46:22'),
            (26,0,'2023-12-17','2001-05-20 05:25:26'),(27,7,'2023-12-15','2015-05-07 15:56:20'),
            (28,0,'2023-12-16','2016-02-10 04:26:23'),(29,0,'2023-12-18','2019-09-04 05:15:31'),
            (30,1,'2023-12-11','2000-12-08 10:19:58'),(31,9,'2023-12-14','2004-07-11 03:51:22'),
            (32,5,'2023-12-16','2013-11-18 16:32:18'),(33,6,null,'2000-11-13 05:31:42'),
            (34,7,'2023-12-18','2013-01-02 22:46:46'),(35,4,'2023-12-12','2008-10-23 09:22:02'),
            (36,2,'2023-12-16','2006-01-18 13:41:57'),(37,5,'2023-12-14','2008-05-27 21:09:03'),
            (38,2,'2023-12-15','2007-01-15 01:30:56'),(39,1,'2023-12-18','2003-04-10 16:23:13'),
            (40,null,'2023-12-18','2018-03-25 20:34:39'),(41,4,'2023-12-15','2007-10-06 00:49:06'),
            (42,7,'2023-12-11','2015-11-11 23:53:16'),(43,null,'2023-12-10','2002-05-26 11:17:20'),
            (44,6,'2023-12-14','2003-04-11 05:05:55'),(45,null,'2014-08-07','2000-10-02 00:55:06'),
            (46,2,'2023-12-16','2018-08-26 22:48:02'),(47,2,'2023-12-11','2004-12-15 08:38:08'),
            (48,5,'2019-01-19','2000-11-09 19:21:35'),(49,6,'2023-12-12','2010-11-21 05:54:00');
    """
    // c1: col_int>2, c2: col_date<'2023-12-12', c3: col_int=5, c4: col_date>'2009-10-14'
    // Many rows match both c2 and c4 (e.g., col_date='2023-12-11'). In the buggy code,
    // c4's event triggered a level jump before c2 could advance. Expected: 2.
    order_qt_v2_fixed_multi_cond_complex """
        SELECT WINDOW_FUNNEL(1736123071, 'fixed', col_datetime,
            col_int > 2, col_date < '2023-12-12', col_int = 5, col_date > '2009-10-14')
        FROM windowfunnel_v2_fixed_multi_cond;
    """

    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_multi_cond """

    // ==================== FIXED mode: continuation E0 should not restart chain ====================
    // When a row matches both the expected next condition (e.g., c2) and c1, the c2 event
    // advances the chain while c1 (stored as continuation) should NOT restart a new chain.
    // This matches V1's row-based semantics.

    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_e0_skip """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_e0_skip (
            ts datetime,
            val int
        ) DUPLICATE KEY(ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_e0_skip VALUES
            ('2020-01-01 00:00:00', 1),
            ('2020-01-02 00:00:00', 3),
            ('2020-01-03 00:00:00', -1);
    """
    // r0: val=1  -> c1(val>0)=T, c2(val>2)=F, c3(val<0)=F. Starts chain.
    // r1: val=3  -> c1=T, c2=T, c3=F. c2 advances chain; c1(cont) should NOT restart.
    // r2: val=-1 -> c1=F, c2=F, c3=T. c3 advances to level 2.
    // Expected: 3 (c1@r0 -> c2@r1 -> c3@r2)
    order_qt_v2_fixed_e0_skip """
        SELECT window_funnel(864000, 'fixed', ts, val > 0, val > 2, val < 0) AS level
        FROM windowfunnel_v2_fixed_e0_skip;
    """

    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_e0_skip """

    // ==================== FIXED mode: comprehensive same-row multi-condition edge cases ====================

    // Test: single row matching all conditions → only E0 counts (no next row for c2)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """ INSERT INTO windowfunnel_v2_fixed_edge VALUES ('2020-01-01 00:00:00', 3) """
    order_qt_v2_fixed_edge_single_row_all_match """
        SELECT window_funnel(864000, 'fixed', ts, val>=0, val>=1, val>=2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: R0:E0, R1:E0+E1+E2 (next row matches all conditions)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 0), ('2020-01-02 00:00:00', 3);
    """
    order_qt_v2_fixed_edge_next_row_all """
        SELECT window_funnel(864000, 'fixed', ts, val>=0, val>=1, val>=2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: R0:E0, R1:E1+E2 (not E0), R2:E2 only
    // c3 at R2 doesn't match → chain stops at level 1
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 3), ('2020-01-03 00:00:00', 2);
    """
    order_qt_v2_fixed_edge_e1e2_no_e0 """
        SELECT window_funnel(864000, 'fixed', ts, val=1, val>1, val>2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: 4 conditions progressive (R0:c1, R1:c1+c2+c3, R2:all)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 0), ('2020-01-02 00:00:00', 2), ('2020-01-03 00:00:00', 3);
    """
    order_qt_v2_fixed_edge_4cond_progressive """
        SELECT window_funnel(864000, 'fixed', ts, val>=0, val>=1, val>=2, val>=3) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: R0:E0+E1, R1:E0+E1+E2 (both rows match c1+c2)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 2);
    """
    order_qt_v2_fixed_edge_both_rows_multi """
        SELECT window_funnel(864000, 'fixed', ts, val>=1, val>=1, val>=2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: 4 rows, 4 conditions, each row adds one more matching condition
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 0), ('2020-01-02 00:00:00', 1),
            ('2020-01-03 00:00:00', 2), ('2020-01-04 00:00:00', 3);
    """
    order_qt_v2_fixed_edge_full_chain """
        SELECT window_funnel(864000, 'fixed', ts, val>=0, val>=1, val>=2, val>=3) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: R0:E0, R1:only E2 (c2 not matched) → level jump breaks chain
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 5);
    """
    order_qt_v2_fixed_edge_level_jump_break """
        SELECT window_funnel(864000, 'fixed', ts, val=1, val=2, val>2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: E0 restart from different row should work normally
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 5),
            ('2020-01-03 00:00:00', 1), ('2020-01-04 00:00:00', 2);
    """
    order_qt_v2_fixed_edge_e0_restart_diff_row """
        SELECT window_funnel(864000, 'fixed', ts, val>0, val=2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: Window boundary with multi-condition same-row (exceeds 1s window)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 0), ('2020-01-01 00:00:02', 2);
    """
    order_qt_v2_fixed_edge_window_boundary """
        SELECT window_funnel(1, 'fixed', ts, val>=0, val>=1, val>=2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: Multiple restarts find best chain
    // c1@R0→c2@R1 then R2 has c2 again (breaks). c1@R1→c2@R2→c3@R3 succeeds → level 3.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 2),
            ('2020-01-03 00:00:00', 2), ('2020-01-04 00:00:00', 3);
    """
    order_qt_v2_fixed_edge_multi_restart_best """
        SELECT window_funnel(864000, 'fixed', ts, val>0, val>1, val>2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: Multiple restarts with many E0-only rows
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 1),
            ('2020-01-03 00:00:00', 1), ('2020-01-04 00:00:00', 2),
            ('2020-01-05 00:00:00', 3);
    """
    order_qt_v2_fixed_edge_many_e0_restarts """
        SELECT window_funnel(864000, 'fixed', ts, val>0, val>1, val>2) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: Irrelevant events are skipped in V2 FIXED mode (4.1+ behavior)
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 0),
            ('2020-01-03 00:00:00', 2), ('2020-01-04 00:00:00', 3);
    """
    // R1(val=0) matches NO condition → V2 skips it → E0@R0→E1@R2→E2@R3 = 3
    order_qt_v2_fixed_edge_irrelevant_skip """
        SELECT window_funnel(864000, 'fixed', ts, val=1, val=2, val=3) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: "Relevant-but-wrong row" breaks chain in FIXED mode.
    // R2 matches c2 (relevant) but not c3 (expected) → chain breaks at R2.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 1), ('2020-01-02 00:00:00', 2),
            ('2020-01-03 00:00:00', 2), ('2020-01-04 00:00:00', 3);
    """
    // c1@R0→c2@R1, R2 has c2 again (val=2, not c3=val=3) → breaks chain.
    // No other row matches c1(val=1), so no multi-pass recovery. Expected: 2.
    order_qt_v2_fixed_edge_relevant_but_wrong """
        SELECT window_funnel(864000, 'fixed', ts, val=1, val=2, val=3) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """

    // Test: "Relevant-but-wrong row" with no better starting point → breaks chain.
    // Only c1 at R0. R1 has c2, R2 has c2 again (not c3). No later c1 to restart.
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
    sql """
        CREATE TABLE windowfunnel_v2_fixed_edge (ts datetime, val int)
        DUPLICATE KEY(ts) DISTRIBUTED BY HASH(ts) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO windowfunnel_v2_fixed_edge VALUES
            ('2020-01-01 00:00:00', 10), ('2020-01-02 00:00:00', 5),
            ('2020-01-03 00:00:00', 5), ('2020-01-04 00:00:00', 20);
    """
    // c1: val>8 (only 10,20). c2: val<8 (only 5). c3: val>15 (only 20).
    // c1@R0→c2@R1, R2 has c2 (not c3) → break. c1@R3: no more rows. Best=2.
    order_qt_v2_fixed_edge_relevant_wrong_no_restart """
        SELECT window_funnel(864000, 'fixed', ts, val>8, val<8, val>15) FROM windowfunnel_v2_fixed_edge;
    """
    sql """ DROP TABLE IF EXISTS windowfunnel_v2_fixed_edge """
}
