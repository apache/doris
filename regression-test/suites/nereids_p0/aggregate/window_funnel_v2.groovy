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
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

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

    sql """ DROP TABLE IF EXISTS windowfunnel_v2_test """
}
