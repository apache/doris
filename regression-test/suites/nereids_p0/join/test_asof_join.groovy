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

// ASOF JOIN Syntax:
// FROM <left_table> ASOF [ INNER | LEFT ] JOIN <right_table>
//   MATCH_CONDITION ( <left_table.timecol> <comparison_operator> <right_table.timecol> )
//   ON <left_table.col> = <right_table.col> [ AND ... ]
// comparison_operator: >=, <=, >, <

suite("test_asof_join", "nereids_p0") {
    // Create test tables for ASOF JOIN
    sql "DROP TABLE IF EXISTS asof_trades"
    sql "DROP TABLE IF EXISTS asof_quotes"
    sql "DROP TABLE IF EXISTS asof_events"
    sql "DROP TABLE IF EXISTS asof_snapshots"

    // Table 1: trades - represents trade events with timestamps
    sql """
        CREATE TABLE asof_trades (
            trade_id INT,
            symbol VARCHAR(10),
            trade_time DATETIME,
            price DECIMAL(10, 2),
            quantity INT
        ) DISTRIBUTED BY HASH(trade_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // Table 2: quotes - represents quote snapshots with timestamps
    sql """
        CREATE TABLE asof_quotes (
            quote_id INT,
            symbol VARCHAR(10),
            quote_time DATETIME,
            bid_price DECIMAL(10, 2),
            ask_price DECIMAL(10, 2)
        ) DISTRIBUTED BY HASH(quote_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // Table 3: events - for testing with DATETIME match column
    sql """
        CREATE TABLE asof_events (
            event_id INT,
            category INT,
            event_time DATETIME,
            event_value VARCHAR(50)
        ) DISTRIBUTED BY HASH(event_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // Table 4: snapshots - for testing with DATETIME match column
    sql """
        CREATE TABLE asof_snapshots (
            snapshot_id INT,
            category INT,
            snapshot_time DATETIME,
            snapshot_data VARCHAR(50)
        ) DISTRIBUTED BY HASH(snapshot_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // Insert test data for trades
    sql """
        INSERT INTO asof_trades VALUES
        (1, 'AAPL', '2024-01-01 10:00:05', 150.50, 100),
        (2, 'AAPL', '2024-01-01 10:00:15', 151.00, 200),
        (3, 'AAPL', '2024-01-01 10:00:25', 150.75, 150),
        (4, 'GOOG', '2024-01-01 10:00:10', 2800.00, 50),
        (5, 'GOOG', '2024-01-01 10:00:20', 2805.00, 75),
        (6, 'MSFT', '2024-01-01 10:00:08', 380.00, 120)
    """

    // Insert test data for quotes
    sql """
        INSERT INTO asof_quotes VALUES
        (1, 'AAPL', '2024-01-01 10:00:00', 150.00, 150.10),
        (2, 'AAPL', '2024-01-01 10:00:10', 150.40, 150.60),
        (3, 'AAPL', '2024-01-01 10:00:20', 150.90, 151.10),
        (4, 'GOOG', '2024-01-01 10:00:05', 2795.00, 2800.00),
        (5, 'GOOG', '2024-01-01 10:00:15', 2802.00, 2808.00),
        (6, 'MSFT', '2024-01-01 10:00:00', 378.00, 380.00),
        (7, 'MSFT', '2024-01-01 10:00:10', 379.50, 381.00)
    """

    // Insert test data for events
    sql """
        INSERT INTO asof_events VALUES
        (1, 1, '2024-01-01 10:00:05', 'event_A'),
        (2, 1, '2024-01-01 10:00:15', 'event_B'),
        (3, 1, '2024-01-01 10:00:25', 'event_C'),
        (4, 2, '2024-01-01 10:00:10', 'event_D'),
        (5, 2, '2024-01-01 10:00:20', 'event_E'),
        (6, 3, '2024-01-01 10:00:08', 'event_F')
    """

    // Insert test data for snapshots
    sql """
        INSERT INTO asof_snapshots VALUES
        (1, 1, '2024-01-01 10:00:00', 'snap_1'),
        (2, 1, '2024-01-01 10:00:10', 'snap_2'),
        (3, 1, '2024-01-01 10:00:20', 'snap_3'),
        (4, 2, '2024-01-01 10:00:05', 'snap_4'),
        (5, 2, '2024-01-01 10:00:15', 'snap_5'),
        (6, 3, '2024-01-01 10:00:10', 'snap_6')
    """

    // ==================== Test 1: Basic ASOF LEFT JOIN with >= ====================
    // For each trade, find the most recent quote (quote_time <= trade_time)
    qt_asof_basic_ge """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price, q.ask_price
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time >= q.quote_time)
        ON t.symbol = q.symbol
        ORDER BY t.trade_id
    """

    // ==================== Test 2: Basic ASOF LEFT JOIN with <= ====================
    // For each trade, find the next quote (quote_time >= trade_time)
    qt_asof_basic_le """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time <= q.quote_time)
        ON t.symbol = q.symbol
        ORDER BY t.trade_id
    """

    // ==================== Test 3: ASOF LEFT JOIN with > (strict greater) ====================
    qt_asof_strict_gt """
        SELECT t.trade_id, t.symbol, t.trade_time,
               q.quote_id, q.quote_time
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time > q.quote_time)
        ON t.symbol = q.symbol
        ORDER BY t.trade_id
    """

    // ==================== Test 4: ASOF LEFT JOIN with < (strict less) ====================
    qt_asof_strict_lt """
        SELECT t.trade_id, t.symbol, t.trade_time,
               q.quote_id, q.quote_time
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time < q.quote_time)
        ON t.symbol = q.symbol
        ORDER BY t.trade_id
    """

    // ==================== Test 5: ASOF INNER JOIN (no NULL for unmatched) ====================
    qt_asof_inner """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price
        FROM asof_trades t
        ASOF INNER JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time >= q.quote_time)
        ON t.symbol = q.symbol
        ORDER BY t.trade_id
    """

    // ==================== Test 6: ASOF JOIN with another DATETIME example ====================
    qt_asof_events """
        SELECT e.event_id, e.category, e.event_time, e.event_value,
               s.snapshot_id, s.snapshot_time, s.snapshot_data
        FROM asof_events e
        ASOF LEFT JOIN asof_snapshots s
        MATCH_CONDITION(e.event_time >= s.snapshot_time)
        ON e.category = s.category
        ORDER BY e.event_id
    """

    // ==================== Test 7: No match case (LEFT JOIN should return NULL) ====================
    sql "DROP TABLE IF EXISTS asof_no_match_left"
    sql "DROP TABLE IF EXISTS asof_no_match_right"

    sql """
        CREATE TABLE asof_no_match_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_no_match_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_no_match_left VALUES
        (1, 1, '2024-01-01 10:00:10'),
        (2, 2, '2024-01-01 10:00:20')
    """

    sql """
        INSERT INTO asof_no_match_right VALUES
        (1, 1, '2024-01-01 10:01:00'),
        (2, 3, '2024-01-01 10:00:30')
    """

    // Group 1: left.ts < right.ts, so no match for >=
    // Group 2: no matching group in right table
    qt_asof_no_match """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts
        FROM asof_no_match_left l
        ASOF LEFT JOIN asof_no_match_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 8: Empty right table ====================
    sql "DROP TABLE IF EXISTS asof_empty_right"
    sql """
        CREATE TABLE asof_empty_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    qt_asof_empty_right """
        SELECT l.id, l.grp, l.ts, r.id as rid
        FROM asof_no_match_left l
        ASOF LEFT JOIN asof_empty_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 9: Multiple equality conditions ====================
    sql "DROP TABLE IF EXISTS asof_multi_eq_left"
    sql "DROP TABLE IF EXISTS asof_multi_eq_right"

    sql """
        CREATE TABLE asof_multi_eq_left (
            id INT,
            grp1 INT,
            grp2 VARCHAR(10),
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_multi_eq_right (
            id INT,
            grp1 INT,
            grp2 VARCHAR(10),
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_multi_eq_left VALUES
        (1, 1, 'A', '2024-01-01 10:00:10'),
        (2, 1, 'A', '2024-01-01 10:00:20'),
        (3, 1, 'B', '2024-01-01 10:00:15'),
        (4, 2, 'A', '2024-01-01 10:00:25')
    """

    sql """
        INSERT INTO asof_multi_eq_right VALUES
        (1, 1, 'A', '2024-01-01 10:00:05', 'data_1'),
        (2, 1, 'A', '2024-01-01 10:00:15', 'data_2'),
        (3, 1, 'B', '2024-01-01 10:00:10', 'data_3'),
        (4, 2, 'A', '2024-01-01 10:00:20', 'data_4'),
        (5, 2, 'A', '2024-01-01 10:00:30', 'data_5')
    """

    qt_asof_multi_eq """
        SELECT l.id, l.grp1, l.grp2, l.ts,
               r.id as rid, r.ts as rts, r.data
        FROM asof_multi_eq_left l
        ASOF LEFT JOIN asof_multi_eq_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp1 = r.grp1 AND l.grp2 = r.grp2
        ORDER BY l.id
    """

    // ==================== Test 10: NULL values in equality column ====================
    sql "DROP TABLE IF EXISTS asof_null_eq_left"
    sql "DROP TABLE IF EXISTS asof_null_eq_right"

    sql """
        CREATE TABLE asof_null_eq_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_null_eq_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_null_eq_left VALUES
        (1, 1, '2024-01-01 10:00:10'),
        (2, NULL, '2024-01-01 10:00:20'),
        (3, 2, '2024-01-01 10:00:30')
    """

    sql """
        INSERT INTO asof_null_eq_right VALUES
        (1, 1, '2024-01-01 10:00:05'),
        (2, NULL, '2024-01-01 10:00:15'),
        (3, 2, '2024-01-01 10:00:25')
    """

    qt_asof_null_eq """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts
        FROM asof_null_eq_left l
        ASOF LEFT JOIN asof_null_eq_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 11: NULL values in match column ====================
    sql "DROP TABLE IF EXISTS asof_null_match_left"
    sql "DROP TABLE IF EXISTS asof_null_match_right"

    sql """
        CREATE TABLE asof_null_match_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_null_match_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_null_match_left VALUES
        (1, 1, '2024-01-01 10:00:10'),
        (2, 1, NULL),
        (3, 1, '2024-01-01 10:00:30')
    """

    sql """
        INSERT INTO asof_null_match_right VALUES
        (1, 1, '2024-01-01 10:00:05'),
        (2, 1, NULL),
        (3, 1, '2024-01-01 10:00:25')
    """

    qt_asof_null_match """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts
        FROM asof_null_match_left l
        ASOF LEFT JOIN asof_null_match_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 12: Single row tables ====================
    sql "DROP TABLE IF EXISTS asof_single_left"
    sql "DROP TABLE IF EXISTS asof_single_right"

    sql """
        CREATE TABLE asof_single_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_single_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql "INSERT INTO asof_single_left VALUES (1, 1, '2024-01-01 10:00:10')"
    sql "INSERT INTO asof_single_right VALUES (1, 1, '2024-01-01 10:00:05')"

    qt_asof_single_match """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_single_left l
        ASOF LEFT JOIN asof_single_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    sql "TRUNCATE TABLE asof_single_right"
    sql "INSERT INTO asof_single_right VALUES (1, 1, '2024-01-01 10:00:15')"

    qt_asof_single_no_match """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_single_left l
        ASOF LEFT JOIN asof_single_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 13: Large number of matches in same bucket ====================
    sql "DROP TABLE IF EXISTS asof_many_matches_left"
    sql "DROP TABLE IF EXISTS asof_many_matches_right"

    sql """
        CREATE TABLE asof_many_matches_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_many_matches_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql "INSERT INTO asof_many_matches_left VALUES (1, 1, '2024-01-01 10:00:50')"

    sql """
        INSERT INTO asof_many_matches_right VALUES
        (1, 1, '2024-01-01 10:00:10'), (2, 1, '2024-01-01 10:00:20'), 
        (3, 1, '2024-01-01 10:00:30'), (4, 1, '2024-01-01 10:00:40'), 
        (5, 1, '2024-01-01 10:00:45'), (6, 1, '2024-01-01 10:00:48'), 
        (7, 1, '2024-01-01 10:00:49'), (8, 1, '2024-01-01 10:00:50'), 
        (9, 1, '2024-01-01 10:00:51'), (10, 1, '2024-01-01 10:01:00')
    """

    // Should find the closest match: ts=10:00:50 (exact match) for >= case
    qt_asof_many_ge """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_many_matches_left l
        ASOF LEFT JOIN asof_many_matches_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Should find the closest match: ts=10:00:49 for > case
    qt_asof_many_gt """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_many_matches_left l
        ASOF LEFT JOIN asof_many_matches_right r
        MATCH_CONDITION(l.ts > r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Should find the closest match: ts=10:00:50 for <= case
    qt_asof_many_le """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_many_matches_left l
        ASOF LEFT JOIN asof_many_matches_right r
        MATCH_CONDITION(l.ts <= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Should find the closest match: ts=10:00:51 for < case
    qt_asof_many_lt """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_many_matches_left l
        ASOF LEFT JOIN asof_many_matches_right r
        MATCH_CONDITION(l.ts < r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 14: ASOF JOIN in subquery ====================
    qt_asof_subquery """
        SELECT * FROM (
            SELECT t.trade_id, t.symbol, q.quote_id, q.bid_price
            FROM asof_trades t
            ASOF LEFT JOIN asof_quotes q
            MATCH_CONDITION(t.trade_time >= q.quote_time)
            ON t.symbol = q.symbol
        ) sub
        WHERE sub.bid_price > 150
        ORDER BY sub.trade_id
    """

    // ==================== Test 15: ASOF JOIN with aggregation ====================
    qt_asof_agg """
        SELECT t.symbol, COUNT(*) as trade_count, AVG(q.bid_price) as avg_bid
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time >= q.quote_time)
        ON t.symbol = q.symbol
        GROUP BY t.symbol
        ORDER BY t.symbol
    """

    // ==================== Test 16: ASOF JOIN with filter on both sides ====================
    qt_asof_filter """
        SELECT t.trade_id, t.symbol, t.trade_time,
               q.quote_id, q.quote_time
        FROM asof_trades t
        ASOF LEFT JOIN asof_quotes q
        MATCH_CONDITION(t.trade_time >= q.quote_time)
        ON t.symbol = q.symbol
        WHERE t.price > 150 AND (q.bid_price IS NULL OR q.bid_price > 100)
        ORDER BY t.trade_id
    """

    // ==================== Test 17: ASOF INNER JOIN filters out non-matches ====================
    qt_asof_inner_filter """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts
        FROM asof_no_match_left l
        ASOF INNER JOIN asof_no_match_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Complex Test Cases ====================

    // Create tables for complex tests
    sql """ DROP TABLE IF EXISTS asof_orders """
    sql """ DROP TABLE IF EXISTS asof_prices """
    sql """ DROP TABLE IF EXISTS asof_inventory """
    sql """ DROP TABLE IF EXISTS asof_promotions """

    sql """
        CREATE TABLE asof_orders (
            order_id INT,
            product_id INT,
            region VARCHAR(10),
            order_time DATETIME,
            quantity INT
        ) DISTRIBUTED BY HASH(order_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_prices (
            price_id INT,
            product_id INT,
            region VARCHAR(10),
            effective_time DATETIME,
            price DECIMAL(10,2)
        ) DISTRIBUTED BY HASH(price_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_inventory (
            inv_id INT,
            product_id INT,
            region VARCHAR(10),
            snapshot_time DATETIME,
            stock_level INT
        ) DISTRIBUTED BY HASH(inv_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_promotions (
            promo_id INT,
            product_id INT,
            start_time DATETIME,
            discount_pct INT
        ) DISTRIBUTED BY HASH(promo_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // Insert test data
    sql """
        INSERT INTO asof_orders VALUES
        (1, 100, 'US', '2024-01-01 10:00:00', 5),
        (2, 100, 'US', '2024-01-01 11:00:00', 3),
        (3, 100, 'EU', '2024-01-01 10:30:00', 2),
        (4, 200, 'US', '2024-01-01 10:15:00', 10),
        (5, 200, 'US', '2024-01-01 12:00:00', 7),
        (6, 100, 'US', '2024-01-01 09:00:00', 1)
    """

    sql """
        INSERT INTO asof_prices VALUES
        (1, 100, 'US', '2024-01-01 08:00:00', 10.00),
        (2, 100, 'US', '2024-01-01 09:30:00', 12.00),
        (3, 100, 'US', '2024-01-01 10:30:00', 11.50),
        (4, 100, 'EU', '2024-01-01 08:00:00', 15.00),
        (5, 100, 'EU', '2024-01-01 10:00:00', 14.00),
        (6, 200, 'US', '2024-01-01 08:00:00', 25.00),
        (7, 200, 'US', '2024-01-01 11:00:00', 27.00)
    """

    sql """
        INSERT INTO asof_inventory VALUES
        (1, 100, 'US', '2024-01-01 07:00:00', 100),
        (2, 100, 'US', '2024-01-01 09:00:00', 80),
        (3, 100, 'US', '2024-01-01 10:30:00', 50),
        (4, 100, 'EU', '2024-01-01 08:00:00', 200),
        (5, 200, 'US', '2024-01-01 08:00:00', 500),
        (6, 200, 'US', '2024-01-01 11:30:00', 400)
    """

    sql """
        INSERT INTO asof_promotions VALUES
        (1, 100, '2024-01-01 09:00:00', 10),
        (2, 100, '2024-01-01 10:30:00', 15),
        (3, 200, '2024-01-01 10:00:00', 5)
    """

    // ==================== Test 18: Multi-level ASOF JOIN (3 tables) ====================
    // Order -> Price (ASOF) -> Inventory (ASOF)
    qt_asof_multi_level_3 """
        SELECT o.order_id, o.product_id, o.order_time, o.quantity,
               p.price, p.effective_time as price_time,
               i.stock_level, i.snapshot_time as inv_time
        FROM asof_orders o
        ASOF LEFT JOIN asof_prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ASOF LEFT JOIN asof_inventory i
            MATCH_CONDITION(o.order_time >= i.snapshot_time)
            ON o.product_id = i.product_id AND o.region = i.region
        ORDER BY o.order_id
    """

    // ==================== Test 19: Multi-level ASOF JOIN (4 tables) ====================
    // Order -> Price -> Inventory -> Promotions
    qt_asof_multi_level_4 """
        SELECT o.order_id, o.product_id, o.order_time,
               p.price,
               i.stock_level,
               pr.discount_pct
        FROM asof_orders o
        ASOF LEFT JOIN asof_prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ASOF LEFT JOIN asof_inventory i
            MATCH_CONDITION(o.order_time >= i.snapshot_time)
            ON o.product_id = i.product_id AND o.region = i.region
        ASOF LEFT JOIN asof_promotions pr
            MATCH_CONDITION(o.order_time >= pr.start_time)
            ON o.product_id = pr.product_id
        ORDER BY o.order_id
    """

    // ==================== Test 20: ASOF JOIN mixed with regular JOIN ====================
    sql """ DROP TABLE IF EXISTS asof_products """
    sql """
        CREATE TABLE asof_products (
            product_id INT,
            product_name VARCHAR(50),
            category VARCHAR(20)
        ) DISTRIBUTED BY HASH(product_id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO asof_products VALUES
        (100, 'Widget A', 'Electronics'),
        (200, 'Widget B', 'Electronics'),
        (300, 'Gadget C', 'Accessories')
    """

    qt_asof_with_regular_join """
        SELECT o.order_id, prod.product_name, prod.category,
               o.order_time, p.price, p.effective_time
        FROM asof_orders o
        INNER JOIN asof_products prod ON o.product_id = prod.product_id
        ASOF LEFT JOIN asof_prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ORDER BY o.order_id
    """

    // ==================== Test 21: Expression-based equality key ====================
    sql """ DROP TABLE IF EXISTS asof_expr_left """
    sql """ DROP TABLE IF EXISTS asof_expr_right """
    
    sql """
        CREATE TABLE asof_expr_left (
            id INT,
            val1 INT,
            val2 INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    
    sql """
        CREATE TABLE asof_expr_right (
            id INT,
            combined_val INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_expr_left VALUES
        (1, 10, 5, '2024-01-01 10:00:00'),
        (2, 20, 10, '2024-01-01 11:00:00'),
        (3, 15, 5, '2024-01-01 12:00:00')
    """

    sql """
        INSERT INTO asof_expr_right VALUES
        (1, 15, '2024-01-01 09:00:00', 'data_a'),
        (2, 15, '2024-01-01 09:30:00', 'data_b'),
        (3, 15, '2024-01-01 11:30:00', 'data_c'),
        (4, 30, '2024-01-01 10:00:00', 'data_d'),
        (5, 30, '2024-01-01 10:30:00', 'data_e'),
        (6, 20, '2024-01-01 11:00:00', 'data_f')
    """

    // Join on expression: val1 + val2 = combined_val
    qt_asof_expr_key """
        SELECT l.id, l.val1, l.val2, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.val1 + l.val2 = r.combined_val
        ORDER BY l.id
    """

    // ==================== Test 22: String function in equality key ====================
    sql """ DROP TABLE IF EXISTS asof_str_left """
    sql """ DROP TABLE IF EXISTS asof_str_right """

    sql """
        CREATE TABLE asof_str_left (
            id INT,
            full_code VARCHAR(20),
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_str_right (
            id INT,
            prefix VARCHAR(10),
            ts DATETIME,
            value INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_str_left VALUES
        (1, 'ABC-001', '2024-01-01 10:00:00'),
        (2, 'ABC-002', '2024-01-01 11:00:00'),
        (3, 'XYZ-001', '2024-01-01 12:00:00')
    """

    sql """
        INSERT INTO asof_str_right VALUES
        (1, 'ABC', '2024-01-01 09:00:00', 100),
        (2, 'ABC', '2024-01-01 09:30:00', 110),
        (3, 'ABC', '2024-01-01 10:30:00', 120),
        (4, 'XYZ', '2024-01-01 11:00:00', 200),
        (5, 'XYZ', '2024-01-01 11:30:00', 210)
    """

    // Join on SUBSTRING expression
    qt_asof_str_expr """
        SELECT l.id, l.full_code, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_str_left l
        ASOF LEFT JOIN asof_str_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON SUBSTRING(l.full_code, 1, 3) = r.prefix
        ORDER BY l.id
    """

    // ==================== Test 23: ASOF JOIN with self-join ====================
    // Note: self-join with inequality in ON clause not supported, use WHERE instead
    qt_asof_self_join """
        SELECT t1.order_id as order1, t1.order_time as time1,
               t2.order_id as order2, t2.order_time as time2
        FROM asof_orders t1
        ASOF LEFT JOIN asof_orders t2
        MATCH_CONDITION(t1.order_time > t2.order_time)
        ON t1.product_id = t2.product_id AND t1.region = t2.region
        ORDER BY t1.order_id
    """

    // ==================== Test 24: ASOF JOIN with date arithmetic in MATCH_CONDITION ====================
    // Note: MATCH_CONDITION requires direct column comparison, this tests boundary behavior
    sql """ DROP TABLE IF EXISTS asof_date_arith_left """
    sql """ DROP TABLE IF EXISTS asof_date_arith_right """

    sql """
        CREATE TABLE asof_date_arith_left (
            id INT,
            grp INT,
            event_date DATE
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_date_arith_right (
            id INT,
            grp INT,
            ref_date DATE,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_date_arith_left VALUES
        (1, 1, '2024-01-10'),
        (2, 1, '2024-01-15'),
        (3, 2, '2024-01-20')
    """

    sql """
        INSERT INTO asof_date_arith_right VALUES
        (1, 1, '2024-01-05', 'early'),
        (2, 1, '2024-01-08', 'mid'),
        (3, 1, '2024-01-12', 'late'),
        (4, 2, '2024-01-18', 'data_x')
    """

    qt_asof_date """
        SELECT l.id, l.event_date, r.id as rid, r.ref_date, r.data
        FROM asof_date_arith_left l
        ASOF LEFT JOIN asof_date_arith_right r
        MATCH_CONDITION(l.event_date >= r.ref_date)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 25: Complex aggregation after multi-level ASOF JOIN ====================
    qt_asof_complex_agg """
        SELECT o.product_id, o.region,
               COUNT(*) as order_count,
               SUM(o.quantity) as total_qty,
               AVG(p.price) as avg_price,
               MIN(i.stock_level) as min_stock
        FROM asof_orders o
        ASOF LEFT JOIN asof_prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ASOF LEFT JOIN asof_inventory i
            MATCH_CONDITION(o.order_time >= i.snapshot_time)
            ON o.product_id = i.product_id AND o.region = i.region
        GROUP BY o.product_id, o.region
        ORDER BY o.product_id, o.region
    """

    // ==================== Test 26: ASOF JOIN with CASE expression in SELECT ====================
    qt_asof_case_expr """
        SELECT o.order_id,
               CASE 
                   WHEN p.price IS NULL THEN 'NO_PRICE'
                   WHEN p.price < 15 THEN 'LOW'
                   WHEN p.price < 25 THEN 'MEDIUM'
                   ELSE 'HIGH'
               END as price_tier,
               o.quantity * COALESCE(p.price, 0) as total_value
        FROM asof_orders o
        ASOF LEFT JOIN asof_prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ORDER BY o.order_id
    """

    // ==================== Test 27: Multiple ASOF JOINs with different directions ====================
    qt_asof_mixed_directions """
        SELECT o.order_id, o.order_time,
               p_before.price as price_before,
               p_before.effective_time as time_before,
               p_after.price as price_after,
               p_after.effective_time as time_after
        FROM asof_orders o
        ASOF LEFT JOIN asof_prices p_before
            MATCH_CONDITION(o.order_time >= p_before.effective_time)
            ON o.product_id = p_before.product_id AND o.region = p_before.region
        ASOF LEFT JOIN asof_prices p_after
            MATCH_CONDITION(o.order_time <= p_after.effective_time)
            ON o.product_id = p_after.product_id AND o.region = p_after.region
        WHERE o.product_id = 100 AND o.region = 'US'
        ORDER BY o.order_id
    """

    // ==================== Corner Case Tests ====================

    // ==================== Test 28: Duplicate timestamps on build side ====================
    // When multiple build rows have the exact same timestamp, behavior should be deterministic
    sql """ DROP TABLE IF EXISTS asof_dup_ts_left """
    sql """ DROP TABLE IF EXISTS asof_dup_ts_right """

    sql """
        CREATE TABLE asof_dup_ts_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_dup_ts_right (
            id INT,
            grp INT,
            ts DATETIME,
            value INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_dup_ts_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 10:00:30')
    """

    // Multiple rows with SAME timestamp
    sql """
        INSERT INTO asof_dup_ts_right VALUES
        (1, 1, '2024-01-01 09:30:00', 100),
        (2, 1, '2024-01-01 09:30:00', 200),
        (3, 1, '2024-01-01 09:30:00', 300),
        (4, 1, '2024-01-01 10:00:00', 400),
        (5, 1, '2024-01-01 10:00:00', 500)
    """

    // For >= with duplicate timestamps, should return one of the matching rows
    qt_asof_dup_ts_ge """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_dup_ts_left l
        ASOF LEFT JOIN asof_dup_ts_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 29: Exact boundary with strict inequality ====================
    sql """ DROP TABLE IF EXISTS asof_boundary_left """
    sql """ DROP TABLE IF EXISTS asof_boundary_right """

    sql """
        CREATE TABLE asof_boundary_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_boundary_right (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_boundary_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 10:00:01'),
        (3, 1, '2024-01-01 09:59:59')
    """

    sql """
        INSERT INTO asof_boundary_right VALUES
        (1, 1, '2024-01-01 10:00:00', 'exact'),
        (2, 1, '2024-01-01 09:00:00', 'before'),
        (3, 1, '2024-01-01 11:00:00', 'after')
    """

    // Test > (strict greater): probe.ts=10:00:00 should NOT match build.ts=10:00:00
    qt_asof_boundary_gt """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_boundary_left l
        ASOF LEFT JOIN asof_boundary_right r
        MATCH_CONDITION(l.ts > r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Test < (strict less): probe.ts=10:00:00 should NOT match build.ts=10:00:00
    qt_asof_boundary_lt """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_boundary_left l
        ASOF LEFT JOIN asof_boundary_right r
        MATCH_CONDITION(l.ts < r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Test >= : probe.ts=10:00:00 SHOULD match build.ts=10:00:00
    qt_asof_boundary_ge """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_boundary_left l
        ASOF LEFT JOIN asof_boundary_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Test <= : probe.ts=10:00:00 SHOULD match build.ts=10:00:00
    qt_asof_boundary_le """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_boundary_left l
        ASOF LEFT JOIN asof_boundary_right r
        MATCH_CONDITION(l.ts <= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 30: All build rows filtered out ====================
    sql """ DROP TABLE IF EXISTS asof_all_filtered_left """
    sql """ DROP TABLE IF EXISTS asof_all_filtered_right """

    sql """
        CREATE TABLE asof_all_filtered_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_all_filtered_right (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_all_filtered_left VALUES
        (1, 1, '2024-01-01 08:00:00')
    """

    sql """
        INSERT INTO asof_all_filtered_right VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 11:00:00'),
        (3, 1, '2024-01-01 12:00:00')
    """

    // All build rows have ts > probe.ts, so >= should find nothing
    qt_asof_all_filtered """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_all_filtered_left l
        ASOF LEFT JOIN asof_all_filtered_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 31: Multiple groups with different match patterns ====================
    sql """ DROP TABLE IF EXISTS asof_multi_grp_left """
    sql """ DROP TABLE IF EXISTS asof_multi_grp_right """

    sql """
        CREATE TABLE asof_multi_grp_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_multi_grp_right (
            id INT,
            grp INT,
            ts DATETIME,
            value INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_multi_grp_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 2, '2024-01-01 10:00:00'),
        (3, 3, '2024-01-01 10:00:00'),
        (4, 4, '2024-01-01 10:00:00')
    """

    sql """
        INSERT INTO asof_multi_grp_right VALUES
        (1, 1, '2024-01-01 09:00:00', 100),
        (2, 1, '2024-01-01 09:30:00', 110),
        (3, 1, '2024-01-01 09:45:00', 120),
        (4, 2, '2024-01-01 11:00:00', 200),
        (5, 3, '2024-01-01 10:00:00', 300)
    """
    // grp=1: has matches before 10:00:00
    // grp=2: only has match after 10:00:00 (no match for >=)
    // grp=3: exact match at 10:00:00
    // grp=4: no build rows at all

    qt_asof_multi_grp """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_multi_grp_left l
        ASOF LEFT JOIN asof_multi_grp_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 32: Microsecond precision ====================
    sql """ DROP TABLE IF EXISTS asof_precision_left """
    sql """ DROP TABLE IF EXISTS asof_precision_right """

    sql """
        CREATE TABLE asof_precision_left (
            id INT,
            grp INT,
            ts DATETIME(6)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_precision_right (
            id INT,
            grp INT,
            ts DATETIME(6),
            value INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_precision_left VALUES
        (1, 1, '2024-01-01 10:00:00.000500'),
        (2, 1, '2024-01-01 10:00:00.001000')
    """

    sql """
        INSERT INTO asof_precision_right VALUES
        (1, 1, '2024-01-01 10:00:00.000000', 100),
        (2, 1, '2024-01-01 10:00:00.000400', 200),
        (3, 1, '2024-01-01 10:00:00.000600', 300),
        (4, 1, '2024-01-01 10:00:00.001000', 400)
    """

    qt_asof_precision """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= l.ts)
        ON l.grp = r.grp
        ORDER BY l.id
        """
        exception "MATCH_CONDITION must specify column from both table"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts - r.ts >= 1)
        ON l.grp = r.grp
        ORDER BY l.id
        """
        exception "only allow date, datetime and timestamptz in MATCH_CONDITION"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp or l.id > r.id
        ORDER BY l.id
        """
        exception "ASOF JOIN's ON clause must be one or more EQUAL(=) conjuncts"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
        """
        exception "only ASOF JOIN support MATCH_CONDITION"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ORDER BY l.id
        """
        exception "ASOF JOIN must have on or using clause"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts == r.ts)
        ON l.grp = r.grp 
        ORDER BY l.id
        """
        exception "ASOF JOIN's MATCH_CONDITION must be <, <=, >, >="
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        ON l.grp = r.grp 
        ORDER BY l.id
        """
        exception "ASOF JOIN must specify MATCH_CONDITION"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = 1
        ORDER BY l.id
        """
        exception "ASOF join's hash conjuncts must be in form of"
    }

    test {
        sql """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.value
        FROM asof_precision_left l
        ASOF LEFT JOIN asof_precision_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp and l.grp = 1
        ORDER BY l.id
        """
        exception "ASOF join's hash conjuncts must be in form of"
    }

    // ==================== Test 33-40: Comprehensive ASOF JOIN Coverage ====================
    // Cover: LEFT/RIGHT x INNER/OUTER x operators(>=,<=,>,<) x SlotRef/Expression
    // Use explain + contains to verify plan type before execution
    
    // ---------- Setup: Tables to force RIGHT JOIN plan (small left, large right) ----------
    sql """ DROP TABLE IF EXISTS asof_small """
    sql """ DROP TABLE IF EXISTS asof_large """

    // Small table (3 rows) - will become build side, triggering RIGHT JOIN
    sql """
        CREATE TABLE asof_small (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // Large table (20 rows) - will become probe side
    sql """
        CREATE TABLE asof_large (
            id INT,
            grp INT,
            ts DATETIME,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_small VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 12:00:00'),
        (3, 2, '2024-01-01 11:00:00')
    """

    // Insert 20 rows to ensure optimizer prefers RIGHT plan
    sql """
        INSERT INTO asof_large VALUES
        (1, 1, '2024-01-01 08:00:00', 100),
        (2, 1, '2024-01-01 09:00:00', 110),
        (3, 1, '2024-01-01 10:00:00', 120),
        (4, 1, '2024-01-01 11:00:00', 130),
        (5, 1, '2024-01-01 12:00:00', 140),
        (6, 1, '2024-01-01 13:00:00', 150),
        (7, 2, '2024-01-01 09:00:00', 200),
        (8, 2, '2024-01-01 10:00:00', 210),
        (9, 2, '2024-01-01 11:00:00', 220),
        (10, 2, '2024-01-01 12:00:00', 230),
        (11, 1, '2024-01-01 09:30:00', 115),
        (12, 1, '2024-01-01 10:30:00', 125),
        (13, 1, '2024-01-01 11:30:00', 135),
        (14, 2, '2024-01-01 09:30:00', 205),
        (15, 2, '2024-01-01 10:30:00', 215),
        (16, 2, '2024-01-01 11:30:00', 225),
        (17, 1, '2024-01-01 14:00:00', 160),
        (18, 2, '2024-01-01 13:00:00', 240),
        (19, 3, '2024-01-01 10:00:00', 300),
        (20, 3, '2024-01-01 11:00:00', 310)
    """

    sql """ ANALYZE TABLE asof_small WITH SYNC """
    sql """ ANALYZE TABLE asof_large WITH SYNC """

    // ---------- Test 33: Verify ASOF_RIGHT_INNER_JOIN plan and results ----------
    // Verify plan contains ASOF_RIGHT_INNER_JOIN
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF INNER JOIN asof_large l
            MATCH_CONDITION(s.ts >= l.ts)
            ON s.grp = l.grp
            ORDER BY s.id
        """
        contains "ASOF_RIGHT_INNER_JOIN"
    }

    qt_asof_right_inner_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF INNER JOIN asof_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ---------- Test 34: Verify ASOF_RIGHT_OUTER_JOIN plan and results ----------
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF JOIN asof_large l
            MATCH_CONDITION(s.ts >= l.ts)
            ON s.grp = l.grp
            ORDER BY s.id, l.id
        """
        contains "ASOF_RIGHT_OUTER_JOIN"
    }

    qt_asof_right_outer_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF JOIN asof_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id, l.id
    """

    // ---------- Test 35: ASOF RIGHT JOIN with <= operator ----------
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF INNER JOIN asof_large l
            MATCH_CONDITION(s.ts <= l.ts)
            ON s.grp = l.grp
            ORDER BY s.id
        """
        contains "ASOF_RIGHT_INNER_JOIN"
    }

    qt_asof_right_inner_le """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF INNER JOIN asof_large l
        MATCH_CONDITION(s.ts <= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ---------- Test 36: ASOF RIGHT JOIN with strict > operator ----------
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF INNER JOIN asof_large l
            MATCH_CONDITION(s.ts > l.ts)
            ON s.grp = l.grp
            ORDER BY s.id
        """
        contains "ASOF_RIGHT_INNER_JOIN"
    }

    qt_asof_right_inner_gt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF INNER JOIN asof_large l
        MATCH_CONDITION(s.ts > l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ---------- Test 37: ASOF RIGHT JOIN with strict < operator ----------
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF INNER JOIN asof_large l
            MATCH_CONDITION(s.ts < l.ts)
            ON s.grp = l.grp
            ORDER BY s.id
        """
        contains "ASOF_RIGHT_INNER_JOIN"
    }

    qt_asof_right_inner_lt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF INNER JOIN asof_large l
        MATCH_CONDITION(s.ts < l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ---------- Test 38: ASOF LEFT JOIN (verify plan) ----------
    // Swap table order: large table on left forces LEFT plan
    explain {
        sql """
            SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
            FROM asof_large l
            ASOF INNER JOIN asof_small s
            MATCH_CONDITION(l.ts >= s.ts)
            ON l.grp = s.grp
            ORDER BY l.id
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    qt_asof_left_inner_ge """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts >= s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // ---------- Test 39: ASOF LEFT OUTER JOIN with unmatched rows ----------
    explain {
        sql """
            SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
            FROM asof_large l
            ASOF JOIN asof_small s
            MATCH_CONDITION(l.ts >= s.ts)
            ON l.grp = s.grp
            ORDER BY l.id
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    qt_asof_left_outer_ge """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF JOIN asof_small s
        MATCH_CONDITION(l.ts >= s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // ---------- Test 40: Expression-based MATCH_CONDITION (forces linear scan) ----------
    // Use expression instead of simple SlotRef to verify expression path
    qt_asof_expr_condition """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts >= DATE_SUB(s.ts, INTERVAL 0 SECOND))
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // ---------- Test 41: ASOF RIGHT OUTER JOIN with unmatched build rows ----------
    // Add data to asof_small with a group that has no match in asof_large
    sql """ INSERT INTO asof_small VALUES (4, 4, '2024-01-01 10:00:00') """
    sql """ ANALYZE TABLE asof_small WITH SYNC """

    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_small s
            ASOF JOIN asof_large l
            MATCH_CONDITION(s.ts >= l.ts)
            ON s.grp = l.grp
            ORDER BY s.id
        """
        contains "ASOF_RIGHT_OUTER_JOIN"
    }

    qt_asof_right_outer_unmatched """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_small s
        ASOF JOIN asof_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ---------- Test 42: Large group to trigger binary search (LEFT JOIN) ----------
    // Binary search is triggered when group_size > 8 and uses simple SlotRef
    // Use asof_large (20 rows) as left table and asof_small (4 rows) as right table
    // This forces LEFT plan and tests binary search optimization
    
    explain {
        sql """
            SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
            FROM asof_large l
            ASOF INNER JOIN asof_small s
            MATCH_CONDITION(l.ts >= s.ts)
            ON l.grp = s.grp
            ORDER BY l.id
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    // This triggers binary search path when group has > 8 candidates
    qt_asof_bsearch_ge """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts >= s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // Test binary search with <= operator
    qt_asof_bsearch_le """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts <= s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // Test binary search with strict > operator
    qt_asof_bsearch_gt """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts > s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // Test binary search with strict < operator
    qt_asof_bsearch_lt """
        SELECT l.id, l.grp, l.ts as l_ts, s.ts as s_ts, s.id as sid
        FROM asof_large l
        ASOF INNER JOIN asof_small s
        MATCH_CONDITION(l.ts < s.ts)
        ON l.grp = s.grp
        ORDER BY l.id
    """

    // ---------- Test 43: Multiple groups with different match scenarios ----------
    sql """ DROP TABLE IF EXISTS asof_multi_grp_left """
    sql """ DROP TABLE IF EXISTS asof_multi_grp_right """

    sql """
        CREATE TABLE asof_multi_grp_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_multi_grp_right (
            id INT,
            grp INT,
            ts DATETIME,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // Insert 10 rows in left table to force LEFT plan (larger than right table's 6 rows)
    sql """
        INSERT INTO asof_multi_grp_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 2, '2024-01-01 10:00:00'),
        (3, 3, '2024-01-01 10:00:00'),
        (4, 4, '2024-01-01 10:00:00'),
        (5, 1, '2024-01-01 11:00:00'),
        (6, 2, '2024-01-01 09:00:00'),
        (7, 3, '2024-01-01 11:00:00'),
        (8, 1, '2024-01-01 09:00:00'),
        (9, 2, '2024-01-01 11:00:00'),
        (10, 3, '2024-01-01 08:00:00')
    """

    sql """
        INSERT INTO asof_multi_grp_right VALUES
        (1, 1, '2024-01-01 09:00:00', 100),
        (2, 1, '2024-01-01 10:00:00', 110),
        (3, 1, '2024-01-01 11:00:00', 120),
        (4, 2, '2024-01-01 11:00:00', 200),
        (5, 3, '2024-01-01 09:00:00', 300),
        (6, 3, '2024-01-01 10:00:00', 310)
    """

    sql """ ANALYZE TABLE asof_multi_grp_left WITH SYNC """
    sql """ ANALYZE TABLE asof_multi_grp_right WITH SYNC """

    // Test multiple groups with LEFT OUTER JOIN
    // Verify plan type first
    explain {
        sql """
            SELECT l.id, l.grp, l.ts as l_ts, r.ts as r_ts, r.val
            FROM asof_multi_grp_left l
            ASOF LEFT JOIN asof_multi_grp_right r
            MATCH_CONDITION(l.ts >= r.ts)
            ON l.grp = r.grp
            ORDER BY l.id
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    // Expected results:
    // grp=1: id=1 ts=10:00 -> r.ts<=10:00 -> 10:00(val=110); id=5 ts=11:00 -> 11:00(val=120); id=8 ts=09:00 -> 09:00(val=100)
    // grp=2: id=2 ts=10:00 -> r.ts<=10:00? r only has 11:00 -> NULL; id=6 ts=09:00 -> NULL; id=9 ts=11:00 -> 11:00(val=200)
    // grp=3: id=3 ts=10:00 -> 10:00(val=310); id=7 ts=11:00 -> 10:00(val=310); id=10 ts=08:00 -> NULL
    // grp=4: id=4 -> no r row -> NULL
    qt_asof_multi_grp_outer """
        SELECT l.id, l.grp, l.ts as l_ts, r.ts as r_ts, r.val
        FROM asof_multi_grp_left l
        ASOF LEFT JOIN asof_multi_grp_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 44: TIMESTAMPTZ type support ====================
    sql """ DROP TABLE IF EXISTS asof_timestamptz_left """
    sql """ DROP TABLE IF EXISTS asof_timestamptz_right """

    sql """
        CREATE TABLE asof_timestamptz_left (
            id INT,
            grp INT,
            ts TIMESTAMPTZ
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_timestamptz_right (
            id INT,
            grp INT,
            ts TIMESTAMPTZ,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_timestamptz_left VALUES
        (1, 1, '2024-01-01 10:00:00+08:00'),
        (2, 1, '2024-01-01 12:00:00+08:00')
    """

    sql """
        INSERT INTO asof_timestamptz_right VALUES
        (1, 1, '2024-01-01 09:00:00+08:00', 100),
        (2, 1, '2024-01-01 10:00:00+08:00', 200),
        (3, 1, '2024-01-01 11:00:00+08:00', 300)
    """

    qt_asof_timestamptz """
        SELECT l.id, l.ts as l_ts, r.id as rid, r.ts as r_ts, r.val
        FROM asof_timestamptz_left l
        ASOF LEFT JOIN asof_timestamptz_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """
}
