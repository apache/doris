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

import org.junit.jupiter.api.Assertions;

suite("docs/query-data/asof-join.md") {
    try {
        // Preparation: Create trades and quotes tables
        sql "DROP TABLE IF EXISTS trades"
        sql """
        CREATE TABLE trades (
            trade_id INT,
            symbol VARCHAR(10),
            trade_time DATETIME,
            price DECIMAL(10, 2),
            quantity INT
        ) DISTRIBUTED BY HASH(trade_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql "DROP TABLE IF EXISTS quotes"
        sql """
        CREATE TABLE quotes (
            quote_id INT,
            symbol VARCHAR(10),
            quote_time DATETIME,
            bid_price DECIMAL(10, 2),
            ask_price DECIMAL(10, 2)
        ) DISTRIBUTED BY HASH(quote_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql """
        INSERT INTO trades VALUES
        (1, 'AAPL', '2024-01-01 10:00:05', 150.50, 100),
        (2, 'AAPL', '2024-01-01 10:00:15', 151.00, 200),
        (3, 'AAPL', '2024-01-01 10:00:25', 150.75, 150),
        (4, 'GOOG', '2024-01-01 10:00:10', 2800.00, 50),
        (5, 'GOOG', '2024-01-01 10:00:20', 2805.00, 75),
        (6, 'MSFT', '2024-01-01 10:00:08', 380.00, 120)
        """

        sql """
        INSERT INTO quotes VALUES
        (1, 'AAPL', '2024-01-01 10:00:00', 150.00, 150.10),
        (2, 'AAPL', '2024-01-01 10:00:10', 150.40, 150.60),
        (3, 'AAPL', '2024-01-01 10:00:20', 150.90, 151.10),
        (4, 'GOOG', '2024-01-01 10:00:05', 2795.00, 2800.00),
        (5, 'GOOG', '2024-01-01 10:00:15', 2802.00, 2808.00),
        (6, 'MSFT', '2024-01-01 10:00:00', 378.00, 380.00),
        (7, 'MSFT', '2024-01-01 10:00:10', 379.50, 381.00)
        """

        // Preparation: Create orders and prices tables for examples 4, 6, 8
        sql "DROP TABLE IF EXISTS orders"
        sql """
        CREATE TABLE orders (
            order_id INT,
            product_id INT,
            region VARCHAR(20),
            order_time DATETIME
        ) DISTRIBUTED BY HASH(order_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql "DROP TABLE IF EXISTS prices"
        sql """
        CREATE TABLE prices (
            price_id INT,
            product_id INT,
            region VARCHAR(20),
            effective_time DATETIME,
            price DECIMAL(10, 2)
        ) DISTRIBUTED BY HASH(price_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql """
        INSERT INTO orders VALUES
        (1, 101, 'US', '2024-01-01 10:00:10'),
        (2, 101, 'US', '2024-01-01 10:00:30'),
        (3, 102, 'EU', '2024-01-01 10:00:15'),
        (4, 102, 'EU', '2024-01-01 10:00:45')
        """

        sql """
        INSERT INTO prices VALUES
        (1, 101, 'US', '2024-01-01 10:00:00', 100.00),
        (2, 101, 'US', '2024-01-01 10:00:20', 102.00),
        (3, 102, 'EU', '2024-01-01 10:00:00', 200.00),
        (4, 102, 'EU', '2024-01-01 10:00:30', 205.00)
        """

        // Preparation: Create inventory and products tables for example 6
        sql "DROP TABLE IF EXISTS inventory"
        sql """
        CREATE TABLE inventory (
            inv_id INT,
            product_id INT,
            region VARCHAR(20),
            snapshot_time DATETIME,
            stock_level INT
        ) DISTRIBUTED BY HASH(inv_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql """
        INSERT INTO inventory VALUES
        (1, 101, 'US', '2024-01-01 10:00:00', 500),
        (2, 101, 'US', '2024-01-01 10:00:20', 480),
        (3, 102, 'EU', '2024-01-01 10:00:00', 300),
        (4, 102, 'EU', '2024-01-01 10:00:30', 290)
        """

        sql "DROP TABLE IF EXISTS products"
        sql """
        CREATE TABLE products (
            product_id INT,
            product_name VARCHAR(50)
        ) DISTRIBUTED BY HASH(product_id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql """
        INSERT INTO products VALUES
        (101, 'Widget A'),
        (102, 'Widget B')
        """

        // Preparation: Create left_table and right_table for example 5
        sql "DROP TABLE IF EXISTS left_table"
        sql """
        CREATE TABLE left_table (
            id INT,
            grp INT,
            ts DATETIME,
            val VARCHAR(50)
        ) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql "DROP TABLE IF EXISTS right_table"
        sql """
        CREATE TABLE right_table (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(50)
        ) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
        """

        sql """
        INSERT INTO left_table VALUES
        (1, 1, '2024-01-01 12:00:00', 'event_a'),
        (2, 1, '2024-01-01 14:00:00', 'event_b'),
        (3, 2, '2024-01-01 13:00:00', 'event_c')
        """

        sql """
        INSERT INTO right_table VALUES
        (1, 1, '2024-01-01 10:00:00', 'snapshot_1'),
        (2, 1, '2024-01-01 12:00:00', 'snapshot_2'),
        (3, 1, '2024-01-01 13:00:00', 'snapshot_3'),
        (4, 2, '2024-01-01 11:00:00', 'snapshot_4')
        """

        // Example 1: Find the Most Recent Quote for Each Trade (>=)
        order_qt_example1 """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price, q.ask_price
        FROM trades t
        ASOF LEFT JOIN quotes q
            MATCH_CONDITION(t.trade_time >= q.quote_time)
            ON t.symbol = q.symbol
        ORDER BY t.trade_id
        """

        // Example 2: Find the Next Quote After Each Trade (<=)
        order_qt_example2 """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price
        FROM trades t
        ASOF LEFT JOIN quotes q
            MATCH_CONDITION(t.trade_time <= q.quote_time)
            ON t.symbol = q.symbol
        ORDER BY t.trade_id
        """

        // Example 3: ASOF INNER JOIN — Exclude Unmatched Rows
        order_qt_example3 """
        SELECT t.trade_id, t.symbol, t.trade_time, t.price,
               q.quote_id, q.quote_time, q.bid_price
        FROM trades t
        ASOF INNER JOIN quotes q
            MATCH_CONDITION(t.trade_time >= q.quote_time)
            ON t.symbol = q.symbol
        ORDER BY t.trade_id
        """

        // Example 4: Multiple Equality Conditions
        order_qt_example4 """
        SELECT o.order_id, o.product_id, o.region, o.order_time,
               p.price, p.effective_time
        FROM orders o
        ASOF LEFT JOIN prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ORDER BY o.order_id
        """

        // Example 5: Expression in MATCH_CONDITION (at least 1 hour gap)
        order_qt_example5 """
        SELECT l.id, l.ts, r.id AS rid, r.ts AS rts, r.data
        FROM left_table l
        ASOF LEFT JOIN right_table r
            MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
            ON l.grp = r.grp
        ORDER BY l.id
        """

        // Example 6: Multi-level ASOF JOIN
        order_qt_example6 """
        SELECT o.order_id, o.order_time,
               p.price, p.effective_time AS price_time,
               i.stock_level, i.snapshot_time AS inv_time
        FROM orders o
        ASOF LEFT JOIN prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ASOF LEFT JOIN inventory i
            MATCH_CONDITION(o.order_time >= i.snapshot_time)
            ON o.product_id = i.product_id AND o.region = i.region
        ORDER BY o.order_id
        """

        // Example 6 (continued): Mixing ASOF JOIN with regular JOIN
        order_qt_example6_mixed """
        SELECT o.order_id, prod.product_name,
               o.order_time, p.price
        FROM orders o
        INNER JOIN products prod ON o.product_id = prod.product_id
        ASOF LEFT JOIN prices p
            MATCH_CONDITION(o.order_time >= p.effective_time)
            ON o.product_id = p.product_id AND o.region = p.region
        ORDER BY o.order_id
        """

        // Example 7: ASOF JOIN with Aggregation
        order_qt_example7 """
        SELECT t.symbol,
               COUNT(*) AS trade_count,
               AVG(q.bid_price) AS avg_bid
        FROM trades t
        ASOF LEFT JOIN quotes q
            MATCH_CONDITION(t.trade_time >= q.quote_time)
            ON t.symbol = q.symbol
        GROUP BY t.symbol
        ORDER BY t.symbol
        """

        // Example 8: Bidirectional ASOF JOIN — Finding Surrounding Records
        order_qt_example8 """
        SELECT o.order_id, o.order_time,
               p_before.price AS price_before,
               p_before.effective_time AS time_before,
               p_after.price AS price_after,
               p_after.effective_time AS time_after
        FROM orders o
        ASOF LEFT JOIN prices p_before
            MATCH_CONDITION(o.order_time >= p_before.effective_time)
            ON o.product_id = p_before.product_id AND o.region = p_before.region
        ASOF LEFT JOIN prices p_after
            MATCH_CONDITION(o.order_time <= p_after.effective_time)
            ON o.product_id = p_after.product_id AND o.region = p_after.region
        ORDER BY o.order_id
        """

        // Example 9: Directional Matching, Not Absolute Nearest (>=)
        order_qt_example9_ge """
        WITH left_events AS (
            SELECT 1 AS event_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:06' AS DATETIME) AS event_time
        ),
        right_events AS (
            SELECT 1 AS right_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:00' AS DATETIME) AS ref_time
            UNION ALL
            SELECT 2 AS right_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:08' AS DATETIME) AS ref_time
        )
        SELECT l.event_id, l.event_time, r.right_id, r.ref_time
        FROM left_events l
        ASOF LEFT JOIN right_events r
            MATCH_CONDITION(l.event_time >= r.ref_time)
            ON l.symbol = r.symbol
        """

        // Example 9: Directional Matching, Not Absolute Nearest (<=)
        order_qt_example9_le """
        WITH left_events AS (
            SELECT 1 AS event_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:06' AS DATETIME) AS event_time
        ),
        right_events AS (
            SELECT 1 AS right_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:00' AS DATETIME) AS ref_time
            UNION ALL
            SELECT 2 AS right_id, 'AAPL' AS symbol, CAST('2024-01-01 10:00:08' AS DATETIME) AS ref_time
        )
        SELECT l.event_id, l.event_time, r.right_id, r.ref_time
        FROM left_events l
        ASOF LEFT JOIN right_events r
            MATCH_CONDITION(l.event_time <= r.ref_time)
            ON l.symbol = r.symbol
        """

        // Example 10: Duplicate Match Values (TIMESTAMPTZ) — result is non-deterministic, only verify row count
        // The doc states this may return either right_id=1 or right_id=2
        // Cast TIMESTAMPTZ columns to STRING to avoid JDBC decoding issues
        def result10 = sql """
        WITH left_events AS (
            SELECT 1 AS event_id, 'AAPL' AS symbol,
                   CAST('2024-01-01 10:00:05 +00:00' AS TIMESTAMPTZ) AS event_time
        ),
        right_events AS (
            SELECT 1 AS right_id, 'AAPL' AS symbol,
                   CAST('2024-01-01 10:00:00 +00:00' AS TIMESTAMPTZ) AS ref_time, 'snapshot_a' AS tag
            UNION ALL
            SELECT 2 AS right_id, 'AAPL' AS symbol,
                   CAST('2024-01-01 10:00:00 +00:00' AS TIMESTAMPTZ) AS ref_time, 'snapshot_b' AS tag
        )
        SELECT l.event_id, r.right_id, CAST(r.ref_time AS STRING), r.tag
        FROM left_events l
        ASOF LEFT JOIN right_events r
            MATCH_CONDITION(l.event_time >= r.ref_time)
            ON l.symbol = r.symbol
        """
        // Non-deterministic: one row returned, event_id = 1, right_id is either 1 or 2
        assert result10.size() == 1
        assert result10[0][0] == 1

    } catch (Throwable t) {
        Assertions.fail("examples in docs/query-data/asof-join.md failed to exec, please fix it", t)
    }
}
