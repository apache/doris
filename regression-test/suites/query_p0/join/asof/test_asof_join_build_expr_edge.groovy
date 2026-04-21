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

// Edge-case tests for ASOF JOIN build-side expression computation.
// Focus: build_asof_index() correctly evaluates expressions when the build side
// comes from complex plan structures (CTE, subquery, multi-join, chaining, aggregation).
// The build-side expression is prepared against the intermediate row descriptor
// and executed on a dummy-probe + build block; these tests verify that slot ID
// mapping works correctly across various plan topologies.

suite("test_asof_join_build_expr_edge", "query_p0") {

    // ====================== Base table setup ======================
    sql "DROP TABLE IF EXISTS abee_probe"
    sql "DROP TABLE IF EXISTS abee_build"
    sql "DROP TABLE IF EXISTS abee_dim"
    sql "DROP TABLE IF EXISTS abee_build2"
    sql "DROP TABLE IF EXISTS abee_events"

    sql """
        CREATE TABLE abee_probe (
            id INT,
            grp INT,
            grp2 INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE abee_build (
            id INT,
            grp INT,
            grp2 INT,
            ts DATETIME,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE abee_dim (
            id INT,
            grp INT,
            name VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE abee_build2 (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE abee_events (
            id INT,
            grp INT,
            category VARCHAR(10),
            ts DATETIME,
            amount INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // -- Probe data --
    sql """
        INSERT INTO abee_probe VALUES
        (1, 1, 10, '2024-01-01 10:00:00'),
        (2, 1, 10, '2024-01-01 12:00:00'),
        (3, 2, 20, '2024-01-01 11:00:00'),
        (4, 2, 20, '2024-01-01 14:00:00'),
        (5, 3, 30, '2024-01-01 10:00:00')
    """

    // -- Build data (4 rows per grp=1, 4 per grp=2, 2 per grp=3) --
    sql """
        INSERT INTO abee_build VALUES
        (1,  1, 10, '2024-01-01 08:00:00', 100),
        (2,  1, 10, '2024-01-01 09:00:00', 110),
        (3,  1, 10, '2024-01-01 10:00:00', 120),
        (4,  1, 10, '2024-01-01 11:00:00', 130),
        (5,  2, 20, '2024-01-01 09:00:00', 200),
        (6,  2, 20, '2024-01-01 10:00:00', 210),
        (7,  2, 20, '2024-01-01 12:00:00', 220),
        (8,  2, 20, '2024-01-01 13:00:00', 230),
        (9,  3, 30, '2024-01-01 09:00:00', 300),
        (10, 3, 30, '2024-01-01 09:30:00', 310)
    """

    // -- Dimension data --
    sql """
        INSERT INTO abee_dim VALUES
        (1, 1, 'alpha'),
        (2, 2, 'beta'),
        (3, 3, 'gamma')
    """

    // -- Second build table for chained-join test --
    sql """
        INSERT INTO abee_build2 VALUES
        (1, 1, '2024-01-01 07:00:00', 'b2_1'),
        (2, 1, '2024-01-01 09:00:00', 'b2_2'),
        (3, 2, '2024-01-01 08:00:00', 'b2_3'),
        (4, 2, '2024-01-01 11:00:00', 'b2_4'),
        (5, 3, '2024-01-01 08:30:00', 'b2_5')
    """

    // -- Events data for aggregation test --
    sql """
        INSERT INTO abee_events VALUES
        (1, 1, 'A', '2024-01-01 08:00:00', 10),
        (2, 1, 'A', '2024-01-01 08:30:00', 20),
        (3, 1, 'B', '2024-01-01 09:00:00', 30),
        (4, 1, 'B', '2024-01-01 09:30:00', 40),
        (5, 2, 'A', '2024-01-01 10:00:00', 50),
        (6, 2, 'A', '2024-01-01 10:30:00', 60),
        (7, 2, 'B', '2024-01-01 11:00:00', 70),
        (8, 2, 'B', '2024-01-01 11:30:00', 80)
    """

    // =====================================================================
    // Test 1: CTE as both probe and build with build-side expression
    // =====================================================================
    qt_abee_cte_expr """
        WITH cte_p AS (SELECT id, grp, ts FROM abee_probe),
             cte_b AS (SELECT id, grp, ts, val FROM abee_build)
        SELECT l.id, r.id as rid, r.val
        FROM cte_p l
        ASOF LEFT JOIN cte_b r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 2: Inline view (subquery) as build side with expression
    // =====================================================================
    qt_abee_inline_view """
        SELECT l.id, r.rid, r.val
        FROM abee_probe l
        ASOF LEFT JOIN (
            SELECT id AS rid, grp, ts AS rts, val FROM abee_build
        ) r
        MATCH_CONDITION(l.ts >= r.rts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 3: Build side from regular JOIN result + expression
    //   Build child = abee_build INNER JOIN abee_dim  (adds name column)
    // =====================================================================
    qt_abee_join_build """
        SELECT l.id, r.rid, r.val, r.name
        FROM abee_probe l
        ASOF LEFT JOIN (
            SELECT b.id AS rid, b.grp, b.ts, b.val, d.name
            FROM abee_build b INNER JOIN abee_dim d ON b.grp = d.grp
        ) r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 4: Chained ASOF JOINs — second join has build-side expression
    // =====================================================================
    qt_abee_chained """
        SELECT l.id, r1.id AS r1id, r1.val, r2.id AS r2id, r2.data
        FROM abee_probe l
        ASOF LEFT JOIN abee_build r1
            MATCH_CONDITION(l.ts >= r1.ts)
            ON l.grp = r1.grp
        ASOF LEFT JOIN abee_build2 r2
            MATCH_CONDITION(l.ts >= r2.ts + INTERVAL 1 HOUR)
            ON l.grp = r2.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 5: Multiple equality keys with expression
    //   ON l.grp = r.grp AND l.grp2 = r.grp2
    //   Tests hash collision handling with compound keys.
    // =====================================================================
    qt_abee_compound_keys """
        SELECT l.id, r.id AS rid, r.val
        FROM abee_probe l
        ASOF LEFT JOIN abee_build r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp AND l.grp2 = r.grp2
        ORDER BY l.id
    """

    // =====================================================================
    // Test 6: Self-ASOF-JOIN via CTE with expression
    //   Same table as both probe and build.  Tests CTE shared-scan +
    //   intermediate row desc when both sides have identical tuples.
    // =====================================================================
    qt_abee_self_join """
        WITH data AS (SELECT id, grp, ts, val FROM abee_build)
        SELECT l.id, r.id AS rid, r.val
        FROM data l
        ASOF LEFT JOIN data r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 7: Cross-precision DATETIME with expression
    //   Probe: DATETIMEV2(3), Build: DATETIMEV2(6).
    //   FE inserts implicit CAST; tests that expression evaluation
    //   on build side handles type promotion correctly.
    // =====================================================================
    sql "DROP TABLE IF EXISTS abee_ts3"
    sql "DROP TABLE IF EXISTS abee_ts6"

    sql """
        CREATE TABLE abee_ts3 (
            id INT, grp INT, ts DATETIMEV2(3)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE abee_ts6 (
            id INT, grp INT, ts DATETIMEV2(6), val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO abee_ts3 VALUES
        (1, 1, '2024-01-01 10:00:00.123'),
        (2, 1, '2024-01-01 12:00:00.456')
    """
    sql """
        INSERT INTO abee_ts6 VALUES
        (1, 1, '2024-01-01 08:00:00.000001', 100),
        (2, 1, '2024-01-01 09:00:00.123000', 110),
        (3, 1, '2024-01-01 10:00:00.123000', 120),
        (4, 1, '2024-01-01 11:00:00.456789', 130)
    """

    qt_abee_cross_precision """
        SELECT l.id, r.id AS rid, r.val
        FROM abee_ts3 l
        ASOF LEFT JOIN abee_ts6 r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 8: Aggregation subquery as build side with expression
    // =====================================================================
    qt_abee_agg_build """
        SELECT l.id, r.category, r.total
        FROM abee_probe l
        ASOF LEFT JOIN (
            SELECT grp, category, MAX(ts) AS max_ts, SUM(amount) AS total
            FROM abee_events
            GROUP BY grp, category
        ) r
        MATCH_CONDITION(l.ts >= r.max_ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 9: WHERE clause filtering after ASOF + expression
    // =====================================================================
    qt_abee_where_filter """
        SELECT l.id, r.id AS rid, r.val
        FROM abee_probe l
        ASOF LEFT JOIN abee_build r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        WHERE r.val > 150 OR r.val IS NULL
        ORDER BY l.id
    """

    // =====================================================================
    // Test 10: Nested subquery (2-level deep) as build side
    //   Inner filter: val > 100 → removes id=1(100)
    // =====================================================================
    qt_abee_nested_subquery """
        SELECT l.id, r.rid, r.val
        FROM abee_probe l
        ASOF LEFT JOIN (
            SELECT rid, grp, rts, val FROM (
                SELECT id AS rid, grp, ts AS rts, val
                FROM abee_build
                WHERE val > 100
            ) inner_q
        ) r
        MATCH_CONDITION(l.ts >= r.rts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Test 11: ASOF INNER JOIN with CTE + expression (no NULL padding)
    //   Unmatched probe rows should be dropped entirely.
    // =====================================================================
    qt_abee_inner_cte """
        WITH data AS (SELECT id, grp, ts, val FROM abee_build)
        SELECT l.id, r.id AS rid, r.val
        FROM data l
        ASOF INNER JOIN data r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // =====================================================================
    // Cross-validation: compare ASOF JOIN result with equivalent
    // regular LEFT JOIN + ROW_NUMBER to ensure semantic correctness.
    // Uses Test 1 data (abee_probe ASOF LEFT JOIN abee_build with +1h).
    // =====================================================================
    def asof_result = sql """
        SELECT l.id, r.id AS rid, r.val
        FROM abee_probe l
        ASOF LEFT JOIN abee_build r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    def regular_result = sql """
        SELECT id, rid, val FROM (
            SELECT l.id, r.id AS rid, r.val,
                   ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY r.ts DESC) AS rn
            FROM abee_probe l
            LEFT JOIN abee_build r
                ON l.grp = r.grp AND l.ts >= r.ts + INTERVAL 1 HOUR
        ) t
        WHERE rn = 1
        ORDER BY id
    """

    assertEquals(asof_result.size(), regular_result.size())
    for (int i = 0; i < asof_result.size(); i++) {
        assertEquals(asof_result[i][0], regular_result[i][0],
            "Row ${i} id mismatch: ASOF=${asof_result[i][0]} vs Regular=${regular_result[i][0]}")
        assertEquals(asof_result[i][1], regular_result[i][1],
            "Row ${i} rid mismatch: ASOF=${asof_result[i][1]} vs Regular=${regular_result[i][1]}")
        assertEquals(asof_result[i][2], regular_result[i][2],
            "Row ${i} val mismatch: ASOF=${asof_result[i][2]} vs Regular=${regular_result[i][2]}")
    }

    // Cross-validate chained join (Test 4) second ASOF result
    def chain_asof = sql """
        SELECT l.id, r2.id AS r2id, r2.data
        FROM abee_probe l
        ASOF LEFT JOIN abee_build r1
            MATCH_CONDITION(l.ts >= r1.ts) ON l.grp = r1.grp
        ASOF LEFT JOIN abee_build2 r2
            MATCH_CONDITION(l.ts >= r2.ts + INTERVAL 1 HOUR) ON l.grp = r2.grp
        ORDER BY l.id
    """

    def chain_regular = sql """
        SELECT id, r2id, data FROM (
            SELECT l.id, r2.id AS r2id, r2.data,
                   ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY r2.ts DESC) AS rn
            FROM abee_probe l
            LEFT JOIN abee_build2 r2
                ON l.grp = r2.grp AND l.ts >= r2.ts + INTERVAL 1 HOUR
        ) t
        WHERE rn = 1
        ORDER BY id
    """

    assertEquals(chain_asof.size(), chain_regular.size())
    for (int i = 0; i < chain_asof.size(); i++) {
        assertEquals(chain_asof[i][1], chain_regular[i][1],
            "Chain row ${i} r2id mismatch: ASOF=${chain_asof[i][1]} vs Regular=${chain_regular[i][1]}")
        assertEquals(chain_asof[i][2], chain_regular[i][2],
            "Chain row ${i} data mismatch")
    }
}
