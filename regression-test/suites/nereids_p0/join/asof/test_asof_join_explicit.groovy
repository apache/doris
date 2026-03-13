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

suite("test_asof_join_explicit") {
    // ===================================================================================
    // This test suite verifies ASOF JOIN results are correct with both
    // disable_join_reorder=true and disable_join_reorder=false settings.
    // ===================================================================================

    // ==================== Setup Tables ====================
    // Small table (4 rows) vs Large table (20 rows) - 5x difference for CBO to swap
    sql "DROP TABLE IF EXISTS asof_explicit_small"
    sql "DROP TABLE IF EXISTS asof_explicit_large"

    sql """
        CREATE TABLE asof_explicit_small (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_explicit_large (
            id INT,
            grp INT,
            ts DATETIME,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // Small table: 4 rows
    sql """
        INSERT INTO asof_explicit_small VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 12:00:00'),
        (3, 2, '2024-01-01 11:00:00'),
        (4, 3, '2024-01-01 10:00:00')
    """

    // Large table: 20 rows (5x difference)
    sql """
        INSERT INTO asof_explicit_large VALUES
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
        (19, 3, '2024-01-01 09:00:00', 300),
        (20, 3, '2024-01-01 11:00:00', 310)
    """

    // CRITICAL: Analyze tables for CBO to make correct decisions
    sql "ANALYZE TABLE asof_explicit_small WITH SYNC"
    sql "ANALYZE TABLE asof_explicit_large WITH SYNC"

    // ===================================================================================
    // Test 1: ASOF LEFT OUTER JOIN with >= operator
    // ===================================================================================

    // 1a. Verify LEFT JOIN plan with reorder disabled
    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF LEFT JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts >= l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    qt_left_outer_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // 1b. Verify results with reorder enabled
    sql "SET disable_join_reorder = false"
    qt_right_outer_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 2: ASOF INNER JOIN with >= operator
    // ===================================================================================

    // 2a. Verify LEFT INNER plan with reorder disabled
    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF INNER JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts >= l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    qt_left_inner_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // 2b. Verify results with reorder enabled
    sql "SET disable_join_reorder = false"
    qt_right_inner_ge """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts >= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 3: ASOF LEFT OUTER JOIN with <= operator
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF LEFT JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts <= l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    qt_left_outer_le """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts <= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_outer_le """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts <= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 4: ASOF LEFT OUTER JOIN with > operator (strict)
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF LEFT JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts > l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    qt_left_outer_gt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts > l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_outer_gt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts > l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 5: ASOF LEFT OUTER JOIN with < operator (strict)
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF LEFT JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts < l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_OUTER_JOIN"
    }

    qt_left_outer_lt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts < l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_outer_lt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF LEFT JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts < l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 6: ASOF INNER JOIN with <= operator
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF INNER JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts <= l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    qt_left_inner_le """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts <= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_inner_le """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts <= l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 7: ASOF INNER JOIN with > operator (strict)
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF INNER JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts > l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    qt_left_inner_gt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts > l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_inner_gt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts > l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    // ===================================================================================
    // Test 8: ASOF INNER JOIN with < operator (strict)
    // ===================================================================================

    sql "SET disable_join_reorder = true"
    explain {
        sql """
            SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
            FROM asof_explicit_small s
            ASOF INNER JOIN asof_explicit_large l
            MATCH_CONDITION(s.ts < l.ts)
            ON s.grp = l.grp
        """
        contains "ASOF_LEFT_INNER_JOIN"
    }

    qt_left_inner_lt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts < l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """

    sql "SET disable_join_reorder = false"
    qt_right_inner_lt """
        SELECT s.id, s.grp, s.ts as s_ts, l.ts as l_ts, l.val
        FROM asof_explicit_small s
        ASOF INNER JOIN asof_explicit_large l
        MATCH_CONDITION(s.ts < l.ts)
        ON s.grp = l.grp
        ORDER BY s.id
    """
}
