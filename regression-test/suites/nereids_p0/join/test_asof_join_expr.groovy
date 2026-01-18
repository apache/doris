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

// Test suite for ASOF JOIN with datetime expressions in MATCH_CONDITION
// FE allows expressions as long as both sides are date-like types

suite("test_asof_join_expr", "nereids_p0") {

    sql "DROP TABLE IF EXISTS asof_expr_left"
    sql "DROP TABLE IF EXISTS asof_expr_right"

    sql """
        CREATE TABLE asof_expr_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_expr_right (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_expr_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 1, '2024-01-01 11:00:00'),
        (3, 1, '2024-01-01 12:00:00')
    """

    sql """
        INSERT INTO asof_expr_right VALUES
        (1, 1, '2024-01-01 08:00:00', 'r1'),
        (2, 1, '2024-01-01 09:00:00', 'r2'),
        (3, 1, '2024-01-01 10:00:00', 'r3'),
        (4, 1, '2024-01-01 11:00:00', 'r4')
    """

    // Test 1: Expression on build side - r.ts + INTERVAL 1 HOUR
    // l.ts >= r.ts + 1h => r.ts <= l.ts - 1h
    // l.id=1 (10:00): r.ts <= 09:00 => match r2(09:00)
    // l.id=2 (11:00): r.ts <= 10:00 => match r3(10:00)
    // l.id=3 (12:00): r.ts <= 11:00 => match r4(11:00)
    qt_expr_build_interval """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Test 2: Expression on probe side
    qt_expr_probe_interval """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts - INTERVAL 1 HOUR >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Test 3: Simple column reference (baseline)
    qt_expr_simple """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 4: DATE_ADD function ====================
    // DATE_ADD is equivalent to INTERVAL but as a function
    qt_expr_date_add """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= DATE_ADD(r.ts, INTERVAL 1 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 5: DATE_SUB function ====================
    qt_expr_date_sub """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(DATE_SUB(l.ts, INTERVAL 1 HOUR) >= r.ts)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 6: DAYS_ADD function ====================
    qt_expr_days_add """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= DAYS_ADD(r.ts, 0))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 7: Nested functions ====================
    // HOURS_ADD inside MINUTES_ADD - tests nested function evaluation
    qt_expr_nested_func """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= MINUTES_ADD(HOURS_ADD(r.ts, 1), -30))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 8: HOURS_ADD function ====================
    qt_expr_hours_add """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= HOURS_ADD(r.ts, 2))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 9: Multiple intervals combined ====================
    qt_expr_multi_interval """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 30 MINUTE + INTERVAL 30 MINUTE)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 10: Large dataset with expressions ====================
    // Test with more rows to verify linear scan works correctly
    sql "DROP TABLE IF EXISTS asof_expr_large_left"
    sql "DROP TABLE IF EXISTS asof_expr_large_right"

    sql """
        CREATE TABLE asof_expr_large_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_expr_large_right (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // Insert probe rows
    sql """
        INSERT INTO asof_expr_large_left VALUES
        (1, 1, '2024-01-01 12:00:00'),
        (2, 1, '2024-01-01 15:00:00'),
        (3, 1, '2024-01-01 18:00:00')
    """

    // Insert 15 build rows to exceed binary search threshold (>8)
    sql """
        INSERT INTO asof_expr_large_right VALUES
        (1, 1, '2024-01-01 06:00:00', 'r01'),
        (2, 1, '2024-01-01 07:00:00', 'r02'),
        (3, 1, '2024-01-01 08:00:00', 'r03'),
        (4, 1, '2024-01-01 09:00:00', 'r04'),
        (5, 1, '2024-01-01 10:00:00', 'r05'),
        (6, 1, '2024-01-01 11:00:00', 'r06'),
        (7, 1, '2024-01-01 12:00:00', 'r07'),
        (8, 1, '2024-01-01 13:00:00', 'r08'),
        (9, 1, '2024-01-01 14:00:00', 'r09'),
        (10, 1, '2024-01-01 15:00:00', 'r10'),
        (11, 1, '2024-01-01 16:00:00', 'r11'),
        (12, 1, '2024-01-01 17:00:00', 'r12'),
        (13, 1, '2024-01-01 18:00:00', 'r13'),
        (14, 1, '2024-01-01 19:00:00', 'r14'),
        (15, 1, '2024-01-01 20:00:00', 'r15')
    """

    // With expression: l.ts >= r.ts + 3h => r.ts <= l.ts - 3h
    // l.id=1 (12:00): r.ts <= 09:00 => match r04(09:00)
    // l.id=2 (15:00): r.ts <= 12:00 => match r07(12:00)
    // l.id=3 (18:00): r.ts <= 15:00 => match r10(15:00)
    qt_expr_large_dataset """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_large_left l
        ASOF LEFT JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts >= DATE_ADD(r.ts, INTERVAL 3 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 11: Strict inequality with function ====================
    // l.ts > r.ts + 3h => r.ts < l.ts - 3h
    // l.id=1 (12:00): r.ts < 09:00 => match r03(08:00)
    // l.id=2 (15:00): r.ts < 12:00 => match r06(11:00)
    // l.id=3 (18:00): r.ts < 15:00 => match r09(14:00)
    qt_expr_large_strict """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_large_left l
        ASOF LEFT JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts > DATE_ADD(r.ts, INTERVAL 3 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 12: <= with function ====================
    // l.ts <= r.ts - 3h => r.ts >= l.ts + 3h (find smallest)
    // l.id=1 (12:00): r.ts >= 15:00 => match r10(15:00)
    // l.id=2 (15:00): r.ts >= 18:00 => match r13(18:00)
    // l.id=3 (18:00): r.ts >= 21:00 => no match
    qt_expr_large_le """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_large_left l
        ASOF LEFT JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts <= DATE_SUB(r.ts, INTERVAL 3 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 13: < with function ====================
    // l.ts < r.ts - 3h => r.ts > l.ts + 3h (find smallest, strict)
    // l.id=1 (12:00): r.ts > 15:00 => match r11(16:00)
    // l.id=2 (15:00): r.ts > 18:00 => match r14(19:00)
    // l.id=3 (18:00): r.ts > 21:00 => no match
    qt_expr_large_lt """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_large_left l
        ASOF LEFT JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts < DATE_SUB(r.ts, INTERVAL 3 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 14: Expression with no matches ====================
    // Very large offset - no matches expected
    qt_expr_no_match """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts
        FROM asof_expr_large_left l
        ASOF LEFT JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts >= DATE_ADD(r.ts, INTERVAL 24 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 15: INNER JOIN with expression ====================
    qt_expr_inner """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_large_left l
        ASOF INNER JOIN asof_expr_large_right r
        MATCH_CONDITION(l.ts >= DATE_ADD(r.ts, INTERVAL 3 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 16: Multiple groups with expression ====================
    sql "DROP TABLE IF EXISTS asof_expr_multigrp_left"
    sql "DROP TABLE IF EXISTS asof_expr_multigrp_right"

    sql """
        CREATE TABLE asof_expr_multigrp_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_expr_multigrp_right (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_expr_multigrp_left VALUES
        (1, 1, '2024-01-01 10:00:00'),
        (2, 2, '2024-01-01 10:00:00'),
        (3, 3, '2024-01-01 10:00:00')
    """

    sql """
        INSERT INTO asof_expr_multigrp_right VALUES
        (1, 1, '2024-01-01 08:00:00', 'g1_r1'),
        (2, 1, '2024-01-01 09:00:00', 'g1_r2'),
        (3, 2, '2024-01-01 07:00:00', 'g2_r1'),
        (4, 2, '2024-01-01 08:30:00', 'g2_r2'),
        (5, 3, '2024-01-01 09:30:00', 'g3_r1')
    """

    // l.ts >= r.ts + 1h => r.ts <= l.ts - 1h = 09:00
    // grp=1: r.ts <= 09:00 => match r2(09:00)
    // grp=2: r.ts <= 09:00 => match r4(08:30)
    // grp=3: r.ts <= 09:00 => no match (r5 is 09:30)
    qt_expr_multigrp """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_multigrp_left l
        ASOF LEFT JOIN asof_expr_multigrp_right r
        MATCH_CONDITION(l.ts >= DATE_ADD(r.ts, INTERVAL 1 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 17: Expression on both sides ====================
    qt_expr_both_sides """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(DATE_ADD(l.ts, INTERVAL 30 MINUTE) >= DATE_ADD(r.ts, INTERVAL 1 HOUR))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 18: Deeply nested functions ====================
    // SECONDS_ADD(MINUTES_ADD(HOURS_ADD(...))) - 3 levels of nesting
    qt_expr_deep_nested """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= SECONDS_ADD(MINUTES_ADD(HOURS_ADD(r.ts, 1), -30), 0))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 19: MINUTES_ADD function ====================
    qt_expr_minutes_add """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_left l
        ASOF LEFT JOIN asof_expr_right r
        MATCH_CONDITION(l.ts >= MINUTES_ADD(r.ts, 90))
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // ==================== Test 20: Edge case - expression result equals probe value ====================
    sql "DROP TABLE IF EXISTS asof_expr_edge_left"
    sql "DROP TABLE IF EXISTS asof_expr_edge_right"

    sql """
        CREATE TABLE asof_expr_edge_left (
            id INT,
            grp INT,
            ts DATETIME
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_expr_edge_right (
            id INT,
            grp INT,
            ts DATETIME,
            data VARCHAR(20)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO asof_expr_edge_left VALUES
        (1, 1, '2024-01-01 10:00:00')
    """

    // r.ts + 1h = exactly 10:00 for r1
    sql """
        INSERT INTO asof_expr_edge_right VALUES
        (1, 1, '2024-01-01 09:00:00', 'exact_match'),
        (2, 1, '2024-01-01 08:00:00', 'before'),
        (3, 1, '2024-01-01 10:00:00', 'after')
    """

    // l.ts >= r.ts + 1h
    // r1: 09:00 + 1h = 10:00, 10:00 >= 10:00 TRUE (exact match)
    // r2: 08:00 + 1h = 09:00, 10:00 >= 09:00 TRUE
    // r3: 10:00 + 1h = 11:00, 10:00 >= 11:00 FALSE
    // Best match (largest r.ts satisfying): r1(09:00)
    qt_expr_edge_exact """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_edge_left l
        ASOF LEFT JOIN asof_expr_edge_right r
        MATCH_CONDITION(l.ts >= r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

    // Strict: l.ts > r.ts + 1h
    // r1: 10:00 > 10:00 FALSE
    // r2: 10:00 > 09:00 TRUE
    // Best: r2(08:00)
    qt_expr_edge_strict """
        SELECT l.id, l.ts, r.id as rid, r.ts as rts, r.data
        FROM asof_expr_edge_left l
        ASOF LEFT JOIN asof_expr_edge_right r
        MATCH_CONDITION(l.ts > r.ts + INTERVAL 1 HOUR)
        ON l.grp = r.grp
        ORDER BY l.id
    """

}
