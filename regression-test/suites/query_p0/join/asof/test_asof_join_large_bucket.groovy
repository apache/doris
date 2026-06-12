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

// Regression test for ASOF JOIN with many entries per hash bucket.
// Covers two bugs:
//   1. AsofEntry containing vectorized::Field crashed during pdqsort swap
//      (type not supported, type=INVALID)
//   2. Concurrent probe tasks sorting same bucket entries without mutex (SIGSEGV)
// Uses 50 probe rows and 30 build rows per group with nullable ASOF columns
// to exercise pdqsort sorting path and NULL filtering.

suite("test_asof_join_large_bucket", "query_p0") {
    sql """ DROP TABLE IF EXISTS asof_lb_left """
    sql """ DROP TABLE IF EXISTS asof_lb_right """

    sql """
        CREATE TABLE asof_lb_left (
            id INT,
            grp INT,
            ts DATETIME NULL
        ) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    sql """
        CREATE TABLE asof_lb_right (
            id INT,
            grp INT,
            ts DATETIME NULL,
            val INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """

    // 50 probe rows per group (3 groups) + 1 NULL row per group = 153 rows
    def left_vals = []
    def lid = 1
    for (g in 1..3) {
        for (i in 0..49) {
            left_vals.add("(${lid++}, ${g}, '2024-01-01 10:${String.format('%02d', i)}:00')")
        }
    }
    left_vals.add("(${lid++}, 1, NULL)")
    left_vals.add("(${lid++}, 2, NULL)")
    left_vals.add("(${lid++}, 3, NULL)")
    sql "INSERT INTO asof_lb_left VALUES " + left_vals.join(", ")

    // 30 build rows per group (every 2 minutes, 0..58) + 1 NULL row per group = 93 rows
    def right_vals = []
    def rid = 1
    for (g in 1..3) {
        for (i in 0..29) {
            def minute = i * 2
            right_vals.add("(${rid}, ${g}, '2024-01-01 10:${String.format('%02d', minute)}:00', ${g * 1000 + rid})")
            rid++
        }
    }
    right_vals.add("(${rid++}, 1, NULL, 9001)")
    right_vals.add("(${rid++}, 2, NULL, 9002)")
    right_vals.add("(${rid++}, 3, NULL, 9003)")
    sql "INSERT INTO asof_lb_right VALUES " + right_vals.join(", ")

    // ==================== Test 1: ASOF LEFT JOIN COUNT(*) ====================
    // Mimics the original failing query: SELECT COUNT(*) FROM t1 ASOF LEFT JOIN t2 ...
    // NULL probe rows get NULL on right side (included in LEFT JOIN count)
    qt_lb_left_count """
        SELECT l.grp, COUNT(*) as cnt
        FROM asof_lb_left l
        ASOF LEFT JOIN asof_lb_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        GROUP BY l.grp
        ORDER BY l.grp
    """

    // ==================== Test 2: ASOF INNER JOIN COUNT(*) ====================
    // NULL probe rows excluded from INNER JOIN
    qt_lb_inner_count """
        SELECT l.grp, COUNT(*) as cnt
        FROM asof_lb_left l
        ASOF INNER JOIN asof_lb_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        GROUP BY l.grp
        ORDER BY l.grp
    """

    // ==================== Test 3: Spot-check boundary and NULL rows ====================
    qt_lb_spot_check """
        SELECT l.id, l.grp, l.ts, r.id as rid, r.ts as rts, r.val
        FROM asof_lb_left l
        ASOF LEFT JOIN asof_lb_right r
        MATCH_CONDITION(l.ts >= r.ts)
        ON l.grp = r.grp
        WHERE l.id IN (1, 25, 50, 51, 100, 151, 152)
        ORDER BY l.id
    """

    // ==================== Test 4: <= operator with many entries ====================
    qt_lb_le_count """
        SELECT l.grp, COUNT(*) as cnt
        FROM asof_lb_left l
        ASOF LEFT JOIN asof_lb_right r
        MATCH_CONDITION(l.ts <= r.ts)
        ON l.grp = r.grp
        GROUP BY l.grp
        ORDER BY l.grp
    """

    // ==================== Test 5: Strict > operator ====================
    qt_lb_gt_count """
        SELECT l.grp, COUNT(*) as cnt
        FROM asof_lb_left l
        ASOF LEFT JOIN asof_lb_right r
        MATCH_CONDITION(l.ts > r.ts)
        ON l.grp = r.grp
        GROUP BY l.grp
        ORDER BY l.grp
    """

    // ==================== Test 6: Strict < operator ====================
    qt_lb_lt_count """
        SELECT l.grp, COUNT(*) as cnt
        FROM asof_lb_left l
        ASOF LEFT JOIN asof_lb_right r
        MATCH_CONDITION(l.ts < r.ts)
        ON l.grp = r.grp
        GROUP BY l.grp
        ORDER BY l.grp
    """
}
