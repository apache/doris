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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_agg_5") {

    // =========================================================
    // Part 1: Bare GROUP BY without aggregate functions (SELECT DISTINCT)
    // Covers: insert new group, partial delete (group survives), full group deletion.
    // Each scenario starts from a fresh COMPLETE to keep group counts at exactly 1 or 2,
    // preventing mock full-table delta inflation from masking correct delete behavior.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_bare_grpby_mv;"""
    sql """drop table if exists test_ivm_agg_bare_grpby_base;"""

    sql """
        CREATE TABLE test_ivm_agg_bare_grpby_base (
            k1 INT,
            k2 VARCHAR(32),
            v1 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial: grp 'a' has 2 rows (k1=1,3), grp 'b' has 2 rows (k1=2,4)
    sql """
        INSERT INTO test_ivm_agg_bare_grpby_base VALUES
            (1, 'a', 10, 0),
            (2, 'b', 20, 0),
            (3, 'a', 30, 0),
            (4, 'b', 40, 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k2 FROM test_ivm_agg_bare_grpby_base GROUP BY k2;
    """

    // Scenario A: Insert new group 'c'
    // COMPLETE baseline — distinct k2: 'a'(cnt=2), 'b'(cnt=2)
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    order_qt_bare_grpby_after_complete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Insert new group 'c' — INCREMENTAL adds one group. Delta: 'a'(+2), 'b'(+2), 'c'(+1).
    // Counts: 'a'→4, 'b'→4, 'c'→1. All three groups present.
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (5, 'c', 50, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    order_qt_bare_grpby_after_insert """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Scenario B: Partial delete — 'a' has 2 rows (k1=1,3), delete k1=3 → group survives
    // Reset to fresh COMPLETE after inserting k1=5,6 to control counts precisely.
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (6, 'c', 60, 0);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")
    // After COMPLETE: 'a'(k1=1,3): cnt=2; 'b'(k1=2,4): cnt=2; 'c'(k1=5,6): cnt=2

    // Delete one 'a' row (k1=3, op=1) and add new 'a' row (k1=7, op=0) as dirty trigger.
    // Delta: 'a'→k1=1(+1)+k1=3(-1)+k1=7(+1)=+1, 'b'→+2, 'c'→+2.
    // 'a' new cnt = 2+1 = 3 > 0, survives. All groups remain.
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (3, 'a', 30, 1);"""
    // Dirty partition so INCREMENTAL runs (need at least one op=0 row to be new)
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (7, 'a', 70, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    // 'a' still present (partial delete, still has k1=1 and k1=7), all 3 groups remain
    order_qt_bare_grpby_after_partial_delete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Scenario C: Full group deletion — delete ALL 'b' rows → group 'b' disappears
    // Reset to fresh COMPLETE with current physical state to control counts.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")
    // Physical rows now: k1=1('a',0), k1=2('b',0), k1=3('a',1), k1=4('b',0),
    //                    k1=5('c',0), k1=6('c',0), k1=7('a',0)
    // COMPLETE ignores binlog_op → 'a': cnt=3, 'b': cnt=2, 'c': cnt=2

    // Delete both 'b' rows in a single batch: k1=2 and k1=4, both op=1.
    // delta for 'b': k1=2(-1) + k1=4(-1) = -2. new_cnt = 2+(-2) = 0 → DELETE_SIGN=1 → gone!
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (2, 'b', 20, 1);"""
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (4, 'b', 40, 1);"""
    // Dirty partition so INCREMENTAL actually runs
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (8, 'a', 80, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    // group 'b' gone (count reached 0), only 'a' and 'c' remain
    order_qt_bare_grpby_after_group_delete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // =========================================================
    // Part 2: Multi-column bare GROUP BY + single-row group deletion
    // Each (k2, v1) group has exactly 1 row. After COMPLETE (count=1),
    // a single INCREMENTAL with op=1 on that row brings count 1→0 → group disappears.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_bare_multikey_mv;"""
    sql """drop table if exists test_ivm_agg_bare_multikey_base;"""

    sql """
        CREATE TABLE test_ivm_agg_bare_multikey_base (
            k1 INT,
            k2 INT,
            v1 VARCHAR(32),
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Each group has exactly 1 row — single-row group edge case
    sql """
        INSERT INTO test_ivm_agg_bare_multikey_base VALUES
            (1, 10, 'x', 0),
            (2, 10, 'y', 0),
            (3, 20, 'x', 0),
            (4, 20, 'y', 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k2, v1 FROM test_ivm_agg_bare_multikey_base GROUP BY k2, v1;
    """

    // Step 1: COMPLETE baseline: (10,x), (10,y), (20,x), (20,y). Each group count = 1.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_multikey_mv")

    order_qt_bare_multi_after_complete """SELECT k2, v1 FROM test_ivm_agg_bare_multikey_mv"""

    // Step 2: In one batch — insert new combo (30,'z') AND delete (20,'y') via k1=4 op=1.
    // Only one INCREMENTAL follows, so counts are not inflated by a prior INCREMENTAL.
    // Delta: (20,'y') → k1=4(op=1→dml=-1) → delta_cnt=-1 → new_count=1+(-1)=0 → gone.
    // Delta: (30,'z') → k1=5(op=0→dml=+1) → new group appears.
    sql """INSERT INTO test_ivm_agg_bare_multikey_base VALUES (4, 20, 'y', 1);"""
    sql """INSERT INTO test_ivm_agg_bare_multikey_base VALUES (5, 30, 'z', 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_multikey_mv")

    // (20,y) gone (count 1→0), (30,z) appears: (10,x), (10,y), (20,x), (30,z)
    order_qt_bare_multi_after_delete """SELECT k2, v1 FROM test_ivm_agg_bare_multikey_mv"""
}
