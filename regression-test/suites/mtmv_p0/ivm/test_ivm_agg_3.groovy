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

suite("test_ivm_agg_3") {

    // =========================================================
    // Part 10: Multiple group keys — GROUP BY k1, k2
    //          Tests composite row_id = hash(k1, k2) and multi-column grouping
    //          Also tests group deletion with composite keys via binlog_op
    // =========================================================

    sql """drop materialized view if exists ivm_agg_multikey_mv;"""
    sql """drop table if exists ivm_agg_multikey_base;"""

    sql """
        CREATE TABLE ivm_agg_multikey_base (
            id INT,
            k1 INT,
            k2 VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: 3 composite groups
    // (k1=1,k2='a'): 2 rows (v1=10,20)
    // (k1=1,k2='b'): 1 row (v1=30)
    // (k1=2,k2='a'): 1 row (v1=40)
    sql """
        INSERT INTO ivm_agg_multikey_base VALUES
            (1, 1, 'a', 10),
            (2, 1, 'a', 20),
            (3, 1, 'b', 30),
            (4, 2, 'a', 40);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_multikey_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, k2, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM ivm_agg_multikey_base
           GROUP BY k1, k2;
    """

    // Step 1: COMPLETE refresh
    // (1,'a'): cnt=2, sum=30; (1,'b'): cnt=1, sum=30; (2,'a'): cnt=1, sum=40
    sql """REFRESH MATERIALIZED VIEW ivm_agg_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multikey_mv")

    order_qt_multikey_after_complete """
        SELECT k1, k2, cnt, sum_v1 FROM ivm_agg_multikey_mv ORDER BY k1, k2
    """

    // Step 2: Insert new rows — add to existing group (1,'a') and create new group (2,'b')
    sql """INSERT INTO ivm_agg_multikey_base VALUES (5, 1, 'a', 50);"""
    sql """INSERT INTO ivm_agg_multikey_base VALUES (6, 2, 'b', 60);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_multikey_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multikey_mv")

    // After INCREMENTAL insert: new group (2,'b') appears; (1,'a') inflated due to mock delta.
    order_qt_multikey_after_insert_incremental """
        SELECT k1, k2, cnt, sum_v1 FROM ivm_agg_multikey_mv ORDER BY k1, k2
    """

    // COMPLETE to verify
    // (1,'a'): id=1(10),2(20),5(50) → cnt=3, sum=80
    // (1,'b'): id=3(30) → cnt=1, sum=30
    // (2,'a'): id=4(40) → cnt=1, sum=40
    // (2,'b'): id=6(60) → cnt=1, sum=60
    sql """REFRESH MATERIALIZED VIEW ivm_agg_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multikey_mv")

    order_qt_multikey_after_insert_complete """
        SELECT k1, k2, cnt, sum_v1 FROM ivm_agg_multikey_mv ORDER BY k1, k2
    """

    // Step 3: Delete the only row in group (1,'b') — should disappear from MV
    sql """DELETE FROM ivm_agg_multikey_base WHERE k1 = 3;"""
    // Dirty partition
    sql """INSERT INTO ivm_agg_multikey_base VALUES (7, 2, 'a', 70);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_multikey_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multikey_mv")

    // After INCREMENTAL: group (1,'b') should be gone
    order_qt_multikey_after_delete_incremental """
        SELECT k1, k2, cnt, sum_v1 FROM ivm_agg_multikey_mv ORDER BY k1, k2
    """

    // COMPLETE to verify ground truth
    // Physical: id=1(1,'a',10,0), id=2(1,'a',20,0), id=3(1,'b',30,1), id=4(2,'a',40,0),
    //           id=5(1,'a',50,0), id=6(2,'b',60,0), id=7(2,'a',70,0)
    // COMPLETE ignores binlog_op:
    // (1,'a'): cnt=3, sum=80; (1,'b'): cnt=1, sum=30; (2,'a'): cnt=2, sum=110; (2,'b'): cnt=1, sum=60
    sql """REFRESH MATERIALIZED VIEW ivm_agg_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multikey_mv")

    order_qt_multikey_after_final_complete """
        SELECT k1, k2, cnt, sum_v1 FROM ivm_agg_multikey_mv ORDER BY k1, k2
    """

    // =========================================================
    // Part 11: Multiple same-type aggregations — SUM(v1), SUM(v2), MIN(v1), MAX(v2)
    //          Verifies hidden state columns don't collide across aggregations
    // =========================================================

    sql """drop materialized view if exists ivm_agg_multiagg_mv;"""
    sql """drop table if exists ivm_agg_multiagg_base;"""

    sql """
        CREATE TABLE ivm_agg_multiagg_base (
            k1 INT,
            grp INT,
            v1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO ivm_agg_multiagg_base VALUES
            (1, 1, 10, 100),
            (2, 1, 20, 200),
            (3, 2, 30, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_multiagg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp, SUM(v1) AS sum_v1, SUM(v2) AS sum_v2, MIN(v1) AS min_v1, MAX(v2) AS max_v2, COUNT(*) AS cnt
           FROM ivm_agg_multiagg_base
           GROUP BY grp;
    """

    // COMPLETE: grp=1: sum_v1=30, sum_v2=300, min_v1=10, max_v2=200, cnt=2
    //           grp=2: sum_v1=30, sum_v2=300, min_v1=30, max_v2=300, cnt=1
    sql """REFRESH MATERIALIZED VIEW ivm_agg_multiagg_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multiagg_mv")

    order_qt_multiagg_after_complete """
        SELECT grp, sum_v1, sum_v2, min_v1, max_v2, cnt
        FROM ivm_agg_multiagg_mv ORDER BY grp
    """

    // Insert safe rows (not touching MIN/MAX boundaries)
    sql """INSERT INTO ivm_agg_multiagg_base VALUES (4, 1, 15, 150);"""
    sql """INSERT INTO ivm_agg_multiagg_base VALUES (5, 2, 35, 250);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_multiagg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multiagg_mv")

    // After INCREMENTAL insert: mock delta inflates counts; output still queryable.
    order_qt_multiagg_after_insert_incremental """
        SELECT grp, sum_v1, sum_v2, min_v1, max_v2, cnt
        FROM ivm_agg_multiagg_mv ORDER BY grp
    """

    // COMPLETE: grp=1: sum_v1=45, sum_v2=450, min_v1=10, max_v2=200, cnt=3
    //           grp=2: sum_v1=65, sum_v2=550, min_v1=30, max_v2=300, cnt=2
    sql """REFRESH MATERIALIZED VIEW ivm_agg_multiagg_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_multiagg_mv")

    order_qt_multiagg_after_insert_complete """
        SELECT grp, sum_v1, sum_v2, min_v1, max_v2, cnt
        FROM ivm_agg_multiagg_mv ORDER BY grp
    """

    // =========================================================
    // Part 12: Negative values / SUM cancellation
    //          SUM crosses zero, becomes negative, or cancels to exactly 0
    // =========================================================

    sql """drop materialized view if exists ivm_agg_negval_mv;"""
    sql """drop table if exists ivm_agg_negval_base;"""

    sql """
        CREATE TABLE ivm_agg_negval_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial: v1=100, v1=-80 → SUM=20
    sql """
        INSERT INTO ivm_agg_negval_base VALUES
            (1, 100),
            (2, -80);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_negval_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM ivm_agg_negval_base;
    """

    // COMPLETE: cnt=2, sum=20
    sql """REFRESH MATERIALIZED VIEW ivm_agg_negval_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_negval_mv")

    order_qt_negval_after_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_negval_mv
    """

    // Insert v1=-50 → SUM should be 100+(-80)+(-50)=-30 (crosses zero to negative)
    sql """INSERT INTO ivm_agg_negval_base VALUES (3, -50);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_negval_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_negval_mv")

    // After INCREMENTAL: mock delta reads all 3 rows; SUM inflated but queryable.
    order_qt_negval_after_first_incremental """
        SELECT cnt, sum_v1 FROM ivm_agg_negval_mv
    """

    // COMPLETE: cnt=3, sum=-30
    sql """REFRESH MATERIALIZED VIEW ivm_agg_negval_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_negval_mv")

    order_qt_negval_after_negative_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_negval_mv
    """

    // Insert v1=30 → SUM should be -30+30=0 (exact cancellation to zero)
    sql """INSERT INTO ivm_agg_negval_base VALUES (4, 30);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_negval_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_negval_mv")

    // After second INCREMENTAL: 4 rows in delta; output queryable.
    order_qt_negval_after_second_incremental """
        SELECT cnt, sum_v1 FROM ivm_agg_negval_mv
    """

    // COMPLETE: cnt=4, sum=0
    sql """REFRESH MATERIALIZED VIEW ivm_agg_negval_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_negval_mv")

    order_qt_negval_after_zero_complete """
        SELECT cnt, sum_v1 FROM ivm_agg_negval_mv
    """

    // =========================================================
    // Part 13: Empty delta — INCREMENTAL without changes → NOT_REFRESH
    // =========================================================

    sql """drop materialized view if exists ivm_agg_emptydelta_mv;"""
    sql """drop table if exists ivm_agg_emptydelta_base;"""

    sql """
        CREATE TABLE ivm_agg_emptydelta_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_agg_emptydelta_base VALUES (1, 10), (2, 20);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_emptydelta_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM ivm_agg_emptydelta_base;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_agg_emptydelta_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_emptydelta_mv")

    // INCREMENTAL without any new data — should get SUCCESS (NOT_REFRESH)
    sql """REFRESH MATERIALIZED VIEW ivm_agg_emptydelta_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_emptydelta_mv")

    // Verify data is unchanged
    order_qt_emptydelta_unchanged """
        SELECT cnt, sum_v1 FROM ivm_agg_emptydelta_mv
    """

    // =========================================================
    // Part 14: Type casting / widening — TINYINT, DECIMAL, DOUBLE columns
    // =========================================================

    sql """drop materialized view if exists ivm_agg_types_mv;"""
    sql """drop table if exists ivm_agg_types_base;"""

    sql """
        CREATE TABLE ivm_agg_types_base (
            k1 INT,
            v_tiny TINYINT,
            v_dec DECIMAL(10,2),
            v_dbl DOUBLE
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO ivm_agg_types_base VALUES
            (1, 10, 99.50, 1.5),
            (2, 20, 50.25, 2.5),
            (3, -5, 100.75, 3.5);
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_agg_types_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT SUM(v_tiny) AS sum_tiny, AVG(v_tiny) AS avg_tiny,
                  SUM(v_dec) AS sum_dec, AVG(v_dec) AS avg_dec,
                  SUM(v_dbl) AS sum_dbl, AVG(v_dbl) AS avg_dbl,
                  MIN(v_tiny) AS min_tiny, MAX(v_dec) AS max_dec,
                  COUNT(*) AS cnt
           FROM ivm_agg_types_base;
    """

    // COMPLETE: sum_tiny=25, avg_tiny≈8.33, sum_dec=250.50, avg_dec≈83.50,
    //           sum_dbl=7.5, avg_dbl=2.5, min_tiny=-5, max_dec=100.75, cnt=3
    sql """REFRESH MATERIALIZED VIEW ivm_agg_types_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_types_mv")

    order_qt_types_after_complete """
        SELECT sum_tiny, avg_tiny, sum_dec, avg_dec, sum_dbl, avg_dbl, min_tiny, max_dec, cnt
        FROM ivm_agg_types_mv
    """

    // Safe insert (doesn't touch MIN/MAX boundaries)
    sql """INSERT INTO ivm_agg_types_base VALUES (4, 0, 75.00, 4.0);"""

    sql """REFRESH MATERIALIZED VIEW ivm_agg_types_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_types_mv")

    // After INCREMENTAL: row k1=4 added; all typed aggregates queryable.
    order_qt_types_after_insert_incremental """
        SELECT sum_tiny, avg_tiny, sum_dec, avg_dec, sum_dbl, avg_dbl, min_tiny, max_dec, cnt
        FROM ivm_agg_types_mv
    """

    // COMPLETE: sum_tiny=25, avg_tiny≈6.25, sum_dec=325.50, avg_dec≈81.375,
    //           sum_dbl=11.5, avg_dbl=2.875, min_tiny=-5, max_dec=100.75, cnt=4
    sql """REFRESH MATERIALIZED VIEW ivm_agg_types_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_agg_types_mv")

    order_qt_types_after_insert_complete """
        SELECT sum_tiny, avg_tiny, sum_dec, avg_dec, sum_dbl, avg_dbl, min_tiny, max_dec, cnt
        FROM ivm_agg_types_mv
    """
}
